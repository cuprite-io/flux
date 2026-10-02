package autopilot

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cuprite-io/assay"
	"github.com/cuprite-io/flux/autopilot"
	"github.com/cuprite-io/flux/internal/cluster"
	"github.com/cuprite-io/flux/internal/engine"
	"github.com/cuprite-io/flux/internal/history"
	"github.com/cuprite-io/flux/internal/profiler"
	"github.com/cuprite-io/flux/internal/sink"
	"github.com/cuprite-io/flux/types"
)

var (
	ErrRefinerStopped     = errors.New("flux autopilot: refiner is stopped")
	ErrActiveCircuitNotFound = errors.New("flux autopilot: active circuit not found for refinement")
	ErrRefinementNoTags   = errors.New("flux autopilot: no target stream tags specified for refinement")
)

// RefinementTrigger identifies the stimulus that initiated the refinement cycle.
type RefinementTrigger string

const (
	TriggerScheduled    RefinementTrigger = "SCHEDULED_INTERVAL"
	TriggerSchemaDrift  RefinementTrigger = "SCHEMA_DRIFT"
	TriggerFiringRate   RefinementTrigger = "FIRING_RATE_ANOMALY"
	TriggerUserFeedback RefinementTrigger = "USER_FEEDBACK"
	TriggerManual       RefinementTrigger = "MANUAL"
)

// AlertHistoryReader abstracts retrieving historical alerts and operator feedback.
type AlertHistoryReader interface {
	GetAlerts(ctx context.Context, circuitID string, limit int) ([]history.AlertRecord, error)
}

// RefinerConfig defines operational tuning for the continuous refinement loop.
type RefinerConfig struct {
	Interval           time.Duration // Cadence for periodic scheduled review (default: 24h)
	MaxRefinementRetries int         // Retries for reflection loop (default: 3)
	AlertHistoryLimit  int           // Number of recent alerts to include in prompt (default: 10)
}

// DefaultRefinerConfig returns the default production configuration.
func DefaultRefinerConfig() RefinerConfig {
	return RefinerConfig{
		Interval:           24 * time.Hour,
		MaxRefinementRetries: 3,
		AlertHistoryLimit:  10,
	}
}

// RefinerOption configures a Refiner instance.
type RefinerOption func(*Refiner)

// WithRefinementInterval configures the periodic review schedule.
func WithRefinementInterval(d time.Duration) RefinerOption {
	return func(r *Refiner) {
		if d > 0 {
			r.config.Interval = d
		}
	}
}

// WithRefinerMaxRetries sets the max reflection retries during circuit re-synthesis.
func WithRefinerMaxRetries(n int) RefinerOption {
	return func(r *Refiner) {
		if n > 0 {
			r.config.MaxRefinementRetries = n
		}
	}
}

// WithRefinerAlertHistoryLimit sets how many recent alerts to fetch for refinement prompts.
func WithRefinerAlertHistoryLimit(n int) RefinerOption {
	return func(r *Refiner) {
		if n > 0 {
			r.config.AlertHistoryLimit = n
		}
	}
}

// WithRefinerStager attaches a shadow stager to verify candidate circuits before hot-swapping.
func WithRefinerStager(stager *Stager) RefinerOption {
	return func(r *Refiner) {
		r.stager = stager
	}
}

// WithRefinerExecutor attaches an engine executor for shadow batch testing.
func WithRefinerExecutor(exec *engine.Executor) RefinerOption {
	return func(r *Refiner) {
		r.executor = exec
	}
}

// WithRefinerElector attaches a leader elector so only cluster leaders execute refinements.
func WithRefinerElector(elector cluster.LeaderElector) RefinerOption {
	return func(r *Refiner) {
		r.elector = elector
	}
}

// RefinementRequest specifies the input data and context for a circuit refinement run.
type RefinementRequest struct {
	CircuitID       string
	Trigger         RefinementTrigger
	Reason          string
	UserGoal        string
	TargetTags      []string
	ActiveCircuit   *types.Circuit
	AlertHistory    []history.AlertRecord
	Schemas         map[string]*assay.SchemaNode
	Profiles        map[string]*profiler.StreamProfile
	Sinks           []sink.Descriptor
	TelemetryEvents []any // Optional sample events for shadow batch verification
}

// RefinementResult contains the outcome of a circuit refinement cycle.
type RefinementResult struct {
	CircuitID       string            `json:"circuit_id"`
	Trigger         RefinementTrigger `json:"trigger"`
	PreviousCircuit *types.Circuit    `json:"previous_circuit"`
	RefinedCircuit  *types.Circuit    `json:"refined_circuit"`
	StagingResult   *StagingResult    `json:"staging_result,omitempty"`
	Attempts        int               `json:"attempts"`
	PromptUsed      string            `json:"prompt_used,omitempty"`
	Timestamp       time.Time         `json:"timestamp"`
}

type trackedCircuit struct {
	circuitID string
	userGoal  string
	tags      []string
}

// Refiner monitors active circuits, detects drift and operator feedback,
// and orchestrates the continuous closed-loop refinement cycle.
type Refiner struct {
	provider      autopilot.AIProvider
	registry      CircuitRegistry
	history       AlertHistoryReader
	synthesizer   *Synthesizer
	promptBuilder *PromptBuilder
	stager        *Stager
	executor      *engine.Executor
	elector       cluster.LeaderElector
	config        RefinerConfig

	mu         sync.RWMutex
	circuits   map[string]trackedCircuit
	cancelLoop context.CancelFunc
	stopped    uint32
	wg         sync.WaitGroup
}

// NewRefiner initializes a new continuous Refiner.
func NewRefiner(provider autopilot.AIProvider, reg CircuitRegistry, hist AlertHistoryReader, opts ...RefinerOption) *Refiner {
	r := &Refiner{
		provider:      provider,
		registry:      reg,
		history:       hist,
		promptBuilder: NewPromptBuilder(),
		config:        DefaultRefinerConfig(),
		circuits:      make(map[string]trackedCircuit),
	}
	for _, opt := range opts {
		opt(r)
	}
	if provider != nil {
		r.synthesizer, _ = NewSynthesizer(provider, WithMaxRetries(r.config.MaxRefinementRetries))
	}
	return r
}

// TrackCircuit registers a circuit for continuous automated monitoring and refinement.
func (r *Refiner) TrackCircuit(circuitID, userGoal string, tags []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.circuits[circuitID] = trackedCircuit{
		circuitID: circuitID,
		userGoal:  userGoal,
		tags:      slices.Clone(tags),
	}
}

// UntrackCircuit removes a circuit from automated monitoring.
func (r *Refiner) UntrackCircuit(circuitID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.circuits, circuitID)
}

// Refine executes a full refinement cycle on a circuit.
func (r *Refiner) Refine(ctx context.Context, req RefinementRequest) (*RefinementResult, error) {
	if atomic.LoadUint32(&r.stopped) == 1 {
		return nil, ErrRefinerStopped
	}

	if req.CircuitID == "" {
		return nil, errors.New("flux autopilot: circuit ID required for refinement")
	}

	// 1. Resolve Active Circuit
	activeCircuit := req.ActiveCircuit
	if activeCircuit == nil && r.registry != nil {
		c, err := r.registry.Get(ctx, req.CircuitID)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrActiveCircuitNotFound, err)
		}
		activeCircuit = c
	}
	if activeCircuit == nil {
		return nil, ErrActiveCircuitNotFound
	}

	// 2. Resolve Tags and Goal
	tags := req.TargetTags
	if len(tags) == 0 {
		tags = activeCircuit.Tags
	}
	if len(tags) == 0 {
		return nil, ErrRefinementNoTags
	}

	goal := req.UserGoal
	if goal == "" {
		r.mu.RLock()
		if tc, ok := r.circuits[req.CircuitID]; ok {
			goal = tc.userGoal
		}
		r.mu.RUnlock()
	}

	// 3. Resolve Recent Alerts & Feedback
	alerts := req.AlertHistory
	if len(alerts) == 0 && r.history != nil {
		fetched, err := r.history.GetAlerts(ctx, req.CircuitID, r.config.AlertHistoryLimit)
		if err == nil {
			alerts = fetched
		}
	}

	// 4. Build Refinement Prompt
	triggerStr := string(req.Trigger)
	if triggerStr == "" {
		triggerStr = string(TriggerManual)
	}
	reason := req.Reason
	if reason == "" {
		reason = "Continuous background tuning and accuracy optimization"
	}

	userPrompt := r.promptBuilder.BuildRefinementPrompt(
		goal,
		tags,
		activeCircuit,
		triggerStr,
		reason,
		alerts,
		req.Schemas,
		req.Profiles,
		req.Sinks,
	)

	// 5. Synthesize Candidate via Reflection Loop
	synthReq := SynthesisRequest{
		Prompt:    userPrompt,
		Tags:      tags,
		CircuitID: req.CircuitID,
		Schemas:   req.Schemas,
		Profiles:  req.Profiles,
		Sinks:     req.Sinks,
	}

	synthRes, err := r.synthesizer.SynthesizeWithReflection(ctx, synthReq)
	if err != nil {
		return nil, fmt.Errorf("flux autopilot: refinement synthesis failed: %w", err)
	}

	candidateCircuit := synthRes.Circuit
	// Increment version monotonically
	candidateCircuit.Version = activeCircuit.Version + 1
	candidateCircuit.Tags = slices.Clone(tags)

	result := &RefinementResult{
		CircuitID:       req.CircuitID,
		Trigger:         req.Trigger,
		PreviousCircuit: activeCircuit,
		RefinedCircuit:  candidateCircuit,
		Attempts:        synthRes.Attempts,
		PromptUsed:      userPrompt,
		Timestamp:       time.Now(),
	}

	// 6. Shadow Testing & Promotion Verification
	if r.stager != nil && len(req.TelemetryEvents) > 0 && r.executor != nil {
		stageRes, errStage := r.stager.EvaluateBatch(ctx, candidateCircuit, tags, r.executor, req.TelemetryEvents)
		if errStage != nil {
			return nil, fmt.Errorf("flux autopilot: shadow evaluation failed during refinement: %w", errStage)
		}
		result.StagingResult = stageRes
		if stageRes.Status != StatusPromoted {
			return result, fmt.Errorf("flux autopilot: refined circuit rejected by shadow safety envelope: %s (%s)",
				stageRes.Status, stageRes.RejectionReason)
		}
	} else if r.registry != nil {
		// Direct atomic hot-swap into Registry
		if err := r.registry.Put(ctx, candidateCircuit); err != nil {
			return nil, fmt.Errorf("flux autopilot: failed to hot-swap refined circuit into registry: %w", err)
		}
	}

	return result, nil
}

// RefineOnFeedback triggers a refinement cycle when operator feedback (e.g. "false_positive") is recorded.
func (r *Refiner) RefineOnFeedback(ctx context.Context, circuitID string, record history.AlertRecord) (*RefinementResult, error) {
	reason := "Operator submitted feedback"
	if record.Feedback != nil {
		reason = fmt.Sprintf("Operator marked alert as %q: %s", record.Feedback.Classification, record.Feedback.Reason)
	}

	return r.Refine(ctx, RefinementRequest{
		CircuitID:    circuitID,
		Trigger:      TriggerUserFeedback,
		Reason:       reason,
		AlertHistory: []history.AlertRecord{record},
	})
}

// RefineOnSchemaDrift triggers refinement when schema changes (new unmapped fields or mutations) are detected.
func (r *Refiner) RefineOnSchemaDrift(ctx context.Context, circuitID, tag string, newSchema *assay.SchemaNode) (*RefinementResult, error) {
	schemas := make(map[string]*assay.SchemaNode)
	if newSchema != nil {
		schemas[tag] = newSchema
	}

	return r.Refine(ctx, RefinementRequest{
		CircuitID: circuitID,
		Trigger:   TriggerSchemaDrift,
		Reason:    fmt.Sprintf("Schema mutation or drift observed on tag %q", tag),
		Schemas:   schemas,
	})
}

// RefineOnFiringAnomaly triggers refinement when an alert storm or zero-firing anomaly is detected.
func (r *Refiner) RefineOnFiringAnomaly(ctx context.Context, circuitID string, observedRate float64, anomalyReason string) (*RefinementResult, error) {
	return r.Refine(ctx, RefinementRequest{
		CircuitID: circuitID,
		Trigger:   TriggerFiringRate,
		Reason:    fmt.Sprintf("Firing rate anomaly (rate: %.2f%%): %s", observedRate*100, anomalyReason),
	})
}

// RefineOnSchedule triggers periodic maintenance refinement for a circuit.
func (r *Refiner) RefineOnSchedule(ctx context.Context, circuitID string) (*RefinementResult, error) {
	return r.Refine(ctx, RefinementRequest{
		CircuitID: circuitID,
		Trigger:   TriggerScheduled,
		Reason:    "Scheduled periodic refinement and rule optimization",
	})
}

// Start runs the continuous background monitoring and scheduled refinement worker.
func (r *Refiner) Start(ctx context.Context) error {
	r.mu.Lock()
	if r.cancelLoop != nil {
		r.mu.Unlock()
		return nil
	}
	loopCtx, cancel := context.WithCancel(ctx)
	r.cancelLoop = cancel
	atomic.StoreUint32(&r.stopped, 0)
	r.mu.Unlock()

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.run(loopCtx)
	}()

	return nil
}

func (r *Refiner) run(ctx context.Context) {
	ticker := time.NewTicker(r.config.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// If a leader elector is present, only the Leader Pod executes refinements
			if r.elector != nil && !r.elector.IsLeader() {
				continue
			}

			r.mu.RLock()
			circuits := make([]trackedCircuit, 0, len(r.circuits))
			for _, tc := range r.circuits {
				circuits = append(circuits, tc)
			}
			r.mu.RUnlock()

			for _, tc := range circuits {
				if ctx.Err() != nil {
					return
				}
				_, _ = r.RefineOnSchedule(ctx, tc.circuitID)
			}
		}
	}
}

// Stop stops the continuous background refinement worker.
func (r *Refiner) Stop(ctx context.Context) error {
	atomic.StoreUint32(&r.stopped, 1)

	r.mu.Lock()
	cancel := r.cancelLoop
	r.cancelLoop = nil
	r.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	r.wg.Wait()
	return nil
}
