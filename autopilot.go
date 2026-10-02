package flux

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cuprite-io/assay"
	"github.com/cuprite-io/flux/autopilot"
	internalautopilot "github.com/cuprite-io/flux/internal/autopilot"
	"github.com/cuprite-io/flux/internal/cluster"
	"github.com/cuprite-io/flux/internal/history"
	"github.com/cuprite-io/flux/internal/profiler"
	"github.com/cuprite-io/flux/internal/schematap"
	"github.com/cuprite-io/flux/types"
)

// AutopilotStatus represents the current lifecycle phase of an autonomous Autopilot session.
type AutopilotStatus string

const (
	// StatusSampling indicates the engine is profiling incoming stream traffic to infer schemas and exemplars.
	StatusSampling AutopilotStatus = "SAMPLING"
	// StatusSynthesizing indicates the AI provider is synthesizing and validating candidate circuit DAGs.
	StatusSynthesizing AutopilotStatus = "SYNTHESIZING"
	// StatusShadowing indicates the candidate circuit is observing live or batch events in shadow execution mode.
	StatusShadowing AutopilotStatus = "SHADOWING"
	// StatusLive indicates the circuit has passed the safety envelope and is actively executing live traffic.
	StatusLive AutopilotStatus = "LIVE"
	// StatusRefining indicates the autonomous refiner is actively updating or tuning the live circuit.
	StatusRefining AutopilotStatus = "REFINING"
	// StatusStopped indicates the session was terminated by the operator.
	StatusStopped AutopilotStatus = "STOPPED"
	// StatusFailed indicates the session encountered an unrecoverable compilation or staging failure.
	StatusFailed AutopilotStatus = "FAILED"
)

// RefinementTrigger identifies what stimulus triggered a continuous refinement cycle.
type RefinementTrigger = internalautopilot.RefinementTrigger

const (
	TriggerScheduled    = internalautopilot.TriggerScheduled
	TriggerSchemaDrift  = internalautopilot.TriggerSchemaDrift
	TriggerFiringRate   = internalautopilot.TriggerFiringRate
	TriggerUserFeedback = internalautopilot.TriggerUserFeedback
	TriggerManual       = internalautopilot.TriggerManual
)

// StagingResult encapsulates the safety envelope verification verdict of candidate circuit shadow evaluation.
type StagingResult = internalautopilot.StagingResult

// StagingStatus represents the verdict of shadow staging verification.
type StagingStatus = internalautopilot.StagingStatus

// AutopilotConfig holds parameters and safety bounds for an autonomous Autopilot session.
type AutopilotConfig struct {
	SampleThreshold        int
	ShadowEvaluationEvents int
	MinTargetFiringRate    float64
	MaxTargetFiringRate    float64
	AlertStormThreshold    float64
	AutoPromotion          bool
	RefinementInterval     time.Duration
	EvaluationBatch        []any
	LeaderElector          cluster.LeaderElector
	MaxReflectionRetries   int
}

// DefaultAutopilotConfig returns production default settings.
func DefaultAutopilotConfig() AutopilotConfig {
	return AutopilotConfig{
		SampleThreshold:        500,
		ShadowEvaluationEvents: 200,
		MinTargetFiringRate:    0.0001,
		MaxTargetFiringRate:    0.02,
		AlertStormThreshold:    0.05,
		AutoPromotion:          true,
		RefinementInterval:     24 * time.Hour,
		MaxReflectionRetries:   3,
	}
}

// AutopilotOption configures an AutopilotConfig instance.
type AutopilotOption func(*AutopilotConfig)

// WithSampleThreshold sets the minimum stream samples required before synthesis begins (default: 500).
// Set to 0 to bypass the cold-start sampling phase and synthesize immediately.
func WithSampleThreshold(n int) AutopilotOption {
	return func(c *AutopilotConfig) {
		if n >= 0 {
			c.SampleThreshold = n
		}
	}
}

// WithShadowEvaluationEvents sets the event observation budget for shadow verification (default: 200).
func WithShadowEvaluationEvents(n int) AutopilotOption {
	return func(c *AutopilotConfig) {
		if n > 0 {
			c.ShadowEvaluationEvents = n
		}
	}
}

// WithMinTargetFiringRate sets the lower acceptable bound for firing rate during shadow evaluation (default: 0.0001).
func WithMinTargetFiringRate(rate float64) AutopilotOption {
	return func(c *AutopilotConfig) {
		c.MinTargetFiringRate = rate
	}
}

// WithMaxTargetFiringRate sets the upper acceptable bound for firing rate during shadow evaluation (default: 0.02).
func WithMaxTargetFiringRate(rate float64) AutopilotOption {
	return func(c *AutopilotConfig) {
		c.MaxTargetFiringRate = rate
	}
}

// WithAlertStormThreshold sets the threshold above which a candidate is rejected as an alert storm (default: 0.05).
func WithAlertStormThreshold(rate float64) AutopilotOption {
	return func(c *AutopilotConfig) {
		c.AlertStormThreshold = rate
	}
}

// WithAutoPromotion toggles whether safe verified circuits are automatically promoted to Live in Registry (default: true).
func WithAutoPromotion(enable bool) AutopilotOption {
	return func(c *AutopilotConfig) {
		c.AutoPromotion = enable
	}
}

// WithRefinementInterval sets the cadence for periodic background review and tuning (default: 24h).
func WithRefinementInterval(d time.Duration) AutopilotOption {
	return func(c *AutopilotConfig) {
		if d > 0 {
			c.RefinementInterval = d
		}
	}
}

// WithEvaluationBatch provides a slice of events for immediate offline shadow evaluation.
func WithEvaluationBatch(events []any) AutopilotOption {
	return func(c *AutopilotConfig) {
		c.EvaluationBatch = events
	}
}

// WithLeaderElector attaches a custom cluster LeaderElector.
// If omitted, an elector backed by the Engine's CacheBackend is automatically utilized.
func WithLeaderElector(elector cluster.LeaderElector) AutopilotOption {
	return func(c *AutopilotConfig) {
		c.LeaderElector = elector
	}
}

// WithMaxReflectionRetries sets the maximum reflection self-correction attempts during synthesis (default: 3).
func WithMaxReflectionRetries(retries int) AutopilotOption {
	return func(c *AutopilotConfig) {
		if retries > 0 {
			c.MaxReflectionRetries = retries
		}
	}
}

// AutopilotHandle provides management, inspection, and feedback controls for an autonomous session.
type AutopilotHandle struct {
	engine   *Engine
	cfg      AutopilotConfig
	prompt   string
	provider autopilot.AIProvider
	tags     []string
	elector  cluster.LeaderElector
	refiner  *internalautopilot.Refiner

	mu             sync.RWMutex
	status         AutopilotStatus
	circuit        *types.Circuit
	activeCircuits []string
	stagingResult  *internalautopilot.StagingResult
	lastErr        error

	cancel context.CancelFunc
	doneCh chan struct{}
}

// Status returns the current lifecycle status of the autopilot session.
func (h *AutopilotHandle) Status() AutopilotStatus {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.status
}

// ActiveCircuits returns the list of active circuit IDs managed by this session.
func (h *AutopilotHandle) ActiveCircuits() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return slices.Clone(h.activeCircuits)
}

// CurrentCircuit returns the latest compiled Circuit definition, or nil if not yet synthesized.
func (h *AutopilotHandle) CurrentCircuit() *types.Circuit {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.circuit
}

// StagingResult returns the verification metrics from the shadow evaluation phase.
func (h *AutopilotHandle) StagingResult() *StagingResult {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.stagingResult
}

// Error returns the failure error if the session entered StatusFailed.
func (h *AutopilotHandle) Error() error {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.lastErr
}

// WaitUntil blocks until the session reaches the targetStatus or enters a terminal state (StatusFailed, StatusStopped).
func (h *AutopilotHandle) WaitUntil(ctx context.Context, targetStatus AutopilotStatus) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		h.mu.RLock()
		st := h.status
		err := h.lastErr
		h.mu.RUnlock()

		if st == targetStatus {
			return nil
		}
		if st == StatusFailed {
			if err != nil {
				return err
			}
			return errors.New("flux autopilot: session failed")
		}
		if st == StatusStopped {
			return errors.New("flux autopilot: session stopped")
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// RecordFeedback attaches operator feedback to an alert and triggers closed-loop rule adaptation.
func (h *AutopilotHandle) RecordFeedback(ctx context.Context, alertID, classification, reason string) error {
	h.mu.RLock()
	circuits := slices.Clone(h.activeCircuits)
	refiner := h.refiner
	h.mu.RUnlock()

	if len(circuits) == 0 {
		return errors.New("flux autopilot: no active circuit to attach feedback")
	}
	circuitID := circuits[0]

	// 1. Record feedback in the history store
	if err := h.engine.RecordAlertFeedback(ctx, circuitID, alertID, history.Feedback{
		Classification: classification,
		Reason:         reason,
	}); err != nil {
		return err
	}

	// 2. If feedback flags false positives or noise, trigger autonomous adaptation
	if refiner != nil && (classification == FeedbackFalsePositive || classification == FeedbackNoisy) {
		rec := history.AlertRecord{
			ID:        alertID,
			CircuitID: circuitID,
			Feedback: &history.Feedback{
				Classification: classification,
				Reason:         reason,
			},
		}
		go func() {
			_, _ = refiner.RefineOnFeedback(context.Background(), circuitID, rec)
		}()
	}

	return nil
}

// TriggerRefinement manually executes a refinement and hot-swap cycle.
func (h *AutopilotHandle) TriggerRefinement(ctx context.Context, trigger RefinementTrigger, reason string) error {
	h.mu.RLock()
	refiner := h.refiner
	circuit := h.circuit
	h.mu.RUnlock()

	if refiner == nil || circuit == nil {
		return errors.New("flux autopilot: refiner not ready or no active circuit")
	}

	h.setStatus(StatusRefining)
	defer func() {
		h.mu.Lock()
		if h.status == StatusRefining {
			h.status = StatusLive
		}
		h.mu.Unlock()
	}()

	req := internalautopilot.RefinementRequest{
		CircuitID:     circuit.ID,
		Trigger:       trigger,
		Reason:        reason,
		UserGoal:      h.prompt,
		TargetTags:    h.tags,
		ActiveCircuit: circuit,
	}

	res, err := refiner.Refine(ctx, req)
	if err != nil {
		return err
	}

	if res != nil && res.RefinedCircuit != nil {
		h.mu.Lock()
		h.circuit = res.RefinedCircuit
		h.mu.Unlock()
	}

	return nil
}

// Stop gracefully terminates the autopilot session, stopping background refiners and electors.
func (h *AutopilotHandle) Stop(ctx context.Context) error {
	h.cancel()
	if h.refiner != nil {
		_ = h.refiner.Stop(ctx)
	}
	if h.elector != nil {
		_ = h.elector.Stop(ctx)
	}
	h.setStatus(StatusStopped)
	select {
	case <-h.doneCh:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (h *AutopilotHandle) setStatus(s AutopilotStatus) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.status = s
}

func (h *AutopilotHandle) fail(err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.status = StatusFailed
	h.lastErr = err
}

// Autopilot initiates an autonomous rule synthesis, shadow verification, and continuous adaptation session.
func (e *Engine) Autopilot(
	ctx context.Context,
	prompt string,
	provider autopilot.AIProvider,
	tags []string,
	opts ...AutopilotOption,
) (*AutopilotHandle, error) {
	if prompt == "" {
		return nil, errors.New("flux: autopilot prompt cannot be empty")
	}
	if provider == nil {
		return nil, errors.New("flux: autopilot AIProvider cannot be nil")
	}
	if len(tags) == 0 {
		return nil, errors.New("flux: autopilot requires at least one target stream tag")
	}

	cfg := DefaultAutopilotConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	// Ensure essential streaming subsystems are active
	if e.schemaTap == nil {
		tap, err := schematap.New(e.cache, schematap.DefaultConfig())
		if err != nil {
			return nil, fmt.Errorf("flux: failed to initialize schema tap: %w", err)
		}
		e.schemaTap = tap
	}
	if e.profiler == nil {
		prof, err := profiler.New(e.cache, profiler.DefaultConfig())
		if err != nil {
			return nil, fmt.Errorf("flux: failed to initialize profiler: %w", err)
		}
		e.profiler = prof
	}
	if e.history == nil {
		hist, err := history.New(e.cache, history.DefaultConfig())
		if err != nil {
			return nil, fmt.Errorf("flux: failed to initialize history store: %w", err)
		}
		e.history = hist
	}

	elector := cfg.LeaderElector
	if elector == nil {
		elector = cluster.NewLeaderElector(e.cache)
	}

	sessionCtx, cancel := context.WithCancel(ctx)
	handle := &AutopilotHandle{
		engine:   e,
		cfg:      cfg,
		prompt:   prompt,
		provider: provider,
		tags:     slices.Clone(tags),
		elector:  elector,
		status:   StatusSampling,
		cancel:   cancel,
		doneCh:   make(chan struct{}),
	}

	e.autopilotMu.Lock()
	e.autopilotHandles = append(e.autopilotHandles, handle)
	e.autopilotMu.Unlock()

	go handle.runSession(sessionCtx)

	return handle, nil
}

func (h *AutopilotHandle) runSession(ctx context.Context) {
	defer close(h.doneCh)

	// 1. Start cluster leader elector
	if err := h.elector.Start(ctx); err != nil {
		h.fail(fmt.Errorf("flux autopilot: failed to start leader elector: %w", err))
		return
	}

	// 2. Cold-Start Sampling Phase
	if h.cfg.SampleThreshold > 0 {
		h.setStatus(StatusSampling)

		var sampledCount int64
		// Check existing samples in profiler
		for _, tag := range h.tags {
			if prof, err := h.engine.profiler.GetProfile(ctx, tag); err == nil && prof != nil {
				sampledCount += int64(prof.TotalSampled)
			}
		}

		if sampledCount < int64(h.cfg.SampleThreshold) {
			sampleDoneCh := make(chan struct{})
			var once sync.Once

			unsub := h.engine.addSparkObserver(func(payload any, tags []string, res *types.SparkResult) {
				match := false
				for _, t := range tags {
					if slices.Contains(h.tags, t) {
						match = true
						break
					}
				}
				if match || len(h.tags) == 0 {
					if atomic.AddInt64(&sampledCount, 1) >= int64(h.cfg.SampleThreshold) {
						once.Do(func() {
							close(sampleDoneCh)
						})
					}
				}
			})

			select {
			case <-ctx.Done():
				unsub()
				h.setStatus(StatusStopped)
				return
			case <-sampleDoneCh:
				unsub()
			}
		}
	}

	// 3. Synthesis & Reflection Phase
	h.setStatus(StatusSynthesizing)

	// Follower pod standby check
	if !h.elector.IsLeader() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				h.setStatus(StatusStopped)
				return
			case <-ticker.C:
				if h.elector.IsLeader() {
					break
				}
				circuits := h.engine.registry.GetMatching(ctx, h.tags...)
				if len(circuits) > 0 {
					h.mu.Lock()
					h.circuit = circuits[0]
					h.activeCircuits = []string{circuits[0].ID}
					h.status = StatusLive
					h.mu.Unlock()
					return
				}
			}
			if h.elector.IsLeader() {
				break
			}
		}
	}

	// Aggregate schemas, profiles, and sinks
	schemas := make(map[string]*assay.SchemaNode)
	for _, tag := range h.tags {
		if sc, err := h.engine.schemaTap.GetSchema(ctx, tag); err == nil && sc != nil {
			schemas[tag] = sc
		}
	}

	profiles := make(map[string]*profiler.StreamProfile)
	for _, tag := range h.tags {
		if prof, err := h.engine.profiler.GetProfile(ctx, tag); err == nil && prof != nil {
			profiles[tag] = prof
		}
	}

	sinks := h.engine.sinks.ListDescriptors()

	req := internalautopilot.SynthesisRequest{
		Prompt:   h.prompt,
		Tags:     h.tags,
		Schemas:  schemas,
		Profiles: profiles,
		Sinks:    sinks,
	}

	synth, err := internalautopilot.NewSynthesizer(h.provider,
		internalautopilot.WithMaxRetries(h.cfg.MaxReflectionRetries),
	)
	if err != nil {
		h.fail(fmt.Errorf("flux autopilot: failed to init synthesizer: %w", err))
		return
	}

	synthRes, err := synth.SynthesizeWithReflection(ctx, req)
	if err != nil {
		h.fail(fmt.Errorf("flux autopilot: circuit synthesis failed: %w", err))
		return
	}
	candidate := synthRes.Circuit

	if candidate == nil {
		h.fail(errors.New("flux autopilot: synthesized circuit is nil"))
		return
	}

	if candidate.ID == "" {
		randomSuffix := make([]byte, 4)
		_, _ = rand.Read(randomSuffix)
		candidate.ID = fmt.Sprintf("autopilot-%s", hex.EncodeToString(randomSuffix))
	}

	// 4. Shadow Verification Phase
	stager := internalautopilot.NewStager(h.engine.registry,
		internalautopilot.WithEvaluationEvents(h.cfg.ShadowEvaluationEvents),
		internalautopilot.WithMinTargetFiringRate(h.cfg.MinTargetFiringRate),
		internalautopilot.WithMaxTargetFiringRate(h.cfg.MaxTargetFiringRate),
		internalautopilot.WithAlertStormThreshold(h.cfg.AlertStormThreshold),
		internalautopilot.WithAutoPromote(h.cfg.AutoPromotion),
	)

	var stagingRes *internalautopilot.StagingResult

	if len(h.cfg.EvaluationBatch) > 0 {
		h.setStatus(StatusShadowing)
		// Offline evaluation against batch events
		res, err := stager.EvaluateBatch(ctx, candidate, h.tags, h.engine.executor, h.cfg.EvaluationBatch)
		if err != nil {
			h.fail(fmt.Errorf("flux autopilot: evaluation batch failed: %w", err))
			return
		}
		stagingRes = res
	} else {
		sess, err := stager.StartSession(ctx, candidate, h.tags)
		if err != nil {
			h.fail(fmt.Errorf("flux autopilot: failed to start staging session: %w", err))
			return
		}

		// Online shadow observation via live Spark events
		completedCh := make(chan *internalautopilot.StagingResult, 1)

		unreg := h.engine.addSparkObserver(func(payload any, tags []string, res *types.SparkResult) {
			if sess.IsComplete() {
				return
			}
			tagMatches := false
			for _, t := range tags {
				if slices.Contains(h.tags, t) {
					tagMatches = true
					break
				}
			}
			if !tagMatches && len(h.tags) > 0 {
				return
			}

			fired := res != nil && res.ShadowFirings > 0
			done, sres, _ := sess.RecordObservation(ctx, payload, fired)
			if done && sres != nil {
				select {
				case completedCh <- sres:
				default:
				}
			}
		})

		// Notify status after observer is fully registered
		h.setStatus(StatusShadowing)

		select {
		case <-ctx.Done():
			unreg()
			h.setStatus(StatusStopped)
			return
		case sres := <-completedCh:
			unreg()
			stagingRes = sres
		}
	}

	h.mu.Lock()
	h.stagingResult = stagingRes
	h.mu.Unlock()

	if stagingRes == nil || !stagingRes.Promoted {
		reason := "safety envelope check failed"
		if stagingRes != nil && stagingRes.RejectionReason != "" {
			reason = stagingRes.RejectionReason
		}
		h.fail(fmt.Errorf("flux autopilot: candidate circuit rejected during shadow staging: %s", reason))
		return
	}

	// 5. Promoted to Live & Continuous Refinement Phase
	h.mu.Lock()
	h.circuit = candidate
	h.activeCircuits = []string{candidate.ID}
	h.status = StatusLive
	h.mu.Unlock()

	// Launch continuous refinement
	refiner := internalautopilot.NewRefiner(h.provider, h.engine.registry, h.engine.history,
		internalautopilot.WithRefinementInterval(h.cfg.RefinementInterval),
		internalautopilot.WithRefinerElector(h.elector),
		internalautopilot.WithRefinerExecutor(h.engine.executor),
	)
	refiner.TrackCircuit(candidate.ID, h.prompt, h.tags)
	_ = refiner.Start(ctx)

	h.mu.Lock()
	h.refiner = refiner
	h.mu.Unlock()

	// Keep session running in background until cancelled
	<-ctx.Done()
	h.setStatus(StatusStopped)
}
