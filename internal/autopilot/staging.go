package autopilot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/cuprite-io/flux/internal/engine"
	"github.com/cuprite-io/flux/internal/state"
	"github.com/cuprite-io/flux/types"
)

var (
	ErrSessionCompleted = errors.New("flux autopilot: staging session already completed")
	ErrNilCircuit       = errors.New("flux autopilot: nil circuit provided")
	ErrNoTargetTags     = errors.New("flux autopilot: at least one target stream tag is required")
)

// StagingStatus represents the evaluation outcome of a candidate circuit.
type StagingStatus string

const (
	StatusStaging    StagingStatus = "STAGING"
	StatusPromoted   StagingStatus = "PROMOTED"
	StatusRejected   StagingStatus = "REJECTED"
	StatusAlertStorm StagingStatus = "ALERT_STORM"
	StatusZeroFiring StagingStatus = "ZERO_FIRING"
)

// StagingConfig defines the safety envelope and evaluation budget for shadow staging.
type StagingConfig struct {
	EvaluationEvents    int     // Number of events to observe (default: 200)
	MinTargetFiringRate float64 // Minimum acceptable firing rate (default: 0.0001, 0.01%)
	MaxTargetFiringRate float64 // Maximum target firing rate (default: 0.02, 2.0%)
	AlertStormThreshold float64 // Alert storm hard rejection limit (default: 0.05, 5.0%)
	AutoPromote         bool    // Automatically promote to Live in Registry if within bounds (default: true)
}

// DefaultStagingConfig returns the default production configuration.
func DefaultStagingConfig() StagingConfig {
	return StagingConfig{
		EvaluationEvents:    200,
		MinTargetFiringRate: 0.0001,
		MaxTargetFiringRate: 0.02,
		AlertStormThreshold: 0.05,
		AutoPromote:         true,
	}
}

// StagingOption configures a Stager instance.
type StagingOption func(*StagingConfig)

// WithEvaluationEvents sets the number of events to observe before evaluating firing rate.
func WithEvaluationEvents(n int) StagingOption {
	return func(c *StagingConfig) {
		if n > 0 {
			c.EvaluationEvents = n
		}
	}
}

// WithMinTargetFiringRate sets the minimum acceptable firing rate.
func WithMinTargetFiringRate(rate float64) StagingOption {
	return func(c *StagingConfig) {
		c.MinTargetFiringRate = rate
	}
}

// WithMaxTargetFiringRate sets the upper bound of acceptable firing rate.
func WithMaxTargetFiringRate(rate float64) StagingOption {
	return func(c *StagingConfig) {
		c.MaxTargetFiringRate = rate
	}
}

// WithAlertStormThreshold sets the hard threshold above which a candidate is rejected as an alert storm.
func WithAlertStormThreshold(rate float64) StagingOption {
	return func(c *StagingConfig) {
		c.AlertStormThreshold = rate
	}
}

// WithAutoPromote configures whether safe circuits are automatically promoted to Live in Registry.
func WithAutoPromote(enable bool) StagingOption {
	return func(c *StagingConfig) {
		c.AutoPromote = enable
	}
}

// StagingResult encapsulates the final verdict of candidate circuit shadow evaluation.
type StagingResult struct {
	CircuitID       string        `json:"circuit_id"`
	TotalEvents     int           `json:"total_events"`
	FiringEvents    int           `json:"firing_events"`
	ErrorEvents     int           `json:"error_events"`
	ObservedRate    float64       `json:"observed_rate"`
	Status          StagingStatus `json:"status"`
	RejectionReason string        `json:"rejection_reason,omitempty"`
	Promoted        bool          `json:"promoted"`
}

// CircuitRegistry abstracts the circuit storage needed for staging and promotion.
type CircuitRegistry interface {
	Put(ctx context.Context, circuit *types.Circuit) error
	Get(ctx context.Context, id string) (*types.Circuit, error)
	Delete(ctx context.Context, id string) error
}

// Stager coordinates candidate circuit evaluation, safety verification, and automatic promotion.
type Stager struct {
	registry CircuitRegistry
	config   StagingConfig
}

// NewStager creates a new candidate circuit Stager.
func NewStager(reg CircuitRegistry, opts ...StagingOption) *Stager {
	cfg := DefaultStagingConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	return &Stager{
		registry: reg,
		config:   cfg,
	}
}

// Config returns a copy of the stager configuration.
func (s *Stager) Config() StagingConfig {
	return s.config
}

// StagingSession tracks live shadow traffic observation for a single candidate circuit.
type StagingSession struct {
	stager       *Stager
	circuit      *types.Circuit
	targetTags   []string
	shadowTag    string
	totalEvents  int64
	errorEvents  int64
	firingEvents int64
	completed    uint32
	mu           sync.RWMutex
	result       *StagingResult
}

// StartSession registers a candidate circuit in shadow mode and initiates an observation session.
func (s *Stager) StartSession(ctx context.Context, circuit *types.Circuit, targetTags []string) (*StagingSession, error) {
	if circuit == nil {
		return nil, ErrNilCircuit
	}
	if len(targetTags) == 0 {
		return nil, ErrNoTargetTags
	}

	shadowTag := "shadow:" + circuit.ID
	candidateTags := append(slices.Clone(targetTags), shadowTag)

	// Shallow clone circuit with shadow tag attached
	stagedCircuit := *circuit
	stagedCircuit.Tags = candidateTags

	if s.registry != nil {
		if err := s.registry.Put(ctx, &stagedCircuit); err != nil {
			return nil, fmt.Errorf("flux autopilot: failed to register shadow candidate: %w", err)
		}
	}

	return &StagingSession{
		stager:     s,
		circuit:    circuit,
		targetTags: slices.Clone(targetTags),
		shadowTag:  shadowTag,
	}, nil
}

// RecordObservation records an incoming event evaluation result into the staging session.
// When the configured event budget is satisfied, it completes the session and returns the verdict.
func (sess *StagingSession) RecordObservation(ctx context.Context, payload any, fired bool) (bool, *StagingResult, error) {
	if sess.IsComplete() {
		return true, sess.Result(), ErrSessionCompleted
	}

	atomic.AddInt64(&sess.totalEvents, 1)
	if IsErrorEvent(payload) {
		atomic.AddInt64(&sess.errorEvents, 1)
	}
	if fired {
		atomic.AddInt64(&sess.firingEvents, 1)
	}

	currentTotal := atomic.LoadInt64(&sess.totalEvents)
	if int(currentTotal) >= sess.stager.config.EvaluationEvents {
		res, err := sess.Finish(ctx)
		return true, res, err
	}

	return false, nil, nil
}

// Finish concludes the observation session early or on budget, evaluating the safety envelope.
func (sess *StagingSession) Finish(ctx context.Context) (*StagingResult, error) {
	sess.mu.Lock()
	defer sess.mu.Unlock()

	if sess.result != nil {
		return sess.result, nil
	}

	atomic.StoreUint32(&sess.completed, 1)

	total := int(atomic.LoadInt64(&sess.totalEvents))
	firings := int(atomic.LoadInt64(&sess.firingEvents))
	errorsObs := int(atomic.LoadInt64(&sess.errorEvents))

	var rate float64
	if total > 0 {
		rate = float64(firings) / float64(total)
	}

	cfg := sess.stager.config
	result := &StagingResult{
		CircuitID:    sess.circuit.ID,
		TotalEvents:  total,
		FiringEvents: firings,
		ErrorEvents:  errorsObs,
		ObservedRate: rate,
		Status:       StatusStaging,
		Promoted:     false,
	}

	// 1. Safety Envelope Check: Alert Storm (> 5%)
	if rate > cfg.AlertStormThreshold {
		result.Status = StatusAlertStorm
		result.RejectionReason = fmt.Sprintf("observed firing rate %.2f%% exceeds alert storm threshold %.2f%%", rate*100, cfg.AlertStormThreshold*100)
		sess.cleanupShadowCircuit(ctx)
		sess.result = result
		return result, nil
	}

	// 2. Safety Envelope Check: Ineffective Rule (0% firings when errors occurred)
	if firings == 0 && errorsObs > 0 {
		result.Status = StatusZeroFiring
		result.RejectionReason = fmt.Sprintf("observed firing rate is 0.0%% despite %d error events observed", errorsObs)
		sess.cleanupShadowCircuit(ctx)
		sess.result = result
		return result, nil
	}

	// 3. Rate boundary checks
	if rate < cfg.MinTargetFiringRate {
		// If no errors were seen and firings == 0, check if min rate requires > 0
		result.Status = StatusRejected
		result.RejectionReason = fmt.Sprintf("observed firing rate %.4f%% is below minimum target %.4f%%", rate*100, cfg.MinTargetFiringRate*100)
		sess.cleanupShadowCircuit(ctx)
		sess.result = result
		return result, nil
	}

	if rate > cfg.MaxTargetFiringRate {
		result.Status = StatusRejected
		result.RejectionReason = fmt.Sprintf("observed firing rate %.2f%% exceeds maximum target %.2f%%", rate*100, cfg.MaxTargetFiringRate*100)
		sess.cleanupShadowCircuit(ctx)
		sess.result = result
		return result, nil
	}

	// 4. Safe bounds satisfied -> Promote to Live!
	result.Status = StatusPromoted
	if cfg.AutoPromote && sess.stager.registry != nil {
		liveCircuit := *sess.circuit
		liveCircuit.Tags = slices.Clone(sess.targetTags)
		if err := sess.stager.registry.Put(ctx, &liveCircuit); err != nil {
			return nil, fmt.Errorf("flux autopilot: failed to promote circuit to live: %w", err)
		}
		result.Promoted = true
	}

	sess.result = result
	return result, nil
}

func (sess *StagingSession) cleanupShadowCircuit(ctx context.Context) {
	if sess.stager.registry != nil {
		_ = sess.stager.registry.Delete(ctx, sess.circuit.ID)
	}
}

// IsComplete returns true if the session has concluded.
func (sess *StagingSession) IsComplete() bool {
	return atomic.LoadUint32(&sess.completed) == 1
}

// TotalEvents returns the current count of observed events.
func (sess *StagingSession) TotalEvents() int {
	return int(atomic.LoadInt64(&sess.totalEvents))
}

// FiringEvents returns the current count of events that satisfied alert conditions.
func (sess *StagingSession) FiringEvents() int {
	return int(atomic.LoadInt64(&sess.firingEvents))
}

// ErrorEvents returns the count of observed error/anomaly events.
func (sess *StagingSession) ErrorEvents() int {
	return int(atomic.LoadInt64(&sess.errorEvents))
}

// CurrentFiringRate returns the current instantaneous firing rate.
func (sess *StagingSession) CurrentFiringRate() float64 {
	tot := atomic.LoadInt64(&sess.totalEvents)
	if tot == 0 {
		return 0.0
	}
	return float64(atomic.LoadInt64(&sess.firingEvents)) / float64(tot)
}

// Result returns the finalized result, or nil if still in progress.
func (sess *StagingSession) Result() *StagingResult {
	sess.mu.RLock()
	defer sess.mu.RUnlock()
	return sess.result
}

// EvaluateBatch synchronously evaluates a candidate circuit against a batch of events using an executor.
func (s *Stager) EvaluateBatch(ctx context.Context, circuit *types.Circuit, targetTags []string, exec *engine.Executor, events []any) (*StagingResult, error) {
	sess, err := s.StartSession(ctx, circuit, targetTags)
	if err != nil {
		return nil, err
	}

	for _, evt := range events {
		sctx := state.AcquireContext(ctx, evt)
		sctx.SetShadowMode(true)

		res, errExec := exec.ExecuteCircuit(ctx, circuit, sctx)
		fired := false
		if errExec == nil && res != nil && res.ShadowFirings > 0 {
			fired = true
		}
		state.ReleaseContext(sctx)

		done, result, errObs := sess.RecordObservation(ctx, evt, fired)
		if errObs != nil && !errors.Is(errObs, ErrSessionCompleted) {
			return nil, errObs
		}
		if done {
			return result, nil
		}
	}

	// If finished events without reaching max budget, conclude with what was observed
	return sess.Finish(ctx)
}

// IsErrorEvent inspects an event payload for common error, exception, or 5xx status indicators.
func IsErrorEvent(payload any) bool {
	if payload == nil {
		return false
	}
	switch p := payload.(type) {
	case map[string]any:
		return isErrorMap(p)
	case []byte:
		var m map[string]any
		if err := json.Unmarshal(p, &m); err == nil {
			return isErrorMap(m)
		}
	case string:
		var m map[string]any
		if err := json.Unmarshal([]byte(p), &m); err == nil {
			return isErrorMap(m)
		}
	default:
		v := reflect.ValueOf(payload)
		if v.Kind() == reflect.Ptr {
			if v.IsNil() {
				return false
			}
			v = v.Elem()
		}
		if v.Kind() == reflect.Struct {
			for i := 0; i < v.NumField(); i++ {
				field := v.Type().Field(i)
				if !field.IsExported() {
					continue
				}
				name := strings.ToLower(field.Name)
				val := v.Field(i).Interface()
				if name == "status" || name == "statuscode" || name == "status_code" || name == "httpstatus" {
					if code, ok := toInt64(val); ok && code >= 500 {
						return true
					}
				}
				if name == "level" || name == "severity" || name == "loglevel" {
					str := strings.ToLower(fmt.Sprint(val))
					if isErrorLevel(str) {
						return true
					}
				}
				if name == "error" || name == "err" || name == "exception" {
					if !v.Field(i).IsZero() {
						return true
					}
				}
			}
		}
	}
	return false
}

func isErrorMap(m map[string]any) bool {
	for k, v := range m {
		key := strings.ToLower(k)
		switch key {
		case "status", "status_code", "statuscode", "http_status":
			if code, ok := toInt64(v); ok && code >= 500 {
				return true
			}
		case "level", "severity", "log_level":
			str := strings.ToLower(fmt.Sprint(v))
			if isErrorLevel(str) {
				return true
			}
		case "error", "err", "exception":
			if v != nil {
				if s, ok := v.(string); ok && s != "" {
					return true
				} else if _, ok := v.(error); ok {
					return true
				}
			}
		}
	}
	return false
}

func isErrorLevel(str string) bool {
	return str == "error" || str == "fatal" || str == "critical" || str == "crit" || str == "panic" || str == "err"
}

func toInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int64:
		return n, true
	case float64:
		return int64(n), true
	case float32:
		return int64(n), true
	case int32:
		return int64(n), true
	default:
		return 0, false
	}
}
