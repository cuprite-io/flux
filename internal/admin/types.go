package admin

import (
	"context"
	"time"

	"github.com/cuprite-io/flux/internal/history"
	"github.com/cuprite-io/flux/types"
)

// Config configures the Flux Engine Admin control plane HTTP server.
type Config struct {
	Addr  string // Listen address, e.g. ":9090", "127.0.0.1:9090"
	Token string // Optional Bearer token secret for authentication
}

// DefaultConfig returns production default Admin configuration.
func DefaultConfig() Config {
	return Config{
		Addr: ":9090",
	}
}

// StatusResponse is the payload returned by GET /api/v1/status.
type StatusResponse struct {
	Service         string    `json:"service"` // "flux-admin"
	Version         string    `json:"version"`
	Status          string    `json:"status"` // "healthy"
	NodeID          string    `json:"node_id,omitempty"`
	LeaderID        string    `json:"leader_id,omitempty"`
	IsLeader        bool      `json:"is_leader"`
	ClusterNodes    int       `json:"cluster_nodes"`
	ActiveNodes     []string  `json:"active_nodes,omitempty"`
	ActiveCircuits  int       `json:"active_circuits"`
	ActiveAutopilot int       `json:"active_autopilot"`
	Timestamp       time.Time `json:"timestamp"`
}

// CircuitSummary is the concise metadata returned by GET /api/v1/circuits.
type CircuitSummary struct {
	ID            string         `json:"id"`
	Tags          []string       `json:"tags,omitempty"`
	RootCondition string         `json:"root_condition,omitempty"`
	NodeCount     int            `json:"node_count"`
	Status        string         `json:"status"` // "LIVE" or "SHADOW"
	Revision      uint32         `json:"revision"`
	CreatedAt     time.Time      `json:"created_at"`
	Definition    *types.Circuit `json:"definition,omitempty"`
}

// AutopilotStagingInfo captures metrics from shadow staging evaluation.
type AutopilotStagingInfo struct {
	TotalEvents     int     `json:"total_events"`
	FiringEvents    int     `json:"firing_events"`
	ErrorEvents     int     `json:"error_events"`
	ObservedRate    float64 `json:"observed_rate"`
	Status          string  `json:"status"`
	RejectionReason string  `json:"rejection_reason,omitempty"`
	Promoted        bool    `json:"promoted"`
}

// AutopilotSessionInfo captures an active or historical Autopilot synthesis session.
type AutopilotSessionInfo struct {
	SessionID      string                `json:"session_id"`
	Prompt         string                `json:"prompt"`
	Status         string                `json:"status"` // "INGESTING", "COMPILING", "STAGING", "REFINING", "ACTIVE", "ERROR"
	Tags           []string              `json:"tags,omitempty"`
	ActiveCircuits []string              `json:"active_circuits,omitempty"`
	StagingResult  *AutopilotStagingInfo `json:"staging_result,omitempty"`
	LastError      string                `json:"last_error,omitempty"`
}

// FeedbackRequest is the payload submitted to POST /api/v1/feedback.
type FeedbackRequest struct {
	CircuitID    string `json:"circuit_id"`
	AlertID      string `json:"alert_id"`
	FeedbackType string `json:"feedback_type"` // "valid", "false_positive", "noisy", "muted"
	Notes        string `json:"notes,omitempty"`
	Operator     string `json:"operator,omitempty"`
}

// FeedbackResponse is the response returned by POST /api/v1/feedback.
type FeedbackResponse struct {
	Status    string    `json:"status"` // "recorded"
	AlertID   string    `json:"alert_id"`
	Timestamp time.Time `json:"timestamp"`
}

// EvalRequest is the payload submitted to POST /api/v1/eval.
type EvalRequest struct {
	Circuit   *types.Circuit `json:"circuit,omitempty"`
	CircuitID string         `json:"circuit_id,omitempty"`
	Tags      []string       `json:"tags,omitempty"`
	Payload   any            `json:"payload"`
	Context   map[string]any `json:"context,omitempty"`
	EntityID  string         `json:"entity_id,omitempty"`
}

// EvalResponse is the response returned by POST /api/v1/eval.
type EvalResponse struct {
	Passed           bool           `json:"passed"`
	ExecutedCircuits []string       `json:"executed_circuits"`
	ReturnedData     map[string]any `json:"returned_data,omitempty"`
	ShadowFirings    int            `json:"shadow_firings,omitempty"`
	DurationMicro    int64          `json:"duration_us"`
	Errors           []string       `json:"errors,omitempty"`
}

// AlertEvent represents a real-time alert dispatched over SSE (/api/v1/alerts/stream).
type AlertEvent struct {
	ID        string         `json:"id"`
	CircuitID string         `json:"circuit_id"`
	NodeName  string         `json:"node_name"`
	SinkName  string         `json:"sink_name"`
	Condition string         `json:"condition"`
	Payload   map[string]any `json:"payload"`
	Timestamp time.Time      `json:"timestamp"`
}

// CacheKeyInfo is the metadata returned for keys in GET /api/v1/cache/keys.
type CacheKeyInfo struct {
	Key       string        `json:"key"`
	Type      string        `json:"type"` // "string", "map", "set", "window"
	Size      int           `json:"size"`
	TTL       time.Duration `json:"ttl"`
	ExpiresAt time.Time     `json:"expires_at,omitempty"`
}

// CacheKeyDetail is the complete inspection data returned by GET /api/v1/cache/item.
type CacheKeyDetail struct {
	Key       string        `json:"key"`
	Type      string        `json:"type"`
	Size      int           `json:"size"`
	TTL       time.Duration `json:"ttl"`
	ExpiresAt time.Time     `json:"expires_at,omitempty"`
	Value     any           `json:"value"`
}

// EngineContext defines the capabilities required from the Engine by the Admin server.
// Flux Engine implements this interface, maintaining zero duplicate in-memory state.
type EngineContext interface {
	Version() string
	NodeID() string
	LeaderID() string
	IsLeader() bool
	ActiveNodes() []string
	ListCircuits(ctx context.Context) ([]*types.Circuit, error)
	GetCircuit(ctx context.Context, id string) (*types.Circuit, error)
	PutCircuit(ctx context.Context, circuit *types.Circuit) error
	GetAlertHistory(ctx context.Context, circuitID string, limit int) ([]history.AlertRecord, error)
	RecordAlertFeedback(ctx context.Context, circuitID, alertID string, fb history.Feedback) error
	GetAutopilotSessions(ctx context.Context) []AutopilotSessionInfo
	DryRunEval(ctx context.Context, req EvalRequest) (*EvalResponse, error)
	RegisterAlertObserver(obs func(evt AlertEvent)) func()
	ScanCacheKeys(ctx context.Context, prefix string, limit int) ([]CacheKeyInfo, error)
	InspectCacheKey(ctx context.Context, key string) (*CacheKeyDetail, error)
}
