package client

import (
	"time"

	"github.com/cuprite-io/flux/types"
)

// StatusResponse is the server status payload returned by the Admin control plane.
type StatusResponse struct {
	Service         string    `json:"service"`
	Version         string    `json:"version"`
	Status          string    `json:"status"`
	NodeID          string    `json:"node_id,omitempty"`
	LeaderID        string    `json:"leader_id,omitempty"`
	IsLeader        bool      `json:"is_leader"`
	ClusterNodes    int       `json:"cluster_nodes"`
	ActiveNodes     []string  `json:"active_nodes,omitempty"`
	ActiveCircuits  int       `json:"active_circuits"`
	ActiveAutopilot int       `json:"active_autopilot"`
	Timestamp       time.Time `json:"timestamp"`
}

// CircuitSummary provides concise inspection details of a deployed circuit.
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

// AutopilotStagingInfo captures verification metrics from shadow staging.
type AutopilotStagingInfo struct {
	TotalEvents     int     `json:"total_events"`
	FiringEvents    int     `json:"firing_events"`
	ErrorEvents     int     `json:"error_events"`
	ObservedRate    float64 `json:"observed_rate"`
	Status          string  `json:"status"`
	RejectionReason string  `json:"rejection_reason,omitempty"`
	Promoted        bool    `json:"promoted"`
}

// AutopilotSession represents an autonomous AI synthesis session.
type AutopilotSession struct {
	SessionID      string                `json:"session_id"`
	Prompt         string                `json:"prompt"`
	Status         string                `json:"status"`
	Tags           []string              `json:"tags,omitempty"`
	ActiveCircuits []string              `json:"active_circuits,omitempty"`
	StagingResult  *AutopilotStagingInfo `json:"staging_result,omitempty"`
	LastError      string                `json:"last_error,omitempty"`
}

// AlertRecord represents a historical alert record.
type AlertRecord struct {
	ID        string         `json:"id"`
	CircuitID string         `json:"circuit_id"`
	NodeName  string         `json:"node_name"`
	SinkName  string         `json:"sink_name"`
	Condition string         `json:"condition,omitempty"`
	Payload   map[string]any `json:"payload,omitempty"`
	Feedback  *FeedbackInfo  `json:"feedback,omitempty"`
}

// FeedbackInfo represents operator classification on an alert.
type FeedbackInfo struct {
	Classification string    `json:"classification"`
	Reason         string    `json:"reason,omitempty"`
	Author         string    `json:"author,omitempty"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// FeedbackRequest is submitted to report operator feedback on an alert.
type FeedbackRequest struct {
	CircuitID    string `json:"circuit_id"`
	AlertID      string `json:"alert_id"`
	FeedbackType string `json:"feedback_type"` // "valid", "false_positive", "noisy", "muted"
	Notes        string `json:"notes,omitempty"`
	Operator     string `json:"operator,omitempty"`
}

// FeedbackResponse is returned after recording operator feedback.
type FeedbackResponse struct {
	Status    string    `json:"status"`
	AlertID   string    `json:"alert_id"`
	Timestamp time.Time `json:"timestamp"`
}

// EvalRequest defines a dry-run execution request.
type EvalRequest struct {
	Circuit   *types.Circuit `json:"circuit,omitempty"`
	CircuitID string         `json:"circuit_id,omitempty"`
	Tags      []string       `json:"tags,omitempty"`
	Payload   any            `json:"payload"`
	Context   map[string]any `json:"context,omitempty"`
	EntityID  string         `json:"entity_id,omitempty"`
}

// EvalResponse contains the dry-run execution output.
type EvalResponse struct {
	Passed           bool           `json:"passed"`
	ExecutedCircuits []string       `json:"executed_circuits"`
	ReturnedData     map[string]any `json:"returned_data,omitempty"`
	ShadowFirings    int            `json:"shadow_firings,omitempty"`
	DurationMicro    int64          `json:"duration_us"`
	Errors           []string       `json:"errors,omitempty"`
}

// AlertEvent is a real-time event received over SSE.
type AlertEvent struct {
	ID        string         `json:"id"`
	CircuitID string         `json:"circuit_id"`
	NodeName  string         `json:"node_name"`
	SinkName  string         `json:"sink_name"`
	Condition string         `json:"condition"`
	Payload   map[string]any `json:"payload"`
	Timestamp time.Time      `json:"timestamp"`
}

// CacheKeyInfo is the metadata returned for keys in the Cache view.
type CacheKeyInfo struct {
	Key       string        `json:"key"`
	Type      string        `json:"type"` // "string", "map", "set", "window"
	Size      int           `json:"size"`
	TTL       time.Duration `json:"ttl"`
	ExpiresAt time.Time     `json:"expires_at,omitempty"`
}

// CacheKeyDetail is the complete inspection data returned for a single cache key.
type CacheKeyDetail struct {
	Key       string        `json:"key"`
	Type      string        `json:"type"`
	Size      int           `json:"size"`
	TTL       time.Duration `json:"ttl"`
	ExpiresAt time.Time     `json:"expires_at,omitempty"`
	Value     any           `json:"value"`
}

