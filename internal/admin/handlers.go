package admin

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cuprite-io/flux/internal/history"
	"github.com/cuprite-io/flux/types"
)

// registerRoutes configures the HTTP routing table for the control plane API.
func (s *Server) registerRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/status", s.handleStatus)
	mux.HandleFunc("/api/v1/circuits", s.handleCircuits)
	mux.HandleFunc("/api/v1/autopilot", s.handleAutopilot)
	mux.HandleFunc("/api/v1/alerts/history", s.handleAlertHistory)
	mux.HandleFunc("/api/v1/alerts/stream", s.handleAlertStream)
	mux.HandleFunc("/api/v1/feedback", s.handleFeedback)
	mux.HandleFunc("/api/v1/eval", s.handleEval)
}

// authMiddleware enforces optional Bearer token authentication and decorates headers.
func (s *Server) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Set Flux identity and telemetry headers on every response
		w.Header().Set("X-Flux-Admin", "true")
		w.Header().Set("X-Flux-Engine", "true")
		w.Header().Set("X-Flux-Version", s.engine.Version())

		if s.cfg.Token != "" {
			authHeader := r.Header.Get("Authorization")
			token := strings.TrimSpace(strings.TrimPrefix(authHeader, "Bearer"))
			if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") || token != s.cfg.Token {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				_ = json.NewEncoder(w).Encode(map[string]string{
					"error": "authentication required: invalid or missing bearer token",
				})
				return
			}
		}

		next.ServeHTTP(w, r)
	})
}

// handleStatus handles GET /api/v1/status.
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	activeNodes := s.engine.ActiveNodes()
	circuits, _ := s.engine.ListCircuits(r.Context())
	autopilotSessions := s.engine.GetAutopilotSessions(r.Context())

	resp := StatusResponse{
		Service:         "flux-admin",
		Version:         s.engine.Version(),
		Status:          "healthy",
		NodeID:          s.engine.NodeID(),
		LeaderID:        s.engine.LeaderID(),
		IsLeader:        s.engine.IsLeader(),
		ClusterNodes:    len(activeNodes),
		ActiveNodes:     activeNodes,
		ActiveCircuits:  len(circuits),
		ActiveAutopilot: len(autopilotSessions),
		Timestamp:       time.Now().UTC(),
	}

	s.writeJSON(w, http.StatusOK, resp)
}

// handleCircuits handles GET /api/v1/circuits and GET /api/v1/circuits?id=<circuit_id>.
func (s *Server) handleCircuits(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	targetID := r.URL.Query().Get("id")
	if targetID != "" {
		circuit, err := s.engine.GetCircuit(r.Context(), targetID)
		if err != nil || circuit == nil {
			s.writeJSONError(w, http.StatusNotFound, fmt.Sprintf("circuit %q not found", targetID))
			return
		}
		summary := mapCircuitSummary(circuit)
		summary.Definition = circuit
		s.writeJSON(w, http.StatusOK, summary)
		return
	}

	circuits, err := s.engine.ListCircuits(r.Context())
	if err != nil {
		s.writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("failed to list circuits: %v", err))
		return
	}

	summaries := make([]CircuitSummary, 0, len(circuits))
	for _, c := range circuits {
		if c != nil {
			summaries = append(summaries, mapCircuitSummary(c))
		}
	}

	s.writeJSON(w, http.StatusOK, summaries)
}

// handleAutopilot handles GET /api/v1/autopilot.
func (s *Server) handleAutopilot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	sessions := s.engine.GetAutopilotSessions(r.Context())
	if sessions == nil {
		sessions = []AutopilotSessionInfo{}
	}
	s.writeJSON(w, http.StatusOK, sessions)
}

// handleAlertHistory handles GET /api/v1/alerts/history?circuit_id=<id>&limit=<n>.
func (s *Server) handleAlertHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	circuitID := r.URL.Query().Get("circuit_id")
	limit := 50
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if parsed, err := strconv.Atoi(limitStr); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	if limit > 1000 {
		limit = 1000
	}

	alerts, err := s.engine.GetAlertHistory(r.Context(), circuitID, limit)
	if err != nil {
		s.writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("failed to get alert history: %v", err))
		return
	}
	if alerts == nil {
		alerts = []history.AlertRecord{}
	}

	s.writeJSON(w, http.StatusOK, alerts)
}

// handleAlertStream handles GET /api/v1/alerts/stream (Server-Sent Events).
func (s *Server) handleAlertStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		s.writeJSONError(w, http.StatusInternalServerError, "streaming unsupported by server")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	subscriber := s.broker.Subscribe()
	defer s.broker.Unsubscribe(subscriber)

	// Keep-alive heartbeat ping every 15 seconds
	pingTicker := time.NewTicker(15 * time.Second)
	defer pingTicker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return

		case evt, ok := <-subscriber:
			if !ok {
				return
			}
			data, err := json.Marshal(evt)
			if err != nil {
				continue
			}
			_, _ = fmt.Fprintf(w, "event: alert\ndata: %s\n\n", data)
			flusher.Flush()

		case <-pingTicker.C:
			_, _ = fmt.Fprint(w, ": ping\n\n")
			flusher.Flush()
		}
	}
}

// handleFeedback handles POST /api/v1/feedback.
func (s *Server) handleFeedback(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 65536))
	if err != nil {
		s.writeJSONError(w, http.StatusBadRequest, "failed to read request body")
		return
	}

	var req FeedbackRequest
	if err := json.Unmarshal(body, &req); err != nil {
		s.writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON payload: %v", err))
		return
	}

	if req.CircuitID == "" || req.AlertID == "" {
		s.writeJSONError(w, http.StatusBadRequest, "circuit_id and alert_id are required")
		return
	}

	switch req.FeedbackType {
	case history.FeedbackValid, history.FeedbackFalsePositive, history.FeedbackNoisy, history.FeedbackMuted:
		// Valid types
	default:
		s.writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("invalid feedback_type %q; must be valid, false_positive, noisy, or muted", req.FeedbackType))
		return
	}

	fb := history.Feedback{
		Classification: req.FeedbackType,
		Reason:         req.Notes,
		Author:         req.Operator,
		UpdatedAt:      time.Now().UTC(),
	}

	if err := s.engine.RecordAlertFeedback(r.Context(), req.CircuitID, req.AlertID, fb); err != nil {
		s.writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("failed to record feedback: %v", err))
		return
	}

	s.writeJSON(w, http.StatusOK, FeedbackResponse{
		Status:    "recorded",
		AlertID:   req.AlertID,
		Timestamp: fb.UpdatedAt,
	})
}

// handleEval handles POST /api/v1/eval.
func (s *Server) handleEval(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1048576)) // 1MB limit for ad-hoc dry-run DAGs
	if err != nil {
		s.writeJSONError(w, http.StatusBadRequest, "failed to read request body")
		return
	}

	var req EvalRequest
	if err := json.Unmarshal(body, &req); err != nil {
		s.writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON payload: %v", err))
		return
	}

	res, err := s.engine.DryRunEval(r.Context(), req)
	if err != nil {
		s.writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("dry run evaluation error: %v", err))
		return
	}

	s.writeJSON(w, http.StatusOK, res)
}

func (s *Server) writeJSON(w http.ResponseWriter, statusCode int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(data)
}

func (s *Server) writeJSONError(w http.ResponseWriter, statusCode int, message string) {
	s.writeJSON(w, statusCode, map[string]string{"error": message})
}

func mapCircuitSummary(c *types.Circuit) CircuitSummary {
	var rootCond string
	if c.Root != nil {
		rootCond = c.Root.Condition
	}
	return CircuitSummary{
		ID:            c.ID,
		Tags:          c.Tags,
		RootCondition: rootCond,
		NodeCount:     countNodes(c.Root),
		Status:        "LIVE",
		Revision:      c.Version,
		CreatedAt:     c.CreatedAt,
	}
}

func countNodes(n *types.Node) int {
	if n == nil {
		return 0
	}
	total := 1
	for _, child := range n.Children {
		total += countNodes(child)
	}
	return total
}
