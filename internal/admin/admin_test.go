package admin_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cuprite-io/flux/internal/admin"
	"github.com/cuprite-io/flux/internal/history"
	"github.com/cuprite-io/flux/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockEngine struct {
	mu            sync.Mutex
	version       string
	nodeID        string
	leaderID      string
	isLeader      bool
	activeNodes   []string
	circuits      map[string]*types.Circuit
	alerts        []history.AlertRecord
	autopilot     []admin.AutopilotSessionInfo
	observers     map[uint64]func(admin.AlertEvent)
	nextObsID     uint64
	feedbackCalls []history.Feedback
}

func newMockEngine() *mockEngine {
	return &mockEngine{
		version:     "0.22.0",
		nodeID:      "node-1",
		leaderID:    "node-1",
		isLeader:    true,
		activeNodes: []string{"node-1", "node-2"},
		circuits:    make(map[string]*types.Circuit),
		observers:   make(map[uint64]func(admin.AlertEvent)),
	}
}

func (m *mockEngine) Version() string { return m.version }
func (m *mockEngine) NodeID() string  { return m.nodeID }
func (m *mockEngine) LeaderID() string { return m.leaderID }
func (m *mockEngine) IsLeader() bool  { return m.isLeader }
func (m *mockEngine) ActiveNodes() []string { return m.activeNodes }

func (m *mockEngine) ListCircuits(ctx context.Context) ([]*types.Circuit, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	res := make([]*types.Circuit, 0, len(m.circuits))
	for _, c := range m.circuits {
		res = append(res, c)
	}
	return res, nil
}

func (m *mockEngine) GetCircuit(ctx context.Context, id string) (*types.Circuit, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.circuits[id]; ok {
		return c, nil
	}
	return nil, fmt.Errorf("circuit %s not found", id)
}

func (m *mockEngine) GetAlertHistory(ctx context.Context, circuitID string, limit int) ([]history.AlertRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var res []history.AlertRecord
	for _, a := range m.alerts {
		if circuitID == "" || a.CircuitID == circuitID {
			res = append(res, a)
			if len(res) >= limit {
				break
			}
		}
	}
	return res, nil
}

func (m *mockEngine) RecordAlertFeedback(ctx context.Context, circuitID, alertID string, fb history.Feedback) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.feedbackCalls = append(m.feedbackCalls, fb)
	return nil
}

func (m *mockEngine) GetAutopilotSessions(ctx context.Context) []admin.AutopilotSessionInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.autopilot
}

func (m *mockEngine) DryRunEval(ctx context.Context, req admin.EvalRequest) (*admin.EvalResponse, error) {
	return &admin.EvalResponse{
		Passed:           true,
		ExecutedCircuits: []string{"eval_test"},
		ReturnedData:     map[string]any{"evaluated": true},
		DurationMicro:    42,
	}, nil
}

func (m *mockEngine) RegisterAlertObserver(obs func(evt admin.AlertEvent)) func() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextObsID++
	id := m.nextObsID
	m.observers[id] = obs
	return func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		delete(m.observers, id)
	}
}

func (m *mockEngine) fireAlert(evt admin.AlertEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, obs := range m.observers {
		obs(evt)
	}
}

func TestAdminServer_StatusAndHandshake(t *testing.T) {
	mockEng := newMockEngine()
	srv, err := admin.NewServer(mockEng, admin.Config{Addr: "127.0.0.1:0"})
	require.NoError(t, err)

	err = srv.Start()
	require.NoError(t, err)
	defer srv.Stop(context.Background())

	resp, err := http.Get(srv.URL() + "/api/v1/status")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "true", resp.Header.Get("X-Flux-Admin"))
	assert.Equal(t, "true", resp.Header.Get("X-Flux-Engine"))
	assert.Equal(t, "0.22.0", resp.Header.Get("X-Flux-Version"))

	var status admin.StatusResponse
	err = json.NewDecoder(resp.Body).Decode(&status)
	require.NoError(t, err)

	assert.Equal(t, "flux-admin", status.Service)
	assert.Equal(t, "0.22.0", status.Version)
	assert.Equal(t, "node-1", status.NodeID)
	assert.Equal(t, "node-1", status.LeaderID)
	assert.True(t, status.IsLeader)
	assert.Equal(t, 2, status.ClusterNodes)
}

func TestAdminServer_AuthMiddleware(t *testing.T) {
	mockEng := newMockEngine()
	srv, err := admin.NewServer(mockEng, admin.Config{
		Addr:  "127.0.0.1:0",
		Token: "super-secret-token",
	})
	require.NoError(t, err)
	require.NoError(t, srv.Start())
	defer srv.Stop(context.Background())

	// 1. Request without token -> 401 Unauthorized
	req, _ := http.NewRequest(http.MethodGet, srv.URL()+"/api/v1/status", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	assert.Equal(t, "true", resp.Header.Get("X-Flux-Admin"))
	resp.Body.Close()

	// 2. Request with bad token -> 401 Unauthorized
	req, _ = http.NewRequest(http.MethodGet, srv.URL()+"/api/v1/status", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	resp.Body.Close()

	// 3. Request with valid token -> 200 OK
	req, _ = http.NewRequest(http.MethodGet, srv.URL()+"/api/v1/status", nil)
	req.Header.Set("Authorization", "Bearer super-secret-token")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()
}

func TestAdminServer_CircuitsEndpoints(t *testing.T) {
	mockEng := newMockEngine()
	c1 := types.NewCircuit("fraud_detection")
	c1.Tags = []string{"stream:payments"}
	c1.Root = types.NewNode("root").WithCondition("amount > 1000")
	mockEng.circuits[c1.ID] = c1

	srv, err := admin.NewServer(mockEng, admin.Config{Addr: "127.0.0.1:0"})
	require.NoError(t, err)
	require.NoError(t, srv.Start())
	defer srv.Stop(context.Background())

	// List circuits
	resp, err := http.Get(srv.URL() + "/api/v1/circuits")
	require.NoError(t, err)
	defer resp.Body.Close()

	var summaries []admin.CircuitSummary
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&summaries))
	require.Len(t, summaries, 1)
	assert.Equal(t, "fraud_detection", summaries[0].ID)
	assert.Equal(t, "amount > 1000", summaries[0].RootCondition)
	assert.Equal(t, 1, summaries[0].NodeCount)

	// Get specific circuit
	resp2, err := http.Get(srv.URL() + "/api/v1/circuits?id=fraud_detection")
	require.NoError(t, err)
	defer resp2.Body.Close()

	var single admin.CircuitSummary
	require.NoError(t, json.NewDecoder(resp2.Body).Decode(&single))
	assert.Equal(t, "fraud_detection", single.ID)
	require.NotNil(t, single.Definition)

	// Non-existent circuit
	resp3, err := http.Get(srv.URL() + "/api/v1/circuits?id=unknown")
	require.NoError(t, err)
	defer resp3.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp3.StatusCode)
}

func TestAdminServer_FeedbackAndEval(t *testing.T) {
	mockEng := newMockEngine()
	srv, err := admin.NewServer(mockEng, admin.Config{Addr: "127.0.0.1:0"})
	require.NoError(t, err)
	require.NoError(t, srv.Start())
	defer srv.Stop(context.Background())

	// Feedback invalid type
	fbBad := `{"circuit_id":"c1","alert_id":"a1","feedback_type":"invalid_type"}`
	resp, err := http.Post(srv.URL()+"/api/v1/feedback", "application/json", strings.NewReader(fbBad))
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	resp.Body.Close()

	// Feedback valid type
	fbGood := `{"circuit_id":"c1","alert_id":"a1","feedback_type":"false_positive","notes":"Test alert"}`
	resp, err = http.Post(srv.URL()+"/api/v1/feedback", "application/json", strings.NewReader(fbGood))
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var fbRes admin.FeedbackResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&fbRes))
	assert.Equal(t, "recorded", fbRes.Status)
	assert.Equal(t, "a1", fbRes.AlertID)
	resp.Body.Close()

	// Eval
	evalPayload := `{"payload":{"amount":500},"tags":["stream:payments"]}`
	resp, err = http.Post(srv.URL()+"/api/v1/eval", "application/json", strings.NewReader(evalPayload))
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var evalRes admin.EvalResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&evalRes))
	assert.True(t, evalRes.Passed)
	resp.Body.Close()
}

func TestAdminServer_AlertStreamSSE(t *testing.T) {
	mockEng := newMockEngine()
	srv, err := admin.NewServer(mockEng, admin.Config{Addr: "127.0.0.1:0"})
	require.NoError(t, err)
	require.NoError(t, srv.Start())
	defer srv.Stop(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL()+"/api/v1/alerts/stream", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))

	// Trigger alert in background
	go func() {
		time.Sleep(50 * time.Millisecond)
		mockEng.fireAlert(admin.AlertEvent{
			ID:        "alert-99",
			CircuitID: "c_fraud",
			SinkName:  "slack",
			Timestamp: time.Now(),
		})
	}()

	reader := bufio.NewReader(resp.Body)
	var eventFound bool
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		if strings.HasPrefix(line, "data:") {
			assert.Contains(t, line, "alert-99")
			assert.Contains(t, line, "c_fraud")
			eventFound = true
			break
		}
	}
	assert.True(t, eventFound, "should receive alert event over SSE stream")
}
