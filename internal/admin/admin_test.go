package admin_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	cacheKeys     []admin.CacheKeyInfo
	cacheDetail   map[string]*admin.CacheKeyDetail
}

func newMockEngine() *mockEngine {
	return &mockEngine{
		version:     "0.25.0",
		nodeID:      "node-1",
		leaderID:    "node-1",
		isLeader:    true,
		activeNodes: []string{"node-1", "node-2"},
		circuits:    make(map[string]*types.Circuit),
		observers:   make(map[uint64]func(admin.AlertEvent)),
		cacheDetail: make(map[string]*admin.CacheKeyDetail),
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

func (m *mockEngine) PutCircuit(ctx context.Context, circuit *types.Circuit) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if circuit == nil || circuit.ID == "" {
		return fmt.Errorf("invalid circuit")
	}
	m.circuits[circuit.ID] = circuit
	return nil
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

func (m *mockEngine) ScanCacheKeys(ctx context.Context, prefix string, limit int) ([]admin.CacheKeyInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var res []admin.CacheKeyInfo
	for _, k := range m.cacheKeys {
		if prefix == "" || strings.HasPrefix(k.Key, prefix) {
			res = append(res, k)
		}
	}
	return res, nil
}

func (m *mockEngine) InspectCacheKey(ctx context.Context, key string) (*admin.CacheKeyDetail, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if d, ok := m.cacheDetail[key]; ok {
		return d, nil
	}
	return nil, errors.New("key not found")
}

func (m *mockEngine) GetClusterTopology(ctx context.Context) (*admin.ClusterTopologyResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var nodes []admin.ClusterNode
	for _, id := range m.activeNodes {
		nodes = append(nodes, admin.ClusterNode{
			NodeID:       id,
			Addr:         "127.0.0.1:9090",
			StreamPort:   8080,
			IsLeader:     id == m.leaderID,
			Status:       "HEALTHY",
			HeartbeatAge: 500 * time.Millisecond,
			LeaseRemain:  9500 * time.Millisecond,
			UpdatedAt:    time.Now().UTC(),
			Uptime:       "1h30m",
		})
	}
	return &admin.ClusterTopologyResponse{
		LeaderID:     m.leaderID,
		SelfNodeID:   m.nodeID,
		TotalNodes:   len(nodes),
		HealthyNodes: len(nodes),
		Nodes:        nodes,
	}, nil
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
	assert.Equal(t, "0.25.0", resp.Header.Get("X-Flux-Version"))

	var status admin.StatusResponse
	err = json.NewDecoder(resp.Body).Decode(&status)
	require.NoError(t, err)

	assert.Equal(t, "flux-admin", status.Service)
	assert.Equal(t, "0.25.0", status.Version)
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

	// Deploy / Hot-Swap new circuit via POST
	newCirc := types.NewCircuit("hot_swap_circuit").WithTags("payments:v2")
	newCirc.Root = types.NewNode("v2_root").WithCondition("payload.amount > 5000")
	newCircBytes, err := json.Marshal(newCirc)
	require.NoError(t, err)

	postResp, err := http.Post(srv.URL()+"/api/v1/circuits", "application/json", bytes.NewReader(newCircBytes))
	require.NoError(t, err)
	defer postResp.Body.Close()
	assert.Equal(t, http.StatusOK, postResp.StatusCode)

	var postSummary admin.CircuitSummary
	require.NoError(t, json.NewDecoder(postResp.Body).Decode(&postSummary))
	assert.Equal(t, "hot_swap_circuit", postSummary.ID)
	assert.Equal(t, "payload.amount > 5000", postSummary.RootCondition)
	assert.Equal(t, "hot_swap_circuit", mockEng.circuits["hot_swap_circuit"].ID)
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

func TestAdminServer_CacheEndpoints(t *testing.T) {
	mockEng := newMockEngine()
	mockEng.cacheKeys = []admin.CacheKeyInfo{
		{Key: "alert:history:fraud_sentinel", Type: "map", Size: 10},
		{Key: "entity:user:42", Type: "string", Size: 35},
		{Key: "schema:payments", Type: "set", Size: 2},
	}
	mockEng.cacheDetail["entity:user:42"] = &admin.CacheKeyDetail{
		Key:   "entity:user:42",
		Type:  "string",
		Size:  35,
		Value: `{"id":42,"role":"admin"}`,
	}

	srv, err := admin.NewServer(mockEng, admin.Config{Addr: "127.0.0.1:0"})
	require.NoError(t, err)
	require.NoError(t, srv.Start())
	defer srv.Stop(context.Background())

	// 1. GET /api/v1/cache/keys without prefix
	resp, err := http.Get(srv.URL() + "/api/v1/cache/keys")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var keys []admin.CacheKeyInfo
	err = json.NewDecoder(resp.Body).Decode(&keys)
	require.NoError(t, err)
	assert.Len(t, keys, 3)

	// 2. GET /api/v1/cache/keys?prefix=entity:
	resp2, err := http.Get(srv.URL() + "/api/v1/cache/keys?prefix=entity:")
	require.NoError(t, err)
	defer resp2.Body.Close()
	assert.Equal(t, http.StatusOK, resp2.StatusCode)

	var filteredKeys []admin.CacheKeyInfo
	err = json.NewDecoder(resp2.Body).Decode(&filteredKeys)
	require.NoError(t, err)
	assert.Len(t, filteredKeys, 1)
	assert.Equal(t, "entity:user:42", filteredKeys[0].Key)

	// 3. GET /api/v1/cache/item?key=entity:user:42
	resp3, err := http.Get(srv.URL() + "/api/v1/cache/item?key=entity:user:42")
	require.NoError(t, err)
	defer resp3.Body.Close()
	assert.Equal(t, http.StatusOK, resp3.StatusCode)

	var detail admin.CacheKeyDetail
	err = json.NewDecoder(resp3.Body).Decode(&detail)
	require.NoError(t, err)
	assert.Equal(t, "entity:user:42", detail.Key)
	assert.Equal(t, "string", detail.Type)
	assert.Equal(t, `{"id":42,"role":"admin"}`, detail.Value)

	// 4. GET /api/v1/cache/item missing key parameter -> 400
	resp4, err := http.Get(srv.URL() + "/api/v1/cache/item")
	require.NoError(t, err)
	defer resp4.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp4.StatusCode)

	// 5. GET /api/v1/cache/item non-existent key -> 404
	resp5, err := http.Get(srv.URL() + "/api/v1/cache/item?key=missing:key")
	require.NoError(t, err)
	defer resp5.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp5.StatusCode)
}

func TestAdminServer_ClusterTopology(t *testing.T) {
	mockEng := newMockEngine()
	srv, err := admin.NewServer(mockEng, admin.Config{Addr: "127.0.0.1:0"})
	require.NoError(t, err)
	require.NoError(t, srv.Start())
	defer func() { _ = srv.Stop(context.Background()) }()

	resp, err := http.Get(srv.URL() + "/api/v1/cluster/nodes")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var topo admin.ClusterTopologyResponse
	err = json.NewDecoder(resp.Body).Decode(&topo)
	require.NoError(t, err)
	assert.Equal(t, "node-1", topo.LeaderID)
	assert.Equal(t, "node-1", topo.SelfNodeID)
	assert.Equal(t, 2, topo.TotalNodes)
	assert.Equal(t, 2, topo.HealthyNodes)
	require.Len(t, topo.Nodes, 2)
	assert.Equal(t, "node-1", topo.Nodes[0].NodeID)
	assert.True(t, topo.Nodes[0].IsLeader)
	assert.Equal(t, "HEALTHY", topo.Nodes[0].Status)
}
