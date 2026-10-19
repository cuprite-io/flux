package client_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cuprite-io/flux/cmd/flux/tui/client"
	"github.com/cuprite-io/flux/internal/admin"
	"github.com/cuprite-io/flux/internal/history"
	"github.com/cuprite-io/flux/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockEngine struct {
	mu        sync.Mutex
	circuits  map[string]*types.Circuit
	alerts    []history.AlertRecord
	observers map[uint64]func(admin.AlertEvent)
	nextObsID uint64
}

func newMockEngine() *mockEngine {
	return &mockEngine{
		circuits:  make(map[string]*types.Circuit),
		observers: make(map[uint64]func(admin.AlertEvent)),
	}
}

func (m *mockEngine) Version() string       { return "0.23.0" }
func (m *mockEngine) NodeID() string        { return "node-test-1" }
func (m *mockEngine) LeaderID() string      { return "node-test-1" }
func (m *mockEngine) IsLeader() bool        { return true }
func (m *mockEngine) ActiveNodes() []string { return []string{"node-test-1", "node-test-2"} }

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
	return nil, fmt.Errorf("not found")
}

func (m *mockEngine) GetAlertHistory(ctx context.Context, circuitID string, limit int) ([]history.AlertRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.alerts, nil
}

func (m *mockEngine) RecordAlertFeedback(ctx context.Context, circuitID, alertID string, fb history.Feedback) error {
	return nil
}

func (m *mockEngine) GetAutopilotSessions(ctx context.Context) []admin.AutopilotSessionInfo {
	return []admin.AutopilotSessionInfo{
		{
			SessionID: "autopilot-1",
			Prompt:    "Detect latency anomalies",
			Status:    "ACTIVE",
		},
	}
}

func (m *mockEngine) DryRunEval(ctx context.Context, req admin.EvalRequest) (*admin.EvalResponse, error) {
	return &admin.EvalResponse{
		Passed:           true,
		ExecutedCircuits: []string{"test-circuit"},
		ReturnedData:     map[string]any{"qualified": true},
		DurationMicro:    55,
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

func TestHTTPClient_FullLifecycle(t *testing.T) {
	mockEng := newMockEngine()
	c1 := types.NewCircuit("fraud_circuit")
	c1.Tags = []string{"stream:payments"}
	c1.Root = types.NewNode("root").WithCondition("amount > 500")
	mockEng.circuits[c1.ID] = c1

	mockEng.alerts = append(mockEng.alerts, history.AlertRecord{
		ID:        "alert-1",
		CircuitID: "fraud_circuit",
		SinkName:  "slack",
		Condition: "amount > 500",
		Payload:   map[string]any{"amount": 999.0},
	})

	srv, err := admin.NewServer(mockEng, admin.Config{
		Addr:  "127.0.0.1:0",
		Token: "test-secret",
	})
	require.NoError(t, err)
	require.NoError(t, srv.Start())
	defer srv.Stop(context.Background())

	// 1. Invalid URL Scheme rejection
	_, err = client.NewHTTPClient("mysql://127.0.0.1:3306", "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported scheme")

	// 2. Auth error with wrong token
	badClient, err := client.NewHTTPClient(srv.URL(), "wrong-secret")
	require.NoError(t, err)
	_, err = badClient.GetStatus(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "authentication required")

	// 3. Connect with valid token
	c, err := client.NewHTTPClient(srv.URL(), "test-secret")
	require.NoError(t, err)
	defer c.Close()

	// GetStatus
	status, err := c.GetStatus(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "flux-admin", status.Service)
	assert.Equal(t, "0.23.0", status.Version)
	assert.Equal(t, 2, status.ClusterNodes)
	assert.True(t, status.IsLeader)

	// GetCircuits
	circuits, err := c.GetCircuits(context.Background())
	require.NoError(t, err)
	require.Len(t, circuits, 1)
	assert.Equal(t, "fraud_circuit", circuits[0].ID)

	// GetCircuit (single)
	single, err := c.GetCircuit(context.Background(), "fraud_circuit")
	require.NoError(t, err)
	assert.Equal(t, "fraud_circuit", single.ID)
	assert.Equal(t, "amount > 500", single.RootCondition)

	// GetAutopilot
	autopilotSessions, err := c.GetAutopilot(context.Background())
	require.NoError(t, err)
	require.Len(t, autopilotSessions, 1)
	assert.Equal(t, "Detect latency anomalies", autopilotSessions[0].Prompt)

	// GetAlertHistory
	alerts, err := c.GetAlertHistory(context.Background(), "fraud_circuit", 10)
	require.NoError(t, err)
	require.Len(t, alerts, 1)
	assert.Equal(t, "alert-1", alerts[0].ID)

	// SubmitFeedback
	fbResp, err := c.SubmitFeedback(context.Background(), client.FeedbackRequest{
		CircuitID:    "fraud_circuit",
		AlertID:      "alert-1",
		FeedbackType: "false_positive",
		Notes:        "Authorized test transaction",
		Operator:     "secops@company.com",
	})
	require.NoError(t, err)
	assert.Equal(t, "recorded", fbResp.Status)
	assert.Equal(t, "alert-1", fbResp.AlertID)

	// DryRunEval
	evalResp, err := c.DryRunEval(context.Background(), client.EvalRequest{
		Payload: map[string]any{"amount": 1000},
		Tags:    []string{"stream:payments"},
	})
	require.NoError(t, err)
	assert.True(t, evalResp.Passed)
	assert.Equal(t, []string{"test-circuit"}, evalResp.ExecutedCircuits)

	// StreamAlerts
	streamCtx, cancelStream := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelStream()

	eventCh, errCh, err := c.StreamAlerts(streamCtx)
	require.NoError(t, err)

	// Fire alert from engine
	go func() {
		time.Sleep(50 * time.Millisecond)
		mockEng.fireAlert(admin.AlertEvent{
			ID:        "stream-alert-1",
			CircuitID: "fraud_circuit",
			SinkName:  "slack",
			Timestamp: time.Now(),
		})
	}()

	select {
	case evt := <-eventCh:
		assert.Equal(t, "stream-alert-1", evt.ID)
		assert.Equal(t, "fraud_circuit", evt.CircuitID)
	case err := <-errCh:
		t.Fatalf("unexpected stream error: %v", err)
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for SSE alert event")
	}
}

func TestLocalClient_StandaloneMode(t *testing.T) {
	lc, err := client.NewLocalClient()
	require.NoError(t, err)
	defer lc.Close()

	ctx := context.Background()

	// 1. GetStatus in standalone
	status, err := lc.GetStatus(ctx)
	require.NoError(t, err)
	assert.Equal(t, "standalone", status.Status)
	assert.Equal(t, 1, status.ClusterNodes)
	assert.True(t, status.IsLeader)

	// 2. Add circuit to local engine
	c1 := types.NewCircuit("local_eval_circ")
	c1.Tags = []string{"stream:test"}
	c1.Root = types.NewNode("root").WithCondition("payload.value > 10")
	require.NoError(t, lc.Engine().Registry().Put(ctx, c1))

	// 3. GetCircuits
	circuits, err := lc.GetCircuits(ctx)
	require.NoError(t, err)
	require.Len(t, circuits, 1)
	assert.Equal(t, "local_eval_circ", circuits[0].ID)

	// 4. GetCircuit single
	single, err := lc.GetCircuit(ctx, "local_eval_circ")
	require.NoError(t, err)
	assert.Equal(t, "local_eval_circ", single.ID)

	// 5. DryRunEval
	evalRes, err := lc.DryRunEval(ctx, client.EvalRequest{
		CircuitID: "local_eval_circ",
		Payload:   map[string]any{"value": 20},
	})
	require.NoError(t, err)
	assert.True(t, evalRes.Passed)
	assert.Equal(t, []string{"local_eval_circ"}, evalRes.ExecutedCircuits)

	// Failed condition evaluation
	evalFail, err := lc.DryRunEval(ctx, client.EvalRequest{
		CircuitID: "local_eval_circ",
		Payload:   map[string]any{"value": 5},
	})
	require.NoError(t, err)
	assert.False(t, evalFail.Passed)

	// Record an alert in history first so feedback can be attached
	_ = lc.Engine().History().Record(ctx, history.AlertRecord{
		ID:        "alert-local-1",
		CircuitID: "local_eval_circ",
		SinkName:  "slack",
	})
	time.Sleep(50 * time.Millisecond)

	// 6. SubmitFeedback
	fbResp, err := lc.SubmitFeedback(ctx, client.FeedbackRequest{
		CircuitID:    "local_eval_circ",
		AlertID:      "alert-local-1",
		FeedbackType: "valid",
	})
	require.NoError(t, err)
	assert.Equal(t, "recorded", fbResp.Status)
}
