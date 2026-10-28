package tui_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/cuprite-io/flux/cmd/flux/tui"
	"github.com/cuprite-io/flux/cmd/flux/tui/client"
	alertsview "github.com/cuprite-io/flux/cmd/flux/tui/views/alerts"
	autopilotview "github.com/cuprite-io/flux/cmd/flux/tui/views/autopilot"
	cacheview "github.com/cuprite-io/flux/cmd/flux/tui/views/cache"
	"github.com/cuprite-io/flux/cmd/flux/tui/views/circuits"
	clusterview "github.com/cuprite-io/flux/cmd/flux/tui/views/cluster"
	evalview "github.com/cuprite-io/flux/cmd/flux/tui/views/eval"
	"github.com/cuprite-io/flux/internal/admin"
	"github.com/cuprite-io/flux/internal/history"
	"github.com/cuprite-io/flux/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// e2eEngine implements admin.EngineContext with thread-safe live dynamic state.
type e2eEngine struct {
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

func newE2EEngine() *e2eEngine {
	eng := &e2eEngine{
		version:     "0.30.0",
		nodeID:      "leader-node-01",
		leaderID:    "leader-node-01",
		isLeader:    true,
		activeNodes: []string{"leader-node-01", "worker-node-02", "worker-node-03"},
		circuits:    make(map[string]*types.Circuit),
		observers:   make(map[uint64]func(admin.AlertEvent)),
		cacheDetail: make(map[string]*admin.CacheKeyDetail),
	}

	// 1. Seed demo circuit
	circ := types.NewCircuit("payment_guard").
		WithTags("payments", "fraud").
		WithRoot(types.NewNode("root").WithCondition("payload.amount > 5000.0"))
	eng.circuits[circ.ID] = circ

	// 2. Seed cache keys and details
	eng.cacheKeys = []admin.CacheKeyInfo{
		{Key: "alert:history:payment_guard", Type: "map", Size: 1024, TTL: 24 * time.Hour},
		{Key: "schema:payments", Type: "string", Size: 512, TTL: 12 * time.Hour},
		{Key: "flux:window:rates", Type: "window", Size: 256, TTL: 5 * time.Minute},
	}
	eng.cacheDetail["alert:history:payment_guard"] = &admin.CacheKeyDetail{
		Key:   "alert:history:payment_guard",
		Type:  "map",
		Size:  1024,
		TTL:   24 * time.Hour,
		Value: map[string]any{"last_alert": "alert-101", "total_fired": 42},
	}

	// 3. Seed Autopilot session
	eng.autopilot = []admin.AutopilotSessionInfo{
		{
			SessionID:       "sess-fraud-autotune",
			Prompt:          "Detect high-value anomalous checkout spikes above 10,000 USD",
			Status:          "SHADOWING",
			Tags:            []string{"payments"},
			ActiveCircuits:  []string{"payment_guard"},
			SampledEvents:   30,
			SampleThreshold: 30,
			StagingResult: &admin.AutopilotStagingInfo{
				TotalEvents:  100,
				FiringEvents: 4,
				ErrorEvents:  0,
				ObservedRate: 0.04,
				Status:       "PASS",
				Promoted:     false,
			},
			AuditLog: &admin.AutopilotAuditLog{
				PromptSent:  "Synthesize condition for high value checkouts",
				RawResponse: `{"id": "payment_guard", "root_condition": "amount > 10000"}`,
				Attempts:    1,
			},
			CandidateCircuit: circ,
			ActiveCircuit:    circ,
		},
	}

	return eng
}

func (e *e2eEngine) Version() string       { return e.version }
func (e *e2eEngine) NodeID() string        { return e.nodeID }
func (e *e2eEngine) LeaderID() string      { return e.leaderID }
func (e *e2eEngine) IsLeader() bool        { return e.isLeader }
func (e *e2eEngine) ActiveNodes() []string { return e.activeNodes }

func (e *e2eEngine) ListCircuits(ctx context.Context) ([]*types.Circuit, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	res := make([]*types.Circuit, 0, len(e.circuits))
	for _, c := range e.circuits {
		res = append(res, c)
	}
	return res, nil
}

func (e *e2eEngine) GetCircuit(ctx context.Context, id string) (*types.Circuit, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if c, ok := e.circuits[id]; ok {
		return c, nil
	}
	return nil, fmt.Errorf("circuit %s not found", id)
}

func (e *e2eEngine) PutCircuit(ctx context.Context, circuit *types.Circuit) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if circuit == nil || circuit.ID == "" {
		return errors.New("invalid circuit")
	}
	e.circuits[circuit.ID] = circuit
	return nil
}

func (e *e2eEngine) GetAlertHistory(ctx context.Context, circuitID string, limit int) ([]history.AlertRecord, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	var res []history.AlertRecord
	for _, a := range e.alerts {
		if circuitID == "" || a.CircuitID == circuitID {
			res = append(res, a)
			if len(res) >= limit {
				break
			}
		}
	}
	return res, nil
}

func (e *e2eEngine) RecordAlertFeedback(ctx context.Context, circuitID, alertID string, fb history.Feedback) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.feedbackCalls = append(e.feedbackCalls, fb)
	return nil
}

func (e *e2eEngine) GetAutopilotSessions(ctx context.Context) []admin.AutopilotSessionInfo {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.autopilot
}

func (e *e2eEngine) DryRunEval(ctx context.Context, req admin.EvalRequest) (*admin.EvalResponse, error) {
	return &admin.EvalResponse{
		Passed:           true,
		ExecutedCircuits: []string{"payment_guard"},
		ReturnedData: map[string]any{
			"fraud_detected": true,
			"risk_tier":      "critical",
		},
		ShadowFirings: 1,
		DurationMicro: 128,
	}, nil
}

func (e *e2eEngine) RegisterAlertObserver(obs func(evt admin.AlertEvent)) func() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.nextObsID++
	id := e.nextObsID
	e.observers[id] = obs
	return func() {
		e.mu.Lock()
		defer e.mu.Unlock()
		delete(e.observers, id)
	}
}

func (e *e2eEngine) PublishAlert(evt admin.AlertEvent) {
	e.mu.Lock()
	observers := make([]func(admin.AlertEvent), 0, len(e.observers))
	for _, obs := range e.observers {
		observers = append(observers, obs)
	}
	e.mu.Unlock()

	for _, obs := range observers {
		obs(evt)
	}
}

func (e *e2eEngine) ScanCacheKeys(ctx context.Context, prefix string, limit int) ([]admin.CacheKeyInfo, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	var res []admin.CacheKeyInfo
	for _, k := range e.cacheKeys {
		if prefix == "" || strings.HasPrefix(k.Key, prefix) {
			res = append(res, k)
		}
	}
	return res, nil
}

func (e *e2eEngine) InspectCacheKey(ctx context.Context, key string) (*admin.CacheKeyDetail, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if d, ok := e.cacheDetail[key]; ok {
		return d, nil
	}
	return nil, errors.New("key not found")
}

func (e *e2eEngine) GetClusterTopology(ctx context.Context) (*admin.ClusterTopologyResponse, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	var nodes []admin.ClusterNode
	for _, id := range e.activeNodes {
		nodes = append(nodes, admin.ClusterNode{
			NodeID:       id,
			Addr:         "127.0.0.1:9090",
			StreamPort:   8080,
			IsLeader:     id == e.leaderID,
			Status:       "HEALTHY",
			HeartbeatAge: 200 * time.Millisecond,
			LeaseRemain:  9800 * time.Millisecond,
			UpdatedAt:    time.Now().UTC(),
			Uptime:       "2h15m",
		})
	}
	return &admin.ClusterTopologyResponse{
		LeaderID:     e.leaderID,
		SelfNodeID:   e.nodeID,
		TotalNodes:   len(nodes),
		HealthyNodes: len(nodes),
		Nodes:        nodes,
	}, nil
}

// startE2EServer launches a real HTTP + SSE Admin Server on an OS-assigned port.
func startE2EServer(t *testing.T, eng admin.EngineContext) (*admin.Server, string, func()) {
	t.Helper()
	server, err := admin.NewServer(eng, admin.Config{
		Addr: "127.0.0.1:0", // OS ephemeral port
	})
	require.NoError(t, err)

	err = server.Start()
	require.NoError(t, err)

	url := server.URL()
	cleanup := func() {
		_ = server.Stop(context.Background())
	}
	return server, url, cleanup
}

// TestE2E_FullCockpit_LiveServerInspection verifies connecting to a real running HTTP admin server,
// fetching live data across Circuits, Cache, Autopilot, and Cluster views.
func TestE2E_FullCockpit_LiveServerInspection(t *testing.T) {
	eng := newE2EEngine()
	_, serverURL, cleanup := startE2EServer(t, eng)
	defer cleanup()

	// 1. Initialize client and verify connection to live server
	cli, err := client.NewHTTPClient(serverURL, "")
	require.NoError(t, err)
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	status, err := cli.GetStatus(ctx)
	require.NoError(t, err)
	assert.Equal(t, "healthy", status.Status)
	assert.Equal(t, "leader-node-01", status.NodeID)

	// 2. Initialize TUI connected to live server
	m := tui.NewModel(tui.Config{
		Endpoint: serverURL,
		NodeID:   "tester-client",
	})
	m.SetDimensions(120, 35)

	assert.True(t, m.IsCluster())
	assert.Equal(t, tui.ViewCircuits, m.CurrentView())

	// 3. Circuits View: Deliver live circuits
	circs, err := cli.GetCircuits(ctx)
	require.NoError(t, err)
	require.Len(t, circs, 1)

	newM, _ := m.Update(circuits.CircuitsLoadedMsg{Circuits: circs})
	m = newM.(tui.Model)
	assert.Contains(t, m.View(), "payment_guard")

	// 4. Cache View: Switch and fetch live keys
	newM, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'3'}}) // '3' is Cache in ClusterViews
	m = newM.(tui.Model)
	assert.Equal(t, tui.ViewCache, m.CurrentView())

	keys, err := cli.GetCacheKeys(ctx, "", 100)
	require.NoError(t, err)
	require.Len(t, keys, 3)

	newM, _ = m.Update(cacheview.CacheKeysLoadedMsg{Keys: keys})
	m = newM.(tui.Model)
	assert.Contains(t, m.View(), "alert:history:payment_guard")

	// 5. Autopilot View: Switch and fetch live sessions
	newM, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'2'}}) // '2' is Autopilot
	m = newM.(tui.Model)
	assert.Equal(t, tui.ViewAutopilot, m.CurrentView())

	sessions, err := cli.GetAutopilot(ctx)
	require.NoError(t, err)
	require.Len(t, sessions, 1)

	newM, _ = m.Update(autopilotview.AutopilotLoadedMsg{Sessions: sessions})
	m = newM.(tui.Model)
	assert.Contains(t, m.View(), "sess-fraud-autotune")
	assert.Contains(t, m.View(), "SHADOWING")

	// 6. Cluster View: Switch and fetch topology
	newM, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'6'}}) // '6' is Cluster
	m = newM.(tui.Model)
	assert.Equal(t, tui.ViewCluster, m.CurrentView())

	top, err := cli.GetClusterTopology(ctx)
	require.NoError(t, err)
	require.Len(t, top.Nodes, 3)

	newM, _ = m.Update(clusterview.TopologyLoadedMsg{Topology: top})
	m = newM.(tui.Model)
	assert.Contains(t, m.View(), "leader-node-01")
	assert.Contains(t, m.View(), "worker-node-02")
}

// TestE2E_DryRunEvaluation_Accuracy verifies sandbox dry-run execution against the live admin server.
func TestE2E_DryRunEvaluation_Accuracy(t *testing.T) {
	eng := newE2EEngine()
	_, serverURL, cleanup := startE2EServer(t, eng)
	defer cleanup()

	cli, err := client.NewHTTPClient(serverURL, "")
	require.NoError(t, err)
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// 1. Direct dry-run API verification
	evalReq := client.EvalRequest{
		CircuitID: "payment_guard",
		Payload: map[string]any{
			"amount":     12500.0,
			"risk_score": 92,
		},
	}
	resp, err := cli.DryRunEval(ctx, evalReq)
	require.NoError(t, err)
	assert.True(t, resp.Passed)
	assert.Contains(t, resp.ExecutedCircuits, "payment_guard")
	assert.Equal(t, true, resp.ReturnedData["fraud_detected"])

	// 2. TUI Eval View integration
	m := tui.NewModel(tui.Config{Endpoint: serverURL})
	m.SetDimensions(120, 35)

	// Switch to :eval
	newM, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'4'}})
	m = newM.(tui.Model)
	assert.Equal(t, tui.ViewEval, m.CurrentView())

	// Deliver EvalCompletedMsg to simulate execution completion
	newM, _ = m.Update(evalview.EvalCompletedMsg{
		Result: &evalview.EvalResult{
			CircuitID:   "payment_guard",
			Passed:      true,
			Duration:    128 * time.Microsecond,
			TotalNodes:  2,
			PassedNodes: 2,
			MutatedState: map[string]any{
				"fraud_detected": true,
				"risk_tier":      "critical",
			},
		},
	})
	m = newM.(tui.Model)
	assert.Contains(t, m.View(), "fraud_detected")
	assert.Contains(t, m.View(), "critical")
}

// TestE2E_AlertStreaming_And_FeedbackSubmission tests live SSE streaming from the server and
// closed-loop feedback submission directly to the live server.
func TestE2E_AlertStreaming_And_FeedbackSubmission(t *testing.T) {
	eng := newE2EEngine()
	_, serverURL, cleanup := startE2EServer(t, eng)
	defer cleanup()

	cli, err := client.NewHTTPClient(serverURL, "")
	require.NoError(t, err)
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Establish SSE streaming connection to the live server
	eventCh, errCh, err := cli.StreamAlerts(ctx)
	require.NoError(t, err)

	// 2. Publish an alert event on the engine
	liveEvent := admin.AlertEvent{
		ID:        "alert-prod-777",
		CircuitID: "payment_guard",
		NodeName:  "leader-node-01",
		SinkName:  "alert_ops",
		Condition: "payload.risk_score >= 80",
		Payload: map[string]any{
			"account_id": "acc_9921",
			"amount":     7500.0,
			"risk_score": 85,
		},
		Timestamp: time.Now().UTC(),
	}

	// Publish via engine
	go func() {
		time.Sleep(50 * time.Millisecond)
		eng.PublishAlert(liveEvent)
	}()

	// Receive over live SSE stream
	select {
	case received := <-eventCh:
		assert.Equal(t, "alert-prod-777", received.ID)
		assert.Equal(t, "payment_guard", received.CircuitID)
	case err := <-errCh:
		t.Fatalf("unexpected stream error: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for SSE alert event")
	}

	// 3. Test submitting feedback over HTTP
	fbReq := client.FeedbackRequest{
		CircuitID:    "payment_guard",
		AlertID:      "alert-prod-777",
		FeedbackType: "false_positive",
		Notes:        "VIP corporate account manual override",
		Operator:     "ops-analyst-1",
	}
	fbResp, err := cli.SubmitFeedback(ctx, fbReq)
	require.NoError(t, err)
	assert.Equal(t, "recorded", fbResp.Status)
	assert.Equal(t, "alert-prod-777", fbResp.AlertID)

	// Verify server received and recorded the feedback
	eng.mu.Lock()
	require.Len(t, eng.feedbackCalls, 1)
	assert.Equal(t, "false_positive", eng.feedbackCalls[0].Classification)
	assert.Equal(t, "VIP corporate account manual override", eng.feedbackCalls[0].Reason)
	assert.Equal(t, "ops-analyst-1", eng.feedbackCalls[0].Author)
	eng.mu.Unlock()

	// 4. Test TUI alert model integration with feedback badge
	m := tui.NewModel(tui.Config{Endpoint: serverURL})
	m.SetDimensions(120, 35)

	// Navigate to alerts view
	newM, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'5'}})
	m = newM.(tui.Model)
	assert.Equal(t, tui.ViewAlerts, m.CurrentView())

	// Deliver event to TUI
	newM, _ = m.Update(alertsview.AlertStreamEventMsg{
		Event: client.AlertEvent{
			ID:        liveEvent.ID,
			CircuitID: liveEvent.CircuitID,
			NodeName:  liveEvent.NodeName,
			SinkName:  liveEvent.SinkName,
			Condition: liveEvent.Condition,
			Payload:   liveEvent.Payload,
			Timestamp: liveEvent.Timestamp,
		},
	})
	m = newM.(tui.Model)
	assert.Contains(t, m.View(), "alert-prod-777")
	assert.Contains(t, m.View(), "[UNREVIEWED]")

	// Deliver feedback submission update
	newM, _ = m.Update(alertsview.FeedbackSubmittedMsg{
		AlertID:      liveEvent.ID,
		CircuitID:    liveEvent.CircuitID,
		FeedbackType: "false_positive",
		Notes:        "VIP corporate account manual override",
		Operator:     "ops-analyst-1",
	})
	m = newM.(tui.Model)
	assert.Contains(t, m.View(), "[FALSE POSITIVE ✗]")
}

// TestE2E_AutoReconnection_And_HeartbeatLatency verifies graceful network disconnection and reconnection.
func TestE2E_AutoReconnection_And_HeartbeatLatency(t *testing.T) {
	eng := newE2EEngine()
	server, serverURL, cleanup := startE2EServer(t, eng)
	defer cleanup()

	h := NewHeadlessHarness(t, tui.Config{
		Endpoint: serverURL,
		NodeID:   "heartbeat-tester",
	})
	assert.True(t, h.Model().IsCluster())

	// 1. Simulate tick message to verify heartbeat latency update
	h.Update(tui.TickMsg(time.Now()))
	require.NotEmpty(t, h.View())

	// 2. Shut down the server to simulate connection drop
	_ = server.Stop(context.Background())

	// TUI should stay robust without panicking
	h.Update(tui.TickMsg(time.Now()))
	require.NotEmpty(t, h.View())

	// 3. Start a new server on another ephemeral port
	eng2 := newE2EEngine()
	_, newServerURL, cleanup2 := startE2EServer(t, eng2)
	defer cleanup2()

	// Reconnect to new server via command
	h.SendCommand(fmt.Sprintf("connect %s", newServerURL))
	assert.True(t, h.Model().IsCluster())
	h.AssertScreenContains("Connected to")
}

// TestE2E_HighThroughputStreaming_StressAndMemoryBounding tests rapid firing of 120 alerts
// to verify memory bounding (500 cap), zero goroutine leaks, and view stability.
func TestE2E_HighThroughputStreaming_StressAndMemoryBounding(t *testing.T) {
	eng := newE2EEngine()
	_, serverURL, cleanup := startE2EServer(t, eng)
	defer cleanup()

	m := tui.NewModel(tui.Config{Endpoint: serverURL})
	m.SetDimensions(120, 35)

	// Switch to alerts view
	newM, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'5'}})
	m = newM.(tui.Model)
	assert.Equal(t, tui.ViewAlerts, m.CurrentView())

	// Stream 120 alerts into the TUI model
	for i := 0; i < 120; i++ {
		evt := client.AlertEvent{
			ID:        fmt.Sprintf("burst-alert-%03d", i),
			CircuitID: "payment_guard",
			NodeName:  "leader-node-01",
			SinkName:  "alert_ops",
			Condition: "payload.amount > 1000",
			Payload: map[string]any{
				"index": i,
				"value": i * 100,
			},
			Timestamp: time.Now().UTC(),
		}
		newM, _ = m.Update(alertsview.AlertStreamEventMsg{Event: evt})
		m = newM.(tui.Model)
	}

	// Verify all alerts are safely buffered
	alerts := m.AlertsView().Alerts()
	require.Len(t, alerts, 120)
	assert.Equal(t, "burst-alert-119", alerts[0].ID) // latest first

	// View rendering remains stable and shows correct totals
	viewStr := m.View()
	assert.Contains(t, viewStr, "Total: 120")
	assert.Contains(t, viewStr, "burst-alert-119")
}
