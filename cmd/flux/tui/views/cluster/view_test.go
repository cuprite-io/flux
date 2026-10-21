package cluster_test

import (
	"context"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/cuprite-io/flux/cmd/flux/tui/client"
	clusterview "github.com/cuprite-io/flux/cmd/flux/tui/views/cluster"
	"github.com/cuprite-io/flux/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockClusterClient struct {
	topology *client.ClusterTopology
	err      error
}

func (m *mockClusterClient) GetStatus(ctx context.Context) (*client.StatusResponse, error) {
	return &client.StatusResponse{
		Service:      "flux-admin",
		Version:      "0.26.0",
		NodeID:       "node-1",
		LeaderID:     "node-1",
		IsLeader:     true,
		ClusterNodes: 3,
	}, nil
}

func (m *mockClusterClient) GetCircuits(ctx context.Context) ([]client.CircuitSummary, error) {
	return nil, nil
}
func (m *mockClusterClient) GetCircuit(ctx context.Context, id string) (*client.CircuitSummary, error) {
	return nil, nil
}
func (m *mockClusterClient) PutCircuit(ctx context.Context, circuit *types.Circuit) (*client.CircuitSummary, error) {
	return nil, nil
}
func (m *mockClusterClient) GetAutopilot(ctx context.Context) ([]client.AutopilotSession, error) {
	return nil, nil
}
func (m *mockClusterClient) GetAlertHistory(ctx context.Context, circuitID string, limit int) ([]client.AlertRecord, error) {
	return nil, nil
}
func (m *mockClusterClient) StreamAlerts(ctx context.Context) (<-chan client.AlertEvent, <-chan error, error) {
	return nil, nil, nil
}
func (m *mockClusterClient) SubmitFeedback(ctx context.Context, req client.FeedbackRequest) (*client.FeedbackResponse, error) {
	return nil, nil
}
func (m *mockClusterClient) DryRunEval(ctx context.Context, req client.EvalRequest) (*client.EvalResponse, error) {
	return nil, nil
}
func (m *mockClusterClient) GetCacheKeys(ctx context.Context, prefix string, limit int) ([]client.CacheKeyInfo, error) {
	return nil, nil
}
func (m *mockClusterClient) InspectCacheKey(ctx context.Context, key string) (*client.CacheKeyDetail, error) {
	return nil, nil
}
func (m *mockClusterClient) GetClusterTopology(ctx context.Context) (*client.ClusterTopology, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.topology, nil
}
func (m *mockClusterClient) Close() error {
	return nil
}

func sampleTopology() *client.ClusterTopology {
	now := time.Now().UTC()
	return &client.ClusterTopology{
		LeaderID:     "node-1",
		SelfNodeID:   "node-1",
		TotalNodes:   3,
		HealthyNodes: 3,
		Nodes: []client.ClusterNode{
			{
				NodeID:       "node-1",
				Addr:         "10.0.1.10:9090",
				StreamPort:   8080,
				IsLeader:     true,
				Status:       clusterview.StatusHealthy,
				HeartbeatAge: 800 * time.Millisecond,
				LeaseRemain:  9200 * time.Millisecond,
				UpdatedAt:    now,
				Uptime:       "4h12m",
				Metadata: map[string]string{
					"zone":    "us-east-1a",
					"version": "0.26.0",
				},
			},
			{
				NodeID:       "node-2",
				Addr:         "10.0.1.11:9090",
				StreamPort:   8080,
				IsLeader:     false,
				Status:       clusterview.StatusHealthy,
				HeartbeatAge: 1200 * time.Millisecond,
				LeaseRemain:  8800 * time.Millisecond,
				UpdatedAt:    now,
				Uptime:       "3h50m",
				Metadata: map[string]string{
					"zone": "us-east-1b",
				},
			},
			{
				NodeID:       "node-3",
				Addr:         "10.0.1.12:9090",
				StreamPort:   8080,
				IsLeader:     false,
				Status:       clusterview.StatusWarning,
				HeartbeatAge: 6 * time.Second,
				LeaseRemain:  4 * time.Second,
				UpdatedAt:    now.Add(-6 * time.Second),
				Uptime:       "1h10m",
			},
		},
	}
}

func TestTopologyTracker_ConsensusAndTransitions(t *testing.T) {
	tracker := clusterview.NewTopologyTracker(50)
	assert.Empty(t, tracker.Events())

	// 1. Initial snapshot -> initial election event
	topo1 := sampleTopology()
	nodes := tracker.ProcessTopology(topo1, "node-1")
	require.Len(t, nodes, 3)
	assert.Equal(t, "node-1", nodes[0].NodeID)
	assert.True(t, nodes[0].IsLeader)
	assert.True(t, nodes[0].IsSelf)

	events := tracker.Events()
	require.Len(t, events, 1)
	assert.Equal(t, "ELECTION", events[0].EventType)
	assert.Contains(t, events[0].Message, "node-1 recognized as elected cluster leader")

	// 2. Second snapshot: Node-3 degrades to EXPIRED, node-4 joins
	now := time.Now().UTC()
	topo2 := &client.ClusterTopology{
		LeaderID:     "node-1",
		SelfNodeID:   "node-1",
		TotalNodes:   4,
		HealthyNodes: 2,
		Nodes: []client.ClusterNode{
			topo1.Nodes[0],
			topo1.Nodes[1],
			{
				NodeID:       "node-3",
				Status:       clusterview.StatusExpired,
				HeartbeatAge: 11 * time.Second,
				LeaseRemain:  0,
				UpdatedAt:    now.Add(-11 * time.Second),
			},
			{
				NodeID:       "node-4",
				Status:       clusterview.StatusHealthy,
				HeartbeatAge: 500 * time.Millisecond,
				LeaseRemain:  9500 * time.Millisecond,
				UpdatedAt:    now,
			},
		},
	}

	nodes = tracker.ProcessTopology(topo2, "node-1")
	require.Len(t, nodes, 4)

	events = tracker.Events()
	// Should have: Initial ELECTION, node-3 HEALTH_CHANGE (degraded), node-4 JOIN
	require.Len(t, events, 3)
	assert.Equal(t, "HEALTH_CHANGE", events[1].EventType)
	assert.Contains(t, events[1].Message, "node-3 health transition")
	assert.Equal(t, "JOIN", events[2].EventType)
	assert.Contains(t, events[2].Message, "node-4 joined cluster")

	// 3. Third snapshot: Failover! node-1 expires, node-2 becomes leader
	topo3 := &client.ClusterTopology{
		LeaderID:     "node-2",
		SelfNodeID:   "node-2",
		TotalNodes:   3,
		HealthyNodes: 3,
		Nodes: []client.ClusterNode{
			{
				NodeID:       "node-2",
				IsLeader:     true,
				Status:       clusterview.StatusHealthy,
				HeartbeatAge: 500 * time.Millisecond,
				LeaseRemain:  9500 * time.Millisecond,
				UpdatedAt:    now,
			},
			topo2.Nodes[3], // node-4
		},
	}

	nodes = tracker.ProcessTopology(topo3, "node-2")
	require.Len(t, nodes, 2)
	assert.Equal(t, "node-2", nodes[0].NodeID)
	assert.True(t, nodes[0].IsLeader)

	events = tracker.Events()
	// Failover event should be recorded with critical severity
	foundFailover := false
	for _, ev := range events {
		if ev.EventType == "FAILOVER" {
			foundFailover = true
			assert.Equal(t, "node-1", ev.PrevLeader)
			assert.Equal(t, "node-2", ev.NewLeader)
			assert.Equal(t, clusterview.SeverityCritical, ev.Severity)
		}
	}
	assert.True(t, foundFailover)
}

func TestClusterView_LifecycleAndRendering(t *testing.T) {
	mockCli := &mockClusterClient{topology: sampleTopology()}
	model := clusterview.New(mockCli)
	model.SetDimensions(120, 35)

	// Initial View should render loading or initial state
	initialView := model.View()
	assert.NotEmpty(t, initialView)

	// Execute Init cmd
	cmd := model.Init()
	require.NotNil(t, cmd)
	msg := cmd()
	require.IsType(t, clusterview.TopologyLoadedMsg{}, msg)

	// Update model with TopologyLoadedMsg
	m, _ := model.Update(msg)

	require.Len(t, m.Nodes(), 3)
	leader := m.Leader()
	require.NotNil(t, leader)
	assert.Equal(t, "node-1", leader.NodeID)
	assert.True(t, leader.IsLeader)

	viewOutput := m.View()
	assert.Contains(t, viewOutput, "Topology: 3 Active Nodes")
	assert.Contains(t, viewOutput, "node-1")
	assert.Contains(t, viewOutput, "LEADER ★")
	assert.Contains(t, viewOutput, "HEALTHY")
	assert.Contains(t, viewOutput, "Node Metadata")

	// 2. Test Tab focus toggle (Table -> Detail Panel)
	assert.Equal(t, 0, m.ActivePanel())
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyTab})
	assert.Equal(t, 1, m.ActivePanel())

	// 3. Test 'v' key toggle to Failover Log tab
	assert.Equal(t, 0, m.PanelTab())
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'v'}})
	assert.Equal(t, 1, m.PanelTab())
	assert.Contains(t, m.View(), "Consensus & Failover Audit Log")

	// 4. Test Filtering with '/'
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'/'}})
	assert.True(t, m.IsFiltering())

	for _, r := range "node-2" {
		m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
	}
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	assert.False(t, m.IsFiltering())
	assert.Contains(t, m.View(), "node-2")

	// 5. Test Refresh 'r'
	m, refreshCmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'r'}})
	assert.NotNil(t, refreshCmd)
	assert.Contains(t, m.StatusMsg(), "Refreshing")
}

func TestFormatDurationAndLease(t *testing.T) {
	assert.Equal(t, "450ms", clusterview.FormatDuration(450*time.Millisecond))
	assert.Equal(t, "2.5s", clusterview.FormatDuration(2500*time.Millisecond))
	assert.Equal(t, "3m15s", clusterview.FormatDuration(195*time.Second))

	assert.Equal(t, "EXPIRED", clusterview.FormatLease(0, clusterview.StatusExpired))
	assert.Equal(t, "EXPIRED", clusterview.FormatLease(-time.Second, clusterview.StatusHealthy))
	assert.Equal(t, "8.5s", clusterview.FormatLease(8500*time.Millisecond, clusterview.StatusHealthy))
}
