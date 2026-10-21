package sync_test

import (
	"context"
	"errors"
	"testing"

	"github.com/cuprite-io/flux/cmd/flux/tui/client"
	"github.com/cuprite-io/flux/cmd/flux/tui/sync"
	"github.com/cuprite-io/flux/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockSyncClient struct {
	statusErr   error
	circuitsErr error
	circuits    []client.CircuitSummary
	keys        []client.CacheKeyInfo
	details     map[string]*client.CacheKeyDetail
	alerts      []client.AlertRecord
}

func (m *mockSyncClient) GetStatus(ctx context.Context) (*client.StatusResponse, error) {
	if m.statusErr != nil {
		return nil, m.statusErr
	}
	return &client.StatusResponse{
		Service:      "flux-admin",
		Version:      "0.25.0",
		ClusterNodes: 3,
		LeaderID:     "node-1",
	}, nil
}

func (m *mockSyncClient) GetCircuits(ctx context.Context) ([]client.CircuitSummary, error) {
	if m.circuitsErr != nil {
		return nil, m.circuitsErr
	}
	return m.circuits, nil
}

func (m *mockSyncClient) GetCircuit(ctx context.Context, id string) (*client.CircuitSummary, error) {
	for _, c := range m.circuits {
		if c.ID == id {
			return &c, nil
		}
	}
	return nil, nil
}

func (m *mockSyncClient) PutCircuit(ctx context.Context, circuit *types.Circuit) (*client.CircuitSummary, error) {
	return nil, nil
}

func (m *mockSyncClient) GetAutopilot(ctx context.Context) ([]client.AutopilotSession, error) {
	return []client.AutopilotSession{
		{SessionID: "sess-1", Status: "LIVE"},
	}, nil
}

func (m *mockSyncClient) GetAlertHistory(ctx context.Context, circuitID string, limit int) ([]client.AlertRecord, error) {
	return m.alerts, nil
}

func (m *mockSyncClient) StreamAlerts(ctx context.Context) (<-chan client.AlertEvent, <-chan error, error) {
	return nil, nil, nil
}

func (m *mockSyncClient) SubmitFeedback(ctx context.Context, req client.FeedbackRequest) (*client.FeedbackResponse, error) {
	return nil, nil
}

func (m *mockSyncClient) DryRunEval(ctx context.Context, req client.EvalRequest) (*client.EvalResponse, error) {
	return nil, nil
}

func (m *mockSyncClient) GetCacheKeys(ctx context.Context, prefix string, limit int) ([]client.CacheKeyInfo, error) {
	return m.keys, nil
}

func (m *mockSyncClient) InspectCacheKey(ctx context.Context, key string) (*client.CacheKeyDetail, error) {
	if d, ok := m.details[key]; ok {
		return d, nil
	}
	return nil, errors.New("key not found")
}

func (m *mockSyncClient) GetClusterTopology(ctx context.Context) (*client.ClusterTopology, error) {
	return &client.ClusterTopology{
		LeaderID:     "node-1",
		SelfNodeID:   "node-1",
		TotalNodes:   3,
		HealthyNodes: 3,
	}, nil
}

func (m *mockSyncClient) Close() error {
	return nil
}

func TestFormatBytes(t *testing.T) {
	assert.Equal(t, "500 B", sync.FormatBytes(500))
	assert.Equal(t, "1.0KB", sync.FormatBytes(1024))
	assert.Equal(t, "1.5KB", sync.FormatBytes(1536))
	assert.Equal(t, "2.0MB", sync.FormatBytes(2*1024*1024))
	assert.Equal(t, "1.2GB", sync.FormatBytes(1288490188))
}

func TestSnapshotStore_LifecycleAndQuery(t *testing.T) {
	store := sync.NewStore()
	assert.False(t, store.IsSynced())

	keys, bytes, _ := store.Stats()
	assert.Equal(t, 0, keys)
	assert.Equal(t, int64(0), bytes)

	mockCircuits := []client.CircuitSummary{
		{ID: "fraud_detector", Tags: []string{"payments"}, RootCondition: "payload.amount > 1000", NodeCount: 3},
		{ID: "rate_limiter", Tags: []string{"api"}, RootCondition: "payload.rate > 100", NodeCount: 1},
	}
	mockKeys := []client.CacheKeyInfo{
		{Key: "schema:payments", Type: "string", Size: 45},
		{Key: "alert:history:fraud_detector", Type: "map", Size: 10},
	}
	mockDetails := map[string]*client.CacheKeyDetail{
		"schema:payments": {Key: "schema:payments", Type: "string", Value: `{"amount":"float"}`},
		"alert:history:fraud_detector": {
			Key:   "alert:history:fraud_detector",
			Type:  "map",
			Value: map[string]string{"1": "alert_fired"},
		},
	}
	mockAlerts := map[string][]client.AlertRecord{
		"fraud_detector": {
			{ID: "alt-1", Condition: "amount > 1000", SinkName: "slack_sink"},
		},
	}
	mockAutopilot := []client.AutopilotSession{
		{SessionID: "auto-1", Status: "LIVE"},
	}

	store.SetData(
		&client.StatusResponse{Version: "0.25.0", ClusterNodes: 2},
		mockCircuits,
		mockKeys,
		mockDetails,
		mockAlerts,
		mockAutopilot,
		2048,
	)

	assert.True(t, store.IsSynced())
	kCount, totalB, lastSync := store.Stats()
	assert.Equal(t, 4, kCount) // 2 circuits + 2 keys
	assert.Equal(t, int64(2048), totalB)
	assert.False(t, lastSync.IsZero())

	// Query circuits
	circuits := store.Circuits()
	assert.Len(t, circuits, 2)
	c, ok := store.Circuit("fraud_detector")
	require.True(t, ok)
	assert.Equal(t, "payload.amount > 1000", c.RootCondition)

	// Query keys
	allKeys := store.Keys("")
	assert.Len(t, allKeys, 2)
	schemaKeys := store.Keys("schema:")
	assert.Len(t, schemaKeys, 1)
	assert.Equal(t, "schema:payments", schemaKeys[0].Key)

	// Query detail
	d, ok := store.Detail("schema:payments")
	require.True(t, ok)
	assert.Equal(t, `{"amount":"float"}`, d.Value)

	// Query alerts
	alerts := store.Alerts("fraud_detector")
	require.Len(t, alerts, 1)
	assert.Equal(t, "alt-1", alerts[0].ID)

	// Clear
	store.Clear()
	assert.False(t, store.IsSynced())
	assert.Empty(t, store.Circuits())
	assert.Empty(t, store.Keys(""))
}

func TestSnapshotStore_Search(t *testing.T) {
	store := sync.NewStore()

	store.SetData(
		&client.StatusResponse{Version: "0.25.0"},
		[]client.CircuitSummary{
			{ID: "payment_fraud", Tags: []string{"checkout", "fraud"}, RootCondition: "payload.amount > 500"},
			{ID: "auth_checker", Tags: []string{"security"}, RootCondition: "payload.mfa == false"},
		},
		[]client.CacheKeyInfo{
			{Key: "schema:checkout", Type: "string", Size: 30},
			{Key: "profile:exemplars:checkout", Type: "map", Size: 2},
		},
		map[string]*client.CacheKeyDetail{
			"schema:checkout": {
				Key:   "schema:checkout",
				Type:  "string",
				Value: `{"user_id":"string","currency":"USD"}`,
			},
			"profile:exemplars:checkout": {
				Key:   "profile:exemplars:checkout",
				Type:  "map",
				Value: map[string]string{"fp1": `{"user_id":"usr_99"}`},
			},
		},
		map[string][]client.AlertRecord{
			"payment_fraud": {
				{ID: "alert_01", Condition: "amount > 500", SinkName: "sec_ops_pager"},
			},
		},
		nil,
		4096,
	)

	// 1. Search by Circuit ID
	res := store.Search("payment")
	require.NotEmpty(t, res)
	assert.Equal(t, "circuit", res[0].Type)
	assert.Equal(t, "payment_fraud", res[0].ID)

	// 2. Search by Tag
	res = store.Search("checkout")
	require.NotEmpty(t, res)
	foundCircuit := false
	foundKey := false
	for _, r := range res {
		if r.Type == "circuit" && r.ID == "payment_fraud" {
			foundCircuit = true
		}
		if r.Type == "key" && r.ID == "schema:checkout" {
			foundKey = true
		}
	}
	assert.True(t, foundCircuit)
	assert.True(t, foundKey)

	// 3. Search inside Inspected JSON value
	res = store.Search("USD")
	require.NotEmpty(t, res)
	assert.Equal(t, "key", res[0].Type)
	assert.Equal(t, "schema:checkout", res[0].ID)

	// 4. Search in Alerts SinkName
	res = store.Search("sec_ops")
	require.NotEmpty(t, res)
	assert.Equal(t, "alert", res[0].Type)
	assert.Equal(t, "alert_01", res[0].ID)

	// 5. Search with Regex
	res = store.Search("amount\\s*>\\s*\\d+")
	require.NotEmpty(t, res)

	// 6. Empty search
	assert.Empty(t, store.Search(""))
	assert.Empty(t, store.Search("   "))

	// 7. Non-matching search
	assert.Empty(t, store.Search("nonexistent_pattern_12345"))
}

func TestSyncer_FullLifecycle(t *testing.T) {
	store := sync.NewStore()
	cfg := sync.DefaultConfig()
	syncer := sync.NewSyncer(store, cfg)

	assert.False(t, syncer.IsEnabled())
	assert.Equal(t, sync.StatusDisabled, syncer.Status())
	assert.Equal(t, "Local Sync: OFF", syncer.HeaderBadge())

	mockCli := &mockSyncClient{
		circuits: []client.CircuitSummary{
			{ID: "c1", NodeCount: 1},
		},
		keys: []client.CacheKeyInfo{
			{Key: "k1", Type: "string", Size: 10},
		},
		details: map[string]*client.CacheKeyDetail{
			"k1": {Key: "k1", Type: "string", Value: "hello"},
		},
		alerts: []client.AlertRecord{
			{ID: "a1", Condition: "true"},
		},
	}

	// 1. Enable sync -> returns FetchSnapshotCmd
	cmd := syncer.SetEnabled(true, mockCli)
	assert.True(t, syncer.IsEnabled())
	assert.Equal(t, sync.StatusSyncing, syncer.Status())
	assert.Equal(t, "Local Sync: [SYNCING...]", syncer.HeaderBadge())
	require.NotNil(t, cmd)

	// Execute FetchSnapshotCmd
	msg := cmd()
	compMsg, ok := msg.(sync.SyncCompletedMsg)
	require.True(t, ok)
	require.NoError(t, compMsg.Err)
	assert.Equal(t, 2, compMsg.KeyCount) // 1 circuit + 1 key
	assert.True(t, compMsg.TotalBytes > 0)

	// Handle completion
	syncer.HandleCompleted(compMsg)
	assert.Equal(t, sync.StatusSynced, syncer.Status())
	badge := syncer.HeaderBadge()
	assert.Contains(t, badge, "[SYNCED:")
	assert.Contains(t, badge, "2 keys]")

	// Verify store is populated
	assert.True(t, store.IsSynced())
	assert.Len(t, store.Circuits(), 1)
	assert.Len(t, store.Keys(""), 1)

	// 2. Test Error Completion
	syncer.HandleCompleted(sync.SyncCompletedMsg{
		Err: errors.New("network timeout"),
	})
	assert.Equal(t, sync.StatusError, syncer.Status())
	assert.Contains(t, syncer.HeaderBadge(), "SYNC ERR: network timeout")

	// 3. Disable sync -> clears store and resets status
	clearCmd := syncer.SetEnabled(false, mockCli)
	assert.False(t, syncer.IsEnabled())
	assert.Equal(t, sync.StatusDisabled, syncer.Status())
	assert.Equal(t, "Local Sync: OFF", syncer.HeaderBadge())
	assert.False(t, store.IsSynced())

	clearMsg := clearCmd()
	assert.IsType(t, sync.SyncClearedMsg{}, clearMsg)
}

func TestFetchSnapshotCmd_Errors(t *testing.T) {
	store := sync.NewStore()
	cfg := sync.DefaultConfig()

	// 1. Nil client
	cmd := sync.FetchSnapshotCmd(nil, store, cfg)
	msg := cmd()
	compMsg := msg.(sync.SyncCompletedMsg)
	assert.Error(t, compMsg.Err)
	assert.Contains(t, compMsg.Err.Error(), "no client available")

	// 2. Status error
	errCli := &mockSyncClient{statusErr: errors.New("connection refused")}
	cmd = sync.FetchSnapshotCmd(errCli, store, cfg)
	msg = cmd()
	compMsg = msg.(sync.SyncCompletedMsg)
	assert.Error(t, compMsg.Err)
	assert.Contains(t, compMsg.Err.Error(), "failed fetching status")

	// 3. Circuits error
	errCli2 := &mockSyncClient{circuitsErr: errors.New("internal server error")}
	cmd = sync.FetchSnapshotCmd(errCli2, store, cfg)
	msg = cmd()
	compMsg = msg.(sync.SyncCompletedMsg)
	assert.Error(t, compMsg.Err)
	assert.Contains(t, compMsg.Err.Error(), "failed fetching circuits")
}

func TestSnapshotClient(t *testing.T) {
	store := sync.NewStore()
	mockCli := &mockSyncClient{
		circuits: []client.CircuitSummary{
			{ID: "c1", NodeCount: 1},
		},
		keys: []client.CacheKeyInfo{
			{Key: "k1", Type: "string"},
		},
		details: map[string]*client.CacheKeyDetail{
			"k1": {Key: "k1", Type: "string", Value: "val1"},
		},
		alerts: []client.AlertRecord{
			{ID: "a1"},
		},
	}

	snapCli := store.Client(mockCli)

	// 1. Unsynced: falls back to mockCli
	circuits, err := snapCli.GetCircuits(context.Background())
	require.NoError(t, err)
	assert.Len(t, circuits, 1)

	// 2. Synced: reads directly from memory
	store.SetData(
		&client.StatusResponse{Version: "0.26.0"},
		[]client.CircuitSummary{
			{ID: "local_c1", NodeCount: 5},
		},
		[]client.CacheKeyInfo{
			{Key: "local_k1", Type: "hash"},
		},
		map[string]*client.CacheKeyDetail{
			"local_k1": {Key: "local_k1", Type: "hash", Value: "local_val"},
		},
		map[string][]client.AlertRecord{
			"local_c1": {{ID: "local_a1"}},
		},
		[]client.AutopilotSession{{SessionID: "s1"}},
		2048,
	)

	// Status from store
	status, err := snapCli.GetStatus(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "0.26.0", status.Version)

	// Circuits from store
	circuits, err = snapCli.GetCircuits(context.Background())
	require.NoError(t, err)
	require.Len(t, circuits, 1)
	assert.Equal(t, "local_c1", circuits[0].ID)

	// Specific circuit from store
	circ, err := snapCli.GetCircuit(context.Background(), "local_c1")
	require.NoError(t, err)
	require.NotNil(t, circ)
	assert.Equal(t, 5, circ.NodeCount)

	// Keys from store
	keys, err := snapCli.GetCacheKeys(context.Background(), "", 10)
	require.NoError(t, err)
	require.Len(t, keys, 1)
	assert.Equal(t, "local_k1", keys[0].Key)

	// Detail from store
	detail, err := snapCli.InspectCacheKey(context.Background(), "local_k1")
	require.NoError(t, err)
	assert.Equal(t, "local_val", detail.Value)

	// Alerts from store
	alerts, err := snapCli.GetAlertHistory(context.Background(), "local_c1", 10)
	require.NoError(t, err)
	require.Len(t, alerts, 1)
	assert.Equal(t, "local_a1", alerts[0].ID)

	// Autopilot from store
	sessions, err := snapCli.GetAutopilot(context.Background())
	require.NoError(t, err)
	require.Len(t, sessions, 1)
	assert.Equal(t, "s1", sessions[0].SessionID)

	// Close
	assert.NoError(t, snapCli.Close())
}
