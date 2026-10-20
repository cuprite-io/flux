package cache_test

import (
	"context"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/cuprite-io/flux/cmd/flux/tui/client"
	cacheview "github.com/cuprite-io/flux/cmd/flux/tui/views/cache"
	"github.com/cuprite-io/flux/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockCacheClient struct {
	keys   []client.CacheKeyInfo
	detail map[string]*client.CacheKeyDetail
	err    error
}

func (m *mockCacheClient) GetStatus(ctx context.Context) (*client.StatusResponse, error) {
	return &client.StatusResponse{Service: "flux-admin", Version: "0.25.0"}, nil
}
func (m *mockCacheClient) GetCircuits(ctx context.Context) ([]client.CircuitSummary, error) {
	return nil, nil
}
func (m *mockCacheClient) GetCircuit(ctx context.Context, id string) (*client.CircuitSummary, error) {
	return nil, nil
}
func (m *mockCacheClient) PutCircuit(ctx context.Context, circuit *types.Circuit) (*client.CircuitSummary, error) {
	return nil, nil
}
func (m *mockCacheClient) GetAutopilot(ctx context.Context) ([]client.AutopilotSession, error) {
	return nil, nil
}
func (m *mockCacheClient) GetAlertHistory(ctx context.Context, circuitID string, limit int) ([]client.AlertRecord, error) {
	return nil, nil
}
func (m *mockCacheClient) StreamAlerts(ctx context.Context) (<-chan client.AlertEvent, <-chan error, error) {
	return nil, nil, nil
}
func (m *mockCacheClient) SubmitFeedback(ctx context.Context, req client.FeedbackRequest) (*client.FeedbackResponse, error) {
	return nil, nil
}
func (m *mockCacheClient) DryRunEval(ctx context.Context, req client.EvalRequest) (*client.EvalResponse, error) {
	return nil, nil
}
func (m *mockCacheClient) Close() error {
	return nil
}

func (m *mockCacheClient) GetCacheKeys(ctx context.Context, prefix string, limit int) ([]client.CacheKeyInfo, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.keys, nil
}

func (m *mockCacheClient) InspectCacheKey(ctx context.Context, key string) (*client.CacheKeyDetail, error) {
	if m.err != nil {
		return nil, m.err
	}
	if d, ok := m.detail[key]; ok {
		return d, nil
	}
	return nil, nil
}

func sampleCacheData() ([]client.CacheKeyInfo, map[string]*client.CacheKeyDetail) {
	keys := []client.CacheKeyInfo{
		{Key: "alert:history:fraud", Type: "map", Size: 2, TTL: 14 * 24 * time.Hour},
		{Key: "entity:user:101", Type: "string", Size: 48, TTL: 30 * time.Minute},
		{Key: "schema:payments", Type: "set", Size: 3, TTL: 0},
		{Key: "flux:window:auth_rate", Type: "window", Size: 15, TTL: 1 * time.Minute},
	}

	details := map[string]*client.CacheKeyDetail{
		"alert:history:fraud": {
			Key:   "alert:history:fraud",
			Type:  "map",
			Size:  2,
			TTL:   14 * 24 * time.Hour,
			Value: map[string]string{"alert-1": `{"risk":"high"}`, "alert-2": `{"risk":"low"}`},
		},
		"entity:user:101": {
			Key:   "entity:user:101",
			Type:  "string",
			Size:  48,
			TTL:   30 * time.Minute,
			Value: `{"id":101,"name":"Alice","tier":"gold","active":true}`,
		},
		"schema:payments": {
			Key:   "schema:payments",
			Type:  "set",
			Size:  3,
			TTL:   0,
			Value: []string{"amount", "currency", "card_token"},
		},
		"flux:window:auth_rate": {
			Key:   "flux:window:auth_rate",
			Type:  "window",
			Size:  15,
			TTL:   1 * time.Minute,
			Value: int64(15),
		},
	}

	return keys, details
}

func TestCacheView_LifecycleAndInspection(t *testing.T) {
	keys, details := sampleCacheData()
	mock := &mockCacheClient{keys: keys, detail: details}

	view := cacheview.New(mock)
	view.SetDimensions(100, 30)

	// Loading state before msg
	assert.Contains(t, view.View(), "Scanning CacheBackend keys")

	// Deliver loaded keys
	cmd := view.Init()
	require.NotNil(t, cmd)
	loadedMsg := cmd()
	newView, detailCmd := view.Update(loadedMsg)
	require.Len(t, newView.Keys(), 4)

	// Deliver key detail
	if detailCmd != nil {
		newView, _ = newView.Update(detailCmd())
	}

	out := newView.View()
	assert.Contains(t, out, "alert:history:fraud")
	assert.Contains(t, out, "entity:user:101")
	assert.Contains(t, out, "schema:payments")
	assert.Contains(t, out, "flux:window:auth_rate")

	// Test Tab to switch focus to Inspector
	newView, _ = newView.Update(tea.KeyMsg{Type: tea.KeyTab})
	assert.Contains(t, newView.View(), "Key Data Inspector (Active)")

	// Test 'v' to toggle raw view
	newView, _ = newView.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'v'}})
	assert.Contains(t, newView.View(), "[RAW VIEW]")

	// Toggle back
	newView, _ = newView.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'v'}})
	assert.NotContains(t, newView.View(), "[RAW VIEW]")

	// Test Esc returns focus to table
	newView, _ = newView.Update(tea.KeyMsg{Type: tea.KeyEscape})
	assert.Contains(t, newView.View(), "Keys & Namespaces (Active)")
}

func TestCacheView_FuzzyFilter(t *testing.T) {
	keys, details := sampleCacheData()
	mock := &mockCacheClient{keys: keys, detail: details}

	view := cacheview.New(mock)
	view.SetDimensions(100, 30)
	view, _ = view.Update(view.Init()())

	// Press '/' to start filtering
	view, _ = view.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'/'}})
	assert.True(t, view.IsFiltering())

	// Type "user"
	for _, r := range "user" {
		view, _ = view.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
	}

	out := view.View()
	assert.Contains(t, out, "entity:user:101")
	assert.NotContains(t, out, "schema:payments")

	// Submit filter
	view, _ = view.Update(tea.KeyMsg{Type: tea.KeyEnter})
	assert.False(t, view.IsFiltering())

	// Clear filter with Esc
	view, _ = view.Update(tea.KeyMsg{Type: tea.KeyEscape})
	assert.Contains(t, view.View(), "schema:payments")
}

func TestCacheView_InspectorRendering(t *testing.T) {
	keys, details := sampleCacheData()
	_ = keys

	// 1. Inspect JSON string
	jsonInsp := cacheview.RenderKeyInspector(details["entity:user:101"], false, 80)
	assert.Contains(t, jsonInsp, "entity:user:101")
	assert.Contains(t, jsonInsp, "STRING")
	assert.Contains(t, jsonInsp, "Alice")
	assert.Contains(t, jsonInsp, "gold")

	// 2. Inspect Map
	mapInsp := cacheview.RenderKeyInspector(details["alert:history:fraud"], false, 80)
	assert.Contains(t, mapInsp, "MAP / HASH")
	assert.Contains(t, mapInsp, "alert-1")
	assert.Contains(t, mapInsp, "alert-2")

	// 3. Inspect Set
	setInsp := cacheview.RenderKeyInspector(details["schema:payments"], false, 80)
	assert.Contains(t, setInsp, "SET")
	assert.Contains(t, setInsp, "amount")
	assert.Contains(t, setInsp, "currency")

	// 4. Inspect Window
	winInsp := cacheview.RenderKeyInspector(details["flux:window:auth_rate"], false, 80)
	assert.Contains(t, winInsp, "SLIDING WINDOW")
	assert.Contains(t, winInsp, "15")

	// 5. Inspect Nil
	nilInsp := cacheview.RenderKeyInspector(nil, false, 80)
	assert.Contains(t, nilInsp, "No key currently inspected")
}

func TestCacheView_NamespaceTree(t *testing.T) {
	keys, _ := sampleCacheData()

	tree := cacheview.BuildNamespaceTree(keys)
	require.NotNil(t, tree)
	assert.Equal(t, 4, tree.Count)
	assert.Contains(t, tree.Children, "alert")
	assert.Contains(t, tree.Children, "entity")
	assert.Contains(t, tree.Children, "schema")
	assert.Contains(t, tree.Children, "flux")

	rendered := cacheview.RenderNamespaceTree(tree, 60)
	assert.Contains(t, rendered, "Cache Namespace Hierarchy")
	assert.Contains(t, rendered, "[alert]")
	assert.Contains(t, rendered, "[entity]")
	assert.Contains(t, rendered, "[schema]")
	assert.Contains(t, rendered, "[flux]")
}

func TestCacheView_HighlightJSON(t *testing.T) {
	jsonCode := `{"name": "test", "count": 12, "active": true, "nullVal": null}`
	highlighted := cacheview.HighlightJSON(jsonCode)
	assert.Contains(t, highlighted, "test")
	assert.Contains(t, highlighted, "12")
	assert.Contains(t, highlighted, "true")
	assert.Contains(t, highlighted, "null")
}
