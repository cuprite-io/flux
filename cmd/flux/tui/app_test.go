package tui_test

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/cuprite-io/flux/cmd/flux/tui"
	"github.com/cuprite-io/flux/cmd/flux/tui/client"
	tuisync "github.com/cuprite-io/flux/cmd/flux/tui/sync"
	cacheview "github.com/cuprite-io/flux/cmd/flux/tui/views/cache"
	"github.com/cuprite-io/flux/cmd/flux/tui/views/circuits"
	clusterview "github.com/cuprite-io/flux/cmd/flux/tui/views/cluster"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTUI_StandaloneMode_Initialization(t *testing.T) {
	m := tui.NewModel(tui.Config{}) // Empty endpoint = standalone
	m.SetDimensions(120, 40)

	assert.False(t, m.IsCluster())
	assert.Equal(t, tui.ViewCircuits, m.CurrentView())

	viewOutput := m.View()
	require.NotEmpty(t, viewOutput)
	assert.Contains(t, viewOutput, "FLUX")
	assert.Contains(t, viewOutput, "STANDALONE")
	assert.Contains(t, viewOutput, "Circuits")
	assert.Contains(t, viewOutput, "Dry-Run REPL")

	// In Standalone mode, Cache and Cluster tabs are hidden from tab bar
	assert.NotContains(t, viewOutput, "Cache / State")
	assert.NotContains(t, viewOutput, "Cluster Nodes")
}

func TestTUI_ClusterMode_Initialization(t *testing.T) {
	m := tui.NewModel(tui.Config{Endpoint: "10.0.1.45:9090", NodeID: "worker-1"})
	m.SetDimensions(120, 40)

	assert.True(t, m.IsCluster())
	assert.Equal(t, tui.ViewCircuits, m.CurrentView())

	viewOutput := m.View()
	require.NotEmpty(t, viewOutput)
	assert.Contains(t, viewOutput, "FLUX")
	assert.Contains(t, viewOutput, "10.0.1.45:9090")
	assert.Contains(t, viewOutput, "worker-1")
	assert.Contains(t, viewOutput, "Cache / State")
	assert.Contains(t, viewOutput, "Cluster Nodes")
}

func TestTUI_DynamicConnectAndDisconnect(t *testing.T) {
	m := tui.NewModel(tui.Config{}) // starts standalone
	m.SetDimensions(120, 40)
	assert.False(t, m.IsCluster())

	// 1. Try to connect to unreachable endpoint -> should fail and stay standalone
	newM, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{':'}})
	model := newM.(tui.Model)
	for _, r := range "connect 127.0.0.1:54321" {
		newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
		model = newM.(tui.Model)
	}
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyEnter})
	model = newM.(tui.Model)

	assert.False(t, model.IsCluster(), "should remain in standalone mode when endpoint is unreachable")
	assert.Contains(t, model.View(), "STANDALONE")
	assert.Contains(t, model.View(), "Failed to connect")

	// 2. Try unsupported scheme -> should fail immediately with explicit error
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{':'}})
	model = newM.(tui.Model)
	for _, r := range "connect mysql://localhost:3306" {
		newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
		model = newM.(tui.Model)
	}
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyEnter})
	model = newM.(tui.Model)
	assert.False(t, model.IsCluster())
	assert.Contains(t, model.View(), "unsupported scheme")

	// 3. Test non-Flux server (e.g. returns HTML like Google.com) -> should be rejected
	nonFluxServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<html><body>Welcome to Google</body></html>`))
	}))
	defer nonFluxServer.Close()

	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{':'}})
	model = newM.(tui.Model)
	for _, r := range fmt.Sprintf("connect %s", nonFluxServer.URL) {
		newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
		model = newM.(tui.Model)
	}
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyEnter})
	model = newM.(tui.Model)
	assert.False(t, model.IsCluster(), "should reject non-Flux web servers")
	assert.Contains(t, model.View(), "not a Flux Admin control plane")

	// 4. Start a mock Flux Admin HTTP server with token verification and signature
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Flux-Admin", "true")
		if r.Header.Get("Authorization") == "Bearer bad-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"service":"flux-admin","version":"0.25.0","cluster_nodes":3,"leader_id":"test-leader"}`))
	}))
	defer ts.Close()

	// Connect with bad token -> auth required failure
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{':'}})
	model = newM.(tui.Model)
	for _, r := range fmt.Sprintf("connect %s bad-token", ts.URL) {
		newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
		model = newM.(tui.Model)
	}
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyEnter})
	model = newM.(tui.Model)
	assert.False(t, model.IsCluster())
	assert.Contains(t, model.View(), "authentication required")

	// Connect with valid token -> success
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{':'}})
	model = newM.(tui.Model)
	for _, r := range fmt.Sprintf("connect %s valid-token", ts.URL) {
		newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
		model = newM.(tui.Model)
	}
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyEnter})
	model = newM.(tui.Model)

	assert.True(t, model.IsCluster(), "should transition to cluster mode on successful connection")
	assert.Contains(t, model.View(), ts.URL)
	assert.Contains(t, model.View(), "Cache / State")

	// 5. Test :token command
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{':'}})
	model = newM.(tui.Model)
	for _, r := range "token updated-token-secret" {
		newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
		model = newM.(tui.Model)
	}
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyEnter})
	model = newM.(tui.Model)
	assert.Contains(t, model.View(), "token updated")

	// 5. Run :disconnect
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{':'}})
	model = newM.(tui.Model)
	for _, r := range "disconnect" {
		newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
		model = newM.(tui.Model)
	}
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyEnter})
	model = newM.(tui.Model)

	assert.False(t, model.IsCluster())
	assert.Contains(t, model.View(), "STANDALONE")
	assert.NotContains(t, model.View(), "Cache / State")
}

func TestTUI_ViewNavigation_Standalone(t *testing.T) {
	m := tui.NewModel(tui.Config{})
	m.SetDimensions(100, 30)

	// Standalone visible views: 1:Circuits, 2:Eval, 3:Autopilot, 4:Alerts, ?:Help
	// Switch to Eval via '2'
	newM, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'2'}})
	model := newM.(tui.Model)
	assert.Equal(t, tui.ViewEval, model.CurrentView())
	assert.Contains(t, model.View(), "Dry-Run Playground")

	// Switch to Autopilot via '3'
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'3'}})
	model = newM.(tui.Model)
	assert.Equal(t, tui.ViewAutopilot, model.CurrentView())

	// Switch to Alerts via '4'
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'4'}})
	model = newM.(tui.Model)
	assert.Equal(t, tui.ViewAlerts, model.CurrentView())

	// Toggle Help via '?'
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'?'}})
	model = newM.(tui.Model)
	assert.Equal(t, tui.ViewHelp, model.CurrentView())

	// Back to previous via '?'
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'?'}})
	model = newM.(tui.Model)
	assert.Equal(t, tui.ViewAlerts, model.CurrentView())
}

func TestTUI_ViewNavigation_Cluster(t *testing.T) {
	m := tui.NewModel(tui.Config{Endpoint: "localhost:9090"})
	m.SetDimensions(100, 30)

	// Cluster visible views: 1:Circuits, 2:Autopilot, 3:Cache, 4:Eval, 5:Alerts, 6:Cluster
	// Switch to Cache via '3'
	newM, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'3'}})
	model := newM.(tui.Model)
	assert.Equal(t, tui.ViewCache, model.CurrentView())

	// Switch to Cluster via '6'
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'6'}})
	model = newM.(tui.Model)
	assert.Equal(t, tui.ViewCluster, model.CurrentView())
}

func TestTUI_SyncToggle(t *testing.T) {
	// 1. In standalone, sync does nothing and explains cluster requirement
	m := tui.NewModel(tui.Config{})
	m.SetDimensions(100, 30)
	newM, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'s'}})
	model := newM.(tui.Model)
	assert.Contains(t, model.View(), "Cluster Mode")

	// 2. In cluster, sync toggles into syncing and then off
	mCluster := tui.NewModel(tui.Config{Endpoint: "localhost:9090"})
	mCluster.SetDimensions(160, 40)
	assert.Contains(t, mCluster.View(), "Local Sync: OFF")
	assert.False(t, mCluster.IsSyncEnabled())

	// Press 's' to enable
	newM, cmd := mCluster.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'s'}})
	model = newM.(tui.Model)
	assert.True(t, model.IsSyncEnabled())
	assert.Contains(t, model.View(), "Local Sync: [SYNCING...]")
	assert.NotNil(t, cmd)

	// Simulate SyncCompletedMsg
	newM, _ = model.Update(tuisync.SyncCompletedMsg{
		KeyCount:   120,
		TotalBytes: 1468006, // ~1.4MB
		Duration:   25 * time.Millisecond,
	})
	model = newM.(tui.Model)
	assert.Contains(t, model.View(), "[SYNCED: 1.4MB, 120 keys]")

	// Press 's' to disable
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'s'}})
	model = newM.(tui.Model)
	assert.False(t, model.IsSyncEnabled())
	assert.Contains(t, model.View(), "Local Sync: OFF")
}

func TestTUI_SyncEngineCommandsAndLifecycle(t *testing.T) {
	m := tui.NewModel(tui.Config{Endpoint: "127.0.0.1:9090"})
	m.SetDimensions(160, 40)
	assert.False(t, m.IsSyncEnabled())
	require.NotNil(t, m.Syncer())
	require.NotNil(t, m.SnapshotStore())

	// 1. Test :sync command to toggle ON
	newM, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{':'}})
	model := newM.(tui.Model)
	for _, r := range "sync" {
		newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
		model = newM.(tui.Model)
	}
	newM, cmd := model.Update(tea.KeyMsg{Type: tea.KeyEnter})
	model = newM.(tui.Model)
	assert.True(t, model.IsSyncEnabled())
	assert.NotNil(t, cmd)
	assert.Contains(t, model.View(), "Local Sync: [SYNCING...]")

	// 2. Complete sync
	newM, _ = model.Update(tuisync.SyncCompletedMsg{
		KeyCount:   50,
		TotalBytes: 1048576, // 1.0MB
		Duration:   10 * time.Millisecond,
	})
	model = newM.(tui.Model)
	assert.Contains(t, model.View(), "[SYNCED: 1.0MB, 50 keys]")

	// 3. Test :sync off command
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{':'}})
	model = newM.(tui.Model)
	for _, r := range "sync off" {
		newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
		model = newM.(tui.Model)
	}
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyEnter})
	model = newM.(tui.Model)
	assert.False(t, model.IsSyncEnabled())
	assert.Contains(t, model.View(), "Local Sync: OFF")

	// 4. Test :sync on command
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{':'}})
	model = newM.(tui.Model)
	for _, r := range "sync on" {
		newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
		model = newM.(tui.Model)
	}
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyEnter})
	model = newM.(tui.Model)
	assert.True(t, model.IsSyncEnabled())
	assert.Contains(t, model.View(), "Local Sync: [SYNCING...]")

	// 5. Simulate Sync Error
	newM, _ = model.Update(tuisync.SyncCompletedMsg{
		Err: fmt.Errorf("connection refused"),
	})
	model = newM.(tui.Model)
	assert.Contains(t, model.Syncer().HeaderBadge(), "SYNC ERR: connection refused")
	assert.Contains(t, model.View(), "Local sync failed: connection refused")
}

func TestTUI_TickAndUpdate(t *testing.T) {
	m := tui.NewModel(tui.Config{})
	m.SetDimensions(100, 30)

	now := time.Now()
	newM, cmd := m.Update(tui.TickMsg(now))
	require.NotNil(t, newM)
	require.NotNil(t, cmd)
}

func TestTUI_ParseCommand(t *testing.T) {
	tests := []struct {
		input    string
		expected tui.ViewID
		ok       bool
	}{
		{":circuits", tui.ViewCircuits, true},
		{":c", tui.ViewCircuits, true},
		{":1", tui.ViewCircuits, true},
		{":autopilot", tui.ViewAutopilot, true},
		{":auto", tui.ViewAutopilot, true},
		{":2", tui.ViewAutopilot, true},
		{":cache", tui.ViewCache, true},
		{":state", tui.ViewCache, true},
		{":3", tui.ViewCache, true},
		{":eval", tui.ViewEval, true},
		{":dryrun", tui.ViewEval, true},
		{":4", tui.ViewEval, true},
		{":alerts", tui.ViewAlerts, true},
		{":al", tui.ViewAlerts, true},
		{":5", tui.ViewAlerts, true},
		{":cluster", tui.ViewCluster, true},
		{":nodes", tui.ViewCluster, true},
		{":6", tui.ViewCluster, true},
		{":help", tui.ViewHelp, true},
		{":?", tui.ViewHelp, true},
		{":unknown_cmd", tui.ViewCircuits, false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			res, ok := tui.ParseCommand(tt.input)
			assert.Equal(t, tt.ok, ok)
			if ok {
				assert.Equal(t, tt.expected, res)
			}
		})
	}
}

func TestTUI_WindowTitle(t *testing.T) {
	// Standalone mode title
	mStandalone := tui.NewModel(tui.Config{})
	assert.Equal(t, "flux cli", mStandalone.WindowTitle())

	// Cluster mode title
	mCluster := tui.NewModel(tui.Config{Endpoint: "10.0.1.45:9090"})
	assert.Equal(t, "flux cli - 10.0.1.45:9090", mCluster.WindowTitle())

	// Dynamic transition via mock server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Flux-Admin", "true")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"service":"flux-admin","version":"0.25.0"}`))
	}))
	defer ts.Close()

	m := tui.NewModel(tui.Config{})
	assert.Equal(t, "flux cli", m.WindowTitle())

	// Connect
	newM, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{':'}})
	model := newM.(tui.Model)
	for _, r := range fmt.Sprintf("connect %s", ts.URL) {
		newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
		model = newM.(tui.Model)
	}
	newM, cmd := model.Update(tea.KeyMsg{Type: tea.KeyEnter})
	model = newM.(tui.Model)
	assert.NotNil(t, cmd, "should emit WindowTitleCmd")
	assert.Equal(t, fmt.Sprintf("flux cli - %s", ts.URL), model.WindowTitle())

	// Disconnect
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{':'}})
	model = newM.(tui.Model)
	for _, r := range "disconnect" {
		newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
		model = newM.(tui.Model)
	}
	newM, cmd = model.Update(tea.KeyMsg{Type: tea.KeyEnter})
	model = newM.(tui.Model)
	assert.NotNil(t, cmd, "should emit WindowTitleCmd")
	assert.Equal(t, "flux cli", model.WindowTitle())
}

func TestTUI_TitleStackSaveAndRestore(t *testing.T) {
	var buf bytes.Buffer

	// Test nil safety
	tui.SaveWindowTitle(nil)
	tui.RestoreWindowTitle(nil)

	// Test escape sequence generation
	tui.SaveWindowTitle(&buf)
	assert.Equal(t, tui.SaveTitleSeq, buf.String())

	buf.Reset()
	tui.RestoreWindowTitle(&buf)
	assert.Equal(t, tui.RestoreTitleSeq, buf.String())
}

func TestTUI_CircuitsView_Interaction(t *testing.T) {
	// 1. Mock Flux Admin server with circuits endpoints
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Flux-Admin", "true")
		w.Header().Set("Content-Type", "application/json")

		if r.URL.Path == "/api/v1/status" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"service":"flux-admin","version":"0.25.0","cluster_nodes":2,"leader_id":"node-1"}`))
			return
		}

		if r.URL.Path == "/api/v1/circuits" {
			id := r.URL.Query().Get("id")
			if id == "fraud_evaluator" {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{
					"id": "fraud_evaluator",
					"tags": ["payments", "fraud"],
					"root_condition": "payload.amount > 1000",
					"node_count": 2,
					"status": "LIVE",
					"revision": 1,
					"definition": {
						"id": "fraud_evaluator",
						"tags": ["payments", "fraud"],
						"root": {
							"name": "check_amount",
							"condition": "payload.amount > 1000",
							"steps": [{"type": 0, "script": "let x = payload.amount"}],
							"children": [
								{
									"name": "dispatch_alert",
									"steps": [{"type": 1, "sink": "fraud_sink", "sink_type": "kafka"}]
								}
							]
						}
					}
				}`))
				return
			}

			// List
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[
				{
					"id": "fraud_evaluator",
					"tags": ["payments", "fraud"],
					"root_condition": "payload.amount > 1000",
					"node_count": 2,
					"status": "LIVE",
					"revision": 1
				},
				{
					"id": "telemetry_stream",
					"tags": ["metrics"],
					"root_condition": "payload.valid == true",
					"node_count": 1,
					"status": "SHADOW",
					"revision": 3
				}
			]`))
			return
		}

		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	// 2. Initialize TUI model with remote endpoint
	model := tui.NewModel(tui.Config{Endpoint: ts.URL})
	model.SetDimensions(120, 35)

	// Fetch circuits via Init
	cmds := model.Init()
	require.NotNil(t, cmds)

	// Fetch circuits directly through view to populate
	loadedCmd := circuits.FetchCircuitsCmd(model.Client())
	msg := loadedCmd()
	newM, _ := model.Update(msg)
	model = newM.(tui.Model)

	// 3. Verify Table view renders both circuits
	viewOutput := model.View()
	assert.Contains(t, viewOutput, "fraud_evaluator")
	assert.Contains(t, viewOutput, "telemetry_stream")
	assert.Contains(t, viewOutput, "LIVE")
	assert.Contains(t, viewOutput, "SHADOW")

	// 4. Test Filter Mode: press '/'
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'/'}})
	model = newM.(tui.Model)
	assert.True(t, model.CircuitsView().IsFiltering())

	for _, r := range "fraud" {
		newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
		model = newM.(tui.Model)
	}
	assert.Contains(t, model.View(), "fraud_evaluator")
	assert.NotContains(t, model.View(), "telemetry_stream")

	// Press Enter to submit filter
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyEnter})
	model = newM.(tui.Model)
	assert.False(t, model.CircuitsView().IsFiltering())

	// 5. Test Split-Screen Inspector: press Enter
	newM, detailCmd := model.Update(tea.KeyMsg{Type: tea.KeyEnter})
	model = newM.(tui.Model)
	assert.True(t, model.CircuitsView().IsDetail())

	// Deliver full circuit detail
	if detailCmd != nil {
		detailMsg := detailCmd()
		newM, _ = model.Update(detailMsg)
		model = newM.(tui.Model)
	}

	detailOutput := model.View()
	assert.Contains(t, detailOutput, "DAG Hierarchy & Tree")
	assert.Contains(t, detailOutput, "Circuit Specification & Grammar")
	assert.Contains(t, detailOutput, "fraud_evaluator")
	assert.Contains(t, detailOutput, "check_amount")
	assert.Contains(t, detailOutput, "dispatch_alert")

	// 6. Test Tab switches focus between panels
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyTab})
	model = newM.(tui.Model)
	assert.Contains(t, model.View(), "Circuit Specification & Grammar (Active)")

	// 7. Test Esc returns back to table list
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyEscape})
	model = newM.(tui.Model)
	assert.False(t, model.CircuitsView().IsDetail())
	assert.Contains(t, model.View(), "Deployed Decision Pipelines")
}

func TestTUI_CircuitsView_Actions(t *testing.T) {
	// 1. Mock Flux Admin server with circuits endpoints supporting GET, POST
	var storedCircuits = []string{`{
		"id": "fraud_evaluator",
		"tags": ["payments", "fraud"],
		"root_condition": "payload.amount > 1000",
		"node_count": 2,
		"status": "LIVE",
		"revision": 1,
		"definition": {
			"id": "fraud_evaluator",
			"tags": ["payments", "fraud"],
			"root": {
				"name": "check_amount",
				"condition": "payload.amount > 1000",
				"steps": [{"type": 0, "script": "let x = payload.amount"}],
				"children": [
					{
						"name": "dispatch_alert",
						"steps": [{"type": 1, "sink": "fraud_sink", "sink_type": "kafka"}]
					}
				]
			}
		}
	}`}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Flux-Admin", "true")
		w.Header().Set("Content-Type", "application/json")

		if r.URL.Path == "/api/v1/status" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"service":"flux-admin","version":"0.25.0","cluster_nodes":2,"leader_id":"node-1"}`))
			return
		}

		if r.URL.Path == "/api/v1/circuits" {
			if r.Method == http.MethodPost || r.Method == http.MethodPut {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{
					"id": "hotswapped_circuit",
					"tags": ["hotswap"],
					"root_condition": "payload.active == true",
					"node_count": 1,
					"status": "LIVE",
					"revision": 2
				}`))
				return
			}

			// GET by id
			id := r.URL.Query().Get("id")
			if id == "fraud_evaluator" {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(storedCircuits[0]))
				return
			}

			// List
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[` + storedCircuits[0] + `]`))
			return
		}

		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	model := tui.NewModel(tui.Config{Endpoint: ts.URL})
	model.SetDimensions(120, 35)

	// Fetch circuits directly
	loadedCmd := circuits.FetchCircuitsCmd(model.Client())
	msg := loadedCmd()
	newM, _ := model.Update(msg)
	model = newM.(tui.Model)
	assert.Contains(t, model.View(), "fraud_evaluator")

	// 2. Test 'l' (Lint): Opens linter modal
	newM, lintCmd := model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'l'}})
	model = newM.(tui.Model)
	assert.True(t, model.CircuitsView().IsModalActive())
	if lintCmd != nil {
		detailMsg := lintCmd()
		newM, _ = model.Update(detailMsg)
		model = newM.(tui.Model)
	}

	viewWithLint := model.View()
	assert.Contains(t, viewWithLint, "Circuit Linter Static Analysis")
	assert.Contains(t, viewWithLint, "fraud_evaluator")

	// Verify typing 'q' closes modal and does NOT exit the TUI app
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'q'}})
	model = newM.(tui.Model)
	assert.False(t, model.CircuitsView().IsModalActive())
	assert.Equal(t, tui.ViewCircuits, model.CurrentView())

	// 3. Test 'r' (Reload / Hot-Swap): Opens prompt
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'r'}})
	model = newM.(tui.Model)
	assert.True(t, model.CircuitsView().IsModalActive())
	assert.Contains(t, model.View(), "Hot-Swap Circuit Pipeline")

	// Press Esc to dismiss
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyEscape})
	model = newM.(tui.Model)
	assert.False(t, model.CircuitsView().IsModalActive())

	// Perform actual Hot-Swap with temp file
	tmpDir := t.TempDir()
	validJSON := filepath.Join(tmpDir, "hotswap.json")
	_ = os.WriteFile(validJSON, []byte(`{
		"id": "hotswapped_circuit",
		"tags": ["hotswap"],
		"root": {
			"name": "r1",
			"condition": "payload.active == true",
			"steps": []
		}
	}`), 0644)

	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'r'}})
	model = newM.(tui.Model)
	for _, r := range validJSON {
		newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
		model = newM.(tui.Model)
	}
	newM, putCmd := model.Update(tea.KeyMsg{Type: tea.KeyEnter})
	model = newM.(tui.Model)
	assert.False(t, model.CircuitsView().IsModalActive())
	assert.Contains(t, model.View(), "Hot-swapped hotswapped_circuit successfully")
	require.NotNil(t, putCmd)

	// 4. Test 'e' (Export): Dumps definition to file
	// First open detail mode or fetch detail to ensure definition is loaded
	newM, dCmd := model.Update(tea.KeyMsg{Type: tea.KeyEnter})
	model = newM.(tui.Model)
	if dCmd != nil {
		newM, _ = model.Update(dCmd())
		model = newM.(tui.Model)
	}
	assert.True(t, model.CircuitsView().IsDetail())

	// Press 'e' in detail mode
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'e'}})
	model = newM.(tui.Model)
	assert.True(t, model.CircuitsView().IsModalActive())
	assert.Contains(t, model.View(), "Export Circuit Definition")

	exportPath := filepath.Join(tmpDir, "exported.json")
	for range 30 {
		newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyBackspace})
		model = newM.(tui.Model)
	}
	for _, r := range exportPath {
		newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
		model = newM.(tui.Model)
	}
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyEnter})
	model = newM.(tui.Model)
	assert.False(t, model.CircuitsView().IsModalActive())
	assert.Contains(t, model.View(), "Exported fraud_evaluator")

	exportedData, err := os.ReadFile(exportPath)
	require.NoError(t, err)
	assert.Contains(t, string(exportedData), "fraud_evaluator")

	// 5. Test 'd' (Dry-Run): Dispatches DryRunHandoffMsg and transitions to ViewEval
	newM, dryCmd := model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'d'}})
	model = newM.(tui.Model)
	require.NotNil(t, dryCmd)

	handoffMsg := dryCmd()
	require.IsType(t, circuits.DryRunHandoffMsg{}, handoffMsg)

	// Top-level model handles handoff
	newM, _ = model.Update(handoffMsg)
	model = newM.(tui.Model)
	assert.Equal(t, tui.ViewEval, model.CurrentView())
	assert.Contains(t, model.View(), "Transferred circuit \"fraud_evaluator\" to Dry-Run Playground")
}

func TestTUI_CacheView_Interaction(t *testing.T) {
	mockKeysJSON := `[
		{"key":"alert:history:fraud_evaluator","type":"map","size":5,"ttl":-1},
		{"key":"schema:transactions","type":"string","size":120,"ttl":3600},
		{"key":"flux:window:rate_limiter","type":"window","size":10,"ttl":60}
	]`

	mockSchemaDetailJSON := `{
		"key": "schema:transactions",
		"type": "string",
		"size": 120,
		"ttl": 3600,
		"value": "{\"user_id\":\"string\",\"amount\":\"float\",\"timestamp\":\"int\"}"
	}`

	mockAlertDetailJSON := `{
		"key": "alert:history:fraud_evaluator",
		"type": "map",
		"size": 5,
		"ttl": -1,
		"map_value": {
			"total_alerts": "42",
			"last_alert": "high_amount"
		}
	}`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Flux-Admin", "true")
		w.Header().Set("Content-Type", "application/json")

		if r.URL.Path == "/api/v1/status" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"service":"flux-admin","version":"0.25.0","cluster_nodes":2,"leader_id":"node-1"}`))
			return
		}

		if r.URL.Path == "/api/v1/cache/keys" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(mockKeysJSON))
			return
		}

		if r.URL.Path == "/api/v1/cache/item" {
			key := r.URL.Query().Get("key")
			if key == "schema:transactions" {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(mockSchemaDetailJSON))
				return
			}
			if key == "alert:history:fraud_evaluator" {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(mockAlertDetailJSON))
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"key":"` + key + `","type":"string","size":10,"ttl":-1,"value":"test"}`))
			return
		}

		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	model := tui.NewModel(tui.Config{Endpoint: ts.URL})
	model.SetDimensions(120, 35)

	// Switch to Cache view via :cache command
	newM, _ := model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{':'}})
	model = newM.(tui.Model)
	for _, r := range "cache" {
		newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
		model = newM.(tui.Model)
	}
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyEnter})
	model = newM.(tui.Model)
	assert.Equal(t, tui.ViewCache, model.CurrentView())

	// Fetch cache keys
	fetchCmd := cacheview.FetchCacheKeysCmd(model.Client(), "")
	keysMsg := fetchCmd()
	newM, autoCmd := model.Update(keysMsg)
	model = newM.(tui.Model)

	// Auto-fetches detail for first item
	if autoCmd != nil {
		detailMsg := autoCmd()
		newM, _ = model.Update(detailMsg)
		model = newM.(tui.Model)
	}

	viewOutput := model.View()
	assert.Contains(t, viewOutput, "Keys & Namespaces")
	assert.Contains(t, viewOutput, "Key Data Inspector")
	assert.Contains(t, viewOutput, "alert:history:fraud_evaluator")
	assert.Contains(t, viewOutput, "schema:transactions")
	assert.Contains(t, viewOutput, "flux:window:rate_limiter")

	// 1. Test Tab: Toggle panel focus
	assert.Equal(t, 0, model.CacheView().FocusedPanel())
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyTab})
	model = newM.(tui.Model)
	assert.Equal(t, 1, model.CacheView().FocusedPanel())
	assert.Contains(t, model.View(), "Key Data Inspector (Active)")

	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyTab})
	model = newM.(tui.Model)
	assert.Equal(t, 0, model.CacheView().FocusedPanel())
	assert.Contains(t, model.View(), "Keys & Namespaces (Active)")

	// 2. Test Down navigation & Enter inspection
	newM, downCmd := model.Update(tea.KeyMsg{Type: tea.KeyDown})
	model = newM.(tui.Model)
	if downCmd != nil {
		newM, _ = model.Update(downCmd())
		model = newM.(tui.Model)
	}

	newM, enterCmd := model.Update(tea.KeyMsg{Type: tea.KeyEnter})
	model = newM.(tui.Model)
	if enterCmd != nil {
		newM, _ = model.Update(enterCmd())
		model = newM.(tui.Model)
	}
	assert.Contains(t, model.View(), "schema:transactions")

	// 3. Test 'v' (Raw String View Toggle)
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'v'}})
	model = newM.(tui.Model)
	assert.Contains(t, model.View(), "RAW")

	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'v'}})
	model = newM.(tui.Model)
	assert.Contains(t, model.View(), "FORMATTED")

	// 4. Test '/' (Fuzzy Filter)
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'/'}})
	model = newM.(tui.Model)
	assert.True(t, model.CacheView().IsFiltering())

	for _, r := range "schema" {
		newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
		model = newM.(tui.Model)
	}
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyEnter})
	model = newM.(tui.Model)
	assert.False(t, model.CacheView().IsFiltering())
	assert.Contains(t, model.View(), "schema:transactions")
	assert.NotContains(t, model.View(), "alert:history:fraud_evaluator")

	// Press Esc to clear filter
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyEscape})
	model = newM.(tui.Model)
	assert.Contains(t, model.View(), "alert:history:fraud_evaluator")

	// 5. Test 'r' (Refresh)
	newM, refCmd := model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'r'}})
	model = newM.(tui.Model)
	assert.Contains(t, model.View(), "Refreshing cache keys")
	require.NotNil(t, refCmd)
}

func TestTUI_ClusterView_NavigationAndInteraction(t *testing.T) {
	m := tui.NewModel(tui.Config{Endpoint: "127.0.0.1:9090"})
	m.SetDimensions(140, 40)
	require.True(t, m.IsCluster())

	// 1. Navigate to cluster view via command mode ":cluster"
	newM, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{':'}})
	model := newM.(tui.Model)
	for _, r := range "cluster" {
		newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
		model = newM.(tui.Model)
	}
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyEnter})
	model = newM.(tui.Model)

	assert.Equal(t, tui.ViewCluster, model.CurrentView())
	assert.Contains(t, model.View(), "Cluster Nodes")

	// 2. Feed cluster topology with 3 nodes
	topoMsg := clusterview.TopologyLoadedMsg{
		Topology: &client.ClusterTopology{
			LeaderID: "node-alpha",
			Nodes: []client.ClusterNode{
				{
					NodeID:     "node-alpha",
					Status:     "HEALTHY",
					Addr:       "127.0.0.1:9090",
					StreamPort: 9091,
					UpdatedAt:  time.Now(),
					Uptime:     "2h15m",
					IsLeader:   true,
				},
				{
					NodeID:     "node-beta",
					Status:     "WARNING",
					Addr:       "127.0.0.1:9092",
					StreamPort: 9093,
					UpdatedAt:  time.Now().Add(-7 * time.Second),
					Uptime:     "1h10m",
					IsLeader:   false,
				},
				{
					NodeID:     "node-gamma",
					Status:     "EXPIRED",
					Addr:       "127.0.0.1:9094",
					StreamPort: 9095,
					UpdatedAt:  time.Now().Add(-15 * time.Second),
					Uptime:     "45m",
					IsLeader:   false,
				},
			},
		},
	}
	newM, _ = model.Update(topoMsg)
	model = newM.(tui.Model)

	// Check table rows & leader display
	viewStr := model.View()
	assert.Contains(t, viewStr, "node-alpha")
	assert.Contains(t, viewStr, "LEADER ★")
	assert.Contains(t, viewStr, "node-beta")
	assert.Contains(t, viewStr, "node-gamma")
	assert.Contains(t, viewStr, "HEALTHY")
	assert.Contains(t, viewStr, "WARNING")
	assert.Contains(t, viewStr, "EXPIRED")

	// Verify details panel shows selected node metadata
	assert.Contains(t, viewStr, "Node: node-alpha")
	assert.Contains(t, viewStr, "127.0.0.1:9090")
	assert.Contains(t, viewStr, "9091")
	assert.Contains(t, viewStr, "Lease Gauge:")

	// 3. Test Tab key (panel focus toggle)
	assert.Equal(t, 0, model.ClusterView().ActivePanel())
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyTab})
	model = newM.(tui.Model)
	assert.Equal(t, 1, model.ClusterView().ActivePanel())

	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyTab})
	model = newM.(tui.Model)
	assert.Equal(t, 0, model.ClusterView().ActivePanel())

	// 4. Test 'v' key (toggle between Node Metadata and Failover Audit Log)
	assert.Equal(t, 0, model.ClusterView().PanelTab())
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'v'}})
	model = newM.(tui.Model)
	assert.Equal(t, 1, model.ClusterView().PanelTab())
	assert.Contains(t, model.View(), "Consensus & Failover Audit Log")

	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'v'}})
	model = newM.(tui.Model)
	assert.Equal(t, 0, model.ClusterView().PanelTab())
	assert.Contains(t, model.View(), "Node: node-alpha")

	// 5. Test '/' key (Filtering)
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'/'}})
	model = newM.(tui.Model)
	assert.True(t, model.ClusterView().IsFiltering())

	for _, r := range "beta" {
		newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
		model = newM.(tui.Model)
	}
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyEnter})
	model = newM.(tui.Model)
	assert.False(t, model.ClusterView().IsFiltering())

	filterView := model.View()
	assert.Contains(t, filterView, "node-beta")
	assert.NotContains(t, filterView, "node-gamma")
	assert.NotNil(t, model.ClusterView().SelectedNode())
	assert.Equal(t, "node-beta", model.ClusterView().SelectedNode().NodeID)

	// Clear filter with Esc
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyEscape})
	model = newM.(tui.Model)
	clearView := model.View()
	assert.Contains(t, clearView, "node-alpha")
	assert.Contains(t, clearView, "node-beta")
	assert.Contains(t, clearView, "node-gamma")

	// 6. Test 'r' key (Refresh)
	newM, refCmd := model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'r'}})
	model = newM.(tui.Model)
	assert.Contains(t, model.View(), "Refreshing cluster topology")
	require.NotNil(t, refCmd)

	// 7. Test navigate via :nodes command
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{':'}})
	model = newM.(tui.Model)
	for _, r := range "nodes" {
		newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
		model = newM.(tui.Model)
	}
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyEnter})
	model = newM.(tui.Model)
	assert.Equal(t, tui.ViewCluster, model.CurrentView())
}



