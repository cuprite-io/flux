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
	"github.com/cuprite-io/flux/cmd/flux/tui/views/circuits"
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
		_, _ = w.Write([]byte(`{"service":"flux-admin","version":"0.24.0","cluster_nodes":3,"leader_id":"test-leader"}`))
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

	// 2. In cluster, sync toggles on and off
	mCluster := tui.NewModel(tui.Config{Endpoint: "localhost:9090"})
	mCluster.SetDimensions(100, 30)
	assert.Contains(t, mCluster.View(), "Local Sync: OFF")

	newM, _ = mCluster.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'s'}})
	model = newM.(tui.Model)
	assert.Contains(t, model.View(), "Local Sync: ON")

	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'s'}})
	model = newM.(tui.Model)
	assert.Contains(t, model.View(), "Local Sync: OFF")
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
		_, _ = w.Write([]byte(`{"service":"flux-admin","version":"0.24.0"}`))
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
			_, _ = w.Write([]byte(`{"service":"flux-admin","version":"0.24.0","cluster_nodes":2,"leader_id":"node-1"}`))
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
			_, _ = w.Write([]byte(`{"service":"flux-admin","version":"0.24.0","cluster_nodes":2,"leader_id":"node-1"}`))
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



