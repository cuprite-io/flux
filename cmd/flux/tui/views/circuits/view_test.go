package circuits_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/cuprite-io/flux"
	"github.com/cuprite-io/flux/cmd/flux/tui/client"
	"github.com/cuprite-io/flux/cmd/flux/tui/views/circuits"
	"github.com/cuprite-io/flux/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockClient struct {
	circuits []client.CircuitSummary
	getErr   error
}

func (m *mockClient) GetStatus(ctx context.Context) (*client.StatusResponse, error) {
	return &client.StatusResponse{Service: "flux-admin", Version: "0.25.0"}, nil
}

func (m *mockClient) GetCircuits(ctx context.Context) ([]client.CircuitSummary, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	return m.circuits, nil
}

func (m *mockClient) GetCircuit(ctx context.Context, id string) (*client.CircuitSummary, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	for _, c := range m.circuits {
		if c.ID == id {
			return &c, nil
		}
	}
	return nil, nil
}

func (m *mockClient) PutCircuit(ctx context.Context, circuit *types.Circuit) (*client.CircuitSummary, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	summ := client.CircuitSummary{
		ID:         circuit.ID,
		Tags:       circuit.Tags,
		Definition: circuit,
		Status:     "LIVE",
		Revision:   circuit.Version,
	}
	for i, c := range m.circuits {
		if c.ID == circuit.ID {
			m.circuits[i] = summ
			return &summ, nil
		}
	}
	m.circuits = append(m.circuits, summ)
	return &summ, nil
}

func (m *mockClient) GetAutopilot(ctx context.Context) ([]client.AutopilotSession, error) {
	return nil, nil
}

func (m *mockClient) GetAlertHistory(ctx context.Context, circuitID string, limit int) ([]client.AlertRecord, error) {
	return nil, nil
}

func (m *mockClient) StreamAlerts(ctx context.Context) (<-chan client.AlertEvent, <-chan error, error) {
	return nil, nil, nil
}

func (m *mockClient) SubmitFeedback(ctx context.Context, req client.FeedbackRequest) (*client.FeedbackResponse, error) {
	return nil, nil
}

func (m *mockClient) DryRunEval(ctx context.Context, req client.EvalRequest) (*client.EvalResponse, error) {
	return nil, nil
}

func (m *mockClient) GetCacheKeys(ctx context.Context, prefix string, limit int) ([]client.CacheKeyInfo, error) {
	return nil, nil
}

func (m *mockClient) InspectCacheKey(ctx context.Context, key string) (*client.CacheKeyDetail, error) {
	return nil, nil
}

func (m *mockClient) Close() error {
	return nil
}

func sampleTestCircuits() []client.CircuitSummary {
	// Build a rich test DAG
	root := types.NewNode("filter_high_value").
		WithCondition("payload.amount >= 1000.0").
		Step(flux.Volt("let is_suspicious = payload.amount > 5000; payload.is_suspicious = is_suspicious"))

	child1 := types.NewNode("alert_branch").
		WithCondition("payload.is_suspicious == true").
		Step(flux.Sink("fraud_alerts", "kafka")).
		Step(flux.Return(map[string]any{"risk": "HIGH", "action": "BLOCK"}))

	child2 := types.NewNode("audit_branch").
		Step(flux.Sink("audit_logs", "s3")).
		Step(flux.Abort())

	root.AddChildren(child1, child2)

	circuit1 := types.NewCircuit("fraud_sentinel").
		WithTags("stream:payments", "fraud").
		WithRoot(root)
	circuit1.Version = 2

	c1 := client.CircuitSummary{
		ID:            "fraud_sentinel",
		Tags:          []string{"stream:payments", "fraud"},
		RootCondition: "payload.amount >= 1000.0",
		NodeCount:     3,
		Status:        "LIVE",
		Revision:      2,
		CreatedAt:     time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC),
		Definition:    circuit1,
	}

	circuit2 := types.NewCircuit("metrics_tap").
		WithTags("stream:metrics", "telemetry").
		WithRoot(types.NewNode("counter").Step(flux.Volt("state.count = state.count + 1")))

	c2 := client.CircuitSummary{
		ID:            "metrics_tap",
		Tags:          []string{"stream:metrics", "telemetry"},
		RootCondition: "payload.metric != ''",
		NodeCount:     1,
		Status:        "SHADOW",
		Revision:      1,
		CreatedAt:     time.Date(2026, 10, 9, 13, 0, 0, 0, time.UTC),
		Definition:    circuit2,
	}

	return []client.CircuitSummary{c1, c2}
}

func TestCircuitsView_InitializationAndLoading(t *testing.T) {
	mock := &mockClient{circuits: sampleTestCircuits()}
	view := circuits.New(mock)
	view.SetDimensions(100, 30)

	// Before loading msg: shows loading indicator
	viewOutput := view.View()
	assert.Contains(t, viewOutput, "Connecting and fetching circuits")

	// Deliver loaded message
	cmd := view.Init()
	require.NotNil(t, cmd)
	msg := cmd()
	newView, _ := view.Update(msg)

	assert.Len(t, newView.Circuits(), 2)
	assert.False(t, newView.IsDetail())
	assert.False(t, newView.IsFiltering())

	tableOutput := newView.View()
	assert.Contains(t, tableOutput, "fraud_sentinel")
	assert.Contains(t, tableOutput, "metrics_tap")
	assert.Contains(t, tableOutput, "LIVE")
	assert.Contains(t, tableOutput, "SHADOW")
	assert.Contains(t, tableOutput, "stream:payments")
}

func TestCircuitsView_EmptyState(t *testing.T) {
	mock := &mockClient{circuits: []client.CircuitSummary{}}
	view := circuits.New(mock)
	view.SetDimensions(100, 30)

	newView, _ := view.Update(circuits.CircuitsLoadedMsg{Circuits: nil})
	output := newView.View()
	assert.Contains(t, output, "No circuits currently registered")
}

func TestCircuitsView_FuzzyFilter(t *testing.T) {
	mock := &mockClient{circuits: sampleTestCircuits()}
	view := circuits.New(mock)
	view.SetDimensions(100, 30)
	view, _ = view.Update(circuits.CircuitsLoadedMsg{Circuits: mock.circuits})

	// Press '/' to start filtering
	view, _ = view.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'/'}})
	assert.True(t, view.IsFiltering())

	// Type 'fraud'
	for _, r := range "fraud" {
		view, _ = view.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
	}

	out := view.View()
	assert.Contains(t, out, "fraud_sentinel")
	assert.NotContains(t, out, "metrics_tap")

	// Submit filter with Enter
	view, _ = view.Update(tea.KeyMsg{Type: tea.KeyEnter})
	assert.False(t, view.IsFiltering())
	assert.Contains(t, view.View(), "fraud_sentinel")

	// Clear filter with Esc
	view, _ = view.Update(tea.KeyMsg{Type: tea.KeyEscape})
	assert.Contains(t, view.View(), "metrics_tap")
}

func TestCircuitsView_DetailSplitScreen(t *testing.T) {
	mock := &mockClient{circuits: sampleTestCircuits()}
	view := circuits.New(mock)
	view.SetDimensions(120, 35)
	view, _ = view.Update(circuits.CircuitsLoadedMsg{Circuits: mock.circuits})

	// Press Enter to open detail split-screen
	view, cmd := view.Update(tea.KeyMsg{Type: tea.KeyEnter})
	assert.True(t, view.IsDetail())
	_ = cmd

	detailView := view.View()
	// Left Panel: Tree
	assert.Contains(t, detailView, "DAG Hierarchy & Tree")
	assert.Contains(t, detailView, "filter_high_value")
	assert.Contains(t, detailView, "alert_branch")
	assert.Contains(t, detailView, "audit_branch")
	assert.Contains(t, detailView, "VOLT")
	assert.Contains(t, detailView, "SINK")

	// Right Panel: Details Inspector
	assert.Contains(t, detailView, "Circuit Specification & Grammar")
	assert.Contains(t, detailView, "fraud_sentinel")
	assert.Contains(t, detailView, "r2")
	assert.Contains(t, detailView, "ROOT GUARD EXPRESSION")

	// Tab toggles focus
	view, _ = view.Update(tea.KeyMsg{Type: tea.KeyTab})
	assert.Contains(t, view.View(), "Circuit Specification & Grammar (Active)")

	// Scrolling keys (up/down/k/j/pgup/pgdown) work without crashing
	view, _ = view.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'j'}})
	view, _ = view.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'k'}})
	view, _ = view.Update(tea.KeyMsg{Type: tea.KeyPgDown})
	view, _ = view.Update(tea.KeyMsg{Type: tea.KeyPgUp})

	// Esc returns to table
	view, _ = view.Update(tea.KeyMsg{Type: tea.KeyEscape})
	assert.False(t, view.IsDetail())
	assert.Contains(t, view.View(), "Deployed Decision Pipelines")
}

func TestCircuitsView_RenderDAGTree(t *testing.T) {
	circuitsList := sampleTestCircuits()
	summary := circuitsList[0]

	tree := circuits.RenderDAGTree(&summary, 60)
	require.NotEmpty(t, tree)
	assert.Contains(t, tree, "fraud_sentinel")
	assert.Contains(t, tree, "filter_high_value")
	assert.Contains(t, tree, "alert_branch")
	assert.Contains(t, tree, "audit_branch")
	assert.Contains(t, tree, "VOLT")
	assert.Contains(t, tree, "SINK")
	assert.Contains(t, tree, "RETURN")
	assert.Contains(t, tree, "ABORT")
	assert.Contains(t, tree, "payload.amount >= 1000.0")

	// Test nil
	nilTree := circuits.RenderDAGTree(nil, 60)
	assert.Contains(t, nilTree, "no circuit selected")

	// Test empty definition
	emptyTree := circuits.RenderDAGTree(&client.CircuitSummary{ID: "empty"}, 60)
	assert.Contains(t, emptyTree, "empty DAG")
}

func TestCircuitsView_RenderCircuitDetails(t *testing.T) {
	circuitsList := sampleTestCircuits()
	summary := circuitsList[0]

	details := circuits.RenderCircuitDetails(&summary, 80)
	require.NotEmpty(t, details)
	assert.Contains(t, details, "CIRCUIT SPECIFICATION: fraud_sentinel")
	assert.Contains(t, details, "LIVE")
	assert.Contains(t, details, "stream:payments")
	assert.Contains(t, details, "payload.amount >= 1000.0")
	assert.Contains(t, details, "EXECUTION PIPELINE STEPS")
	assert.Contains(t, details, "filter_high_value")
	assert.Contains(t, details, "alert_branch")
	assert.Contains(t, details, "fraud_alerts")

	// Test nil
	nilDetails := circuits.RenderCircuitDetails(nil, 80)
	assert.Contains(t, nilDetails, "no circuit selected")
}

func TestCircuitsView_HighlightCode(t *testing.T) {
	code := `if (payload.amount > 1000 && state.active == true) { return "ALERT"; }`
	highlighted := circuits.HighlightCode(code)
	require.NotEmpty(t, highlighted)
	// Output should contain words
	assert.Contains(t, highlighted, "payload")
	assert.Contains(t, highlighted, "state")
	assert.Contains(t, highlighted, "ALERT")
}

func TestCircuitsView_LinterModal(t *testing.T) {
	mock := &mockClient{circuits: sampleTestCircuits()}
	view := circuits.New(mock)
	view.SetDimensions(100, 30)
	msg := view.Init()()
	view, _ = view.Update(msg)

	assert.False(t, view.IsModalActive())

	// 1. Press 'l' to open linter modal
	view, cmd := view.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'l'}})
	assert.Nil(t, cmd)
	assert.True(t, view.IsModalActive())
	assert.True(t, view.LinterModal().IsActive())

	// Modal output should render diagnostics
	modalOut := view.View()
	assert.Contains(t, modalOut, "Circuit Linter Static Analysis")
	assert.Contains(t, modalOut, "fraud_sentinel")

	// 2. Test navigation within modal
	view, _ = view.Update(tea.KeyMsg{Type: tea.KeyDown})
	view, _ = view.Update(tea.KeyMsg{Type: tea.KeyUp})
	view, _ = view.Update(tea.KeyMsg{Type: tea.KeyPgDown})
	view, _ = view.Update(tea.KeyMsg{Type: tea.KeyPgUp})
	assert.True(t, view.IsModalActive())

	// 3. Test closing modal with 'l'
	view, _ = view.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'l'}})
	assert.False(t, view.IsModalActive())
	assert.False(t, view.LinterModal().IsActive())

	// 4. Test opening and closing with 'Esc'
	view, _ = view.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'l'}})
	assert.True(t, view.IsModalActive())
	view, _ = view.Update(tea.KeyMsg{Type: tea.KeyEscape})
	assert.False(t, view.IsModalActive())

	// 5. Test opening in detail mode
	view, _ = view.Update(tea.KeyMsg{Type: tea.KeyEnter})
	assert.True(t, view.IsDetail())
	view, _ = view.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'l'}})
	assert.True(t, view.IsModalActive())
	view, _ = view.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'q'}})
	assert.False(t, view.IsModalActive())
	assert.True(t, view.IsDetail())
}

func TestCircuitsView_HotSwap(t *testing.T) {
	mock := &mockClient{circuits: sampleTestCircuits()}
	view := circuits.New(mock)
	view.SetDimensions(100, 30)
	msg := view.Init()()
	view, _ = view.Update(msg)

	// 1. Press 'r' to open Hot-Swap modal
	view, cmd := view.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'r'}})
	assert.NotNil(t, cmd)
	assert.True(t, view.IsModalActive())
	assert.Equal(t, circuits.ActionModalHotSwap, view.ActionState().ModalType())
	assert.Contains(t, view.View(), "Hot-Swap Circuit Pipeline")

	// 2. Press Esc to cancel
	view, _ = view.Update(tea.KeyMsg{Type: tea.KeyEscape})
	assert.False(t, view.IsModalActive())

	// 3. Press 'r' and hit Enter with blank -> acts as refresh
	view, _ = view.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'r'}})
	assert.True(t, view.IsModalActive())
	view, refreshCmd := view.Update(tea.KeyMsg{Type: tea.KeyEnter})
	assert.False(t, view.IsModalActive())
	require.NotNil(t, refreshCmd)

	// 4. Press 'r' with non-existent file -> error status
	view, _ = view.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'r'}})
	for _, r := range "/tmp/non_existent_circuit.json" {
		view, _ = view.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
	}
	view, _ = view.Update(tea.KeyMsg{Type: tea.KeyEnter})
	assert.False(t, view.IsModalActive())
	assert.Contains(t, view.StatusMsg(), "Hot-swap failed")

	// 5. Create a valid JSON circuit file in temp dir
	tmpDir := t.TempDir()
	validJSON := filepath.Join(tmpDir, "new_circuit.json")
	jsonContent := `{
		"id": "hotswap_circuit",
		"tags": ["hotswap", "test"],
		"root": {
			"name": "root_node",
			"condition": "payload.valid == true",
			"steps": [{"type": 0, "script": "let ok = true"}]
		}
	}`
	err := os.WriteFile(validJSON, []byte(jsonContent), 0644)
	require.NoError(t, err)

	view, _ = view.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'r'}})
	for _, r := range validJSON {
		view, _ = view.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
	}
	view, putCmd := view.Update(tea.KeyMsg{Type: tea.KeyEnter})
	assert.False(t, view.IsModalActive())
	assert.Contains(t, view.StatusMsg(), "Hot-swapped hotswap_circuit successfully")
	require.NotNil(t, putCmd)

	// Execute putCmd
	loadedMsg := putCmd()
	view, _ = view.Update(loadedMsg)
	assert.Len(t, view.Circuits(), 3)

	// 6. Test with valid YAML circuit file
	validYAML := filepath.Join(tmpDir, "yaml_circuit.yaml")
	yamlContent := `
id: yaml_hotswap
tags:
  - yaml
  - production
root:
  name: yaml_root
  condition: payload.amount > 0
`
	err = os.WriteFile(validYAML, []byte(yamlContent), 0644)
	require.NoError(t, err)

	view, _ = view.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'r'}})
	for _, r := range validYAML {
		view, _ = view.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
	}
	view, putCmd2 := view.Update(tea.KeyMsg{Type: tea.KeyEnter})
	assert.False(t, view.IsModalActive())
	assert.Contains(t, view.StatusMsg(), "Hot-swapped yaml_hotswap successfully")
	require.NotNil(t, putCmd2)
}

func TestCircuitsView_Export(t *testing.T) {
	mock := &mockClient{circuits: sampleTestCircuits()}
	view := circuits.New(mock)
	view.SetDimensions(100, 30)
	msg := view.Init()()
	view, _ = view.Update(msg)

	// 1. Press 'e' to open Export prompt
	view, cmd := view.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'e'}})
	assert.NotNil(t, cmd)
	assert.True(t, view.IsModalActive())
	assert.Equal(t, circuits.ActionModalExport, view.ActionState().ModalType())
	assert.Contains(t, view.View(), "Export Circuit Definition")

	// 2. Press Esc to cancel
	view, _ = view.Update(tea.KeyMsg{Type: tea.KeyEscape})
	assert.False(t, view.IsModalActive())

	// 3. Export to JSON file in temp dir
	tmpDir := t.TempDir()
	exportPathJSON := filepath.Join(tmpDir, "exported_fraud.json")

	view, _ = view.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'e'}})
	// Clear default and type path
	for range 30 {
		view, _ = view.Update(tea.KeyMsg{Type: tea.KeyBackspace})
	}
	for _, r := range exportPathJSON {
		view, _ = view.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
	}
	view, _ = view.Update(tea.KeyMsg{Type: tea.KeyEnter})
	assert.False(t, view.IsModalActive())
	assert.Contains(t, view.StatusMsg(), "Exported fraud_sentinel")

	content, err := os.ReadFile(exportPathJSON)
	require.NoError(t, err)
	assert.Contains(t, string(content), "fraud_sentinel")

	// 4. Export to YAML file
	exportPathYAML := filepath.Join(tmpDir, "exported_fraud.yaml")
	view, _ = view.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'e'}})
	for range 40 {
		view, _ = view.Update(tea.KeyMsg{Type: tea.KeyBackspace})
	}
	for _, r := range exportPathYAML {
		view, _ = view.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
	}
	view, _ = view.Update(tea.KeyMsg{Type: tea.KeyEnter})
	assert.False(t, view.IsModalActive())
	assert.Contains(t, view.StatusMsg(), "Exported fraud_sentinel")

	yamlData, err := os.ReadFile(exportPathYAML)
	require.NoError(t, err)
	assert.Contains(t, string(yamlData), "fraud_sentinel")
}

func TestCircuitsView_DryRunHandoff(t *testing.T) {
	mock := &mockClient{circuits: sampleTestCircuits()}
	view := circuits.New(mock)
	view.SetDimensions(100, 30)
	msg := view.Init()()
	view, _ = view.Update(msg)

	// Press 'd' in table mode -> returns DryRunHandoffMsg
	view, cmd := view.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'d'}})
	require.NotNil(t, cmd)
	handoffMsg := cmd()
	require.IsType(t, circuits.DryRunHandoffMsg{}, handoffMsg)
	dryMsg := handoffMsg.(circuits.DryRunHandoffMsg)
	assert.Equal(t, "fraud_sentinel", dryMsg.CircuitID)
	assert.NotNil(t, dryMsg.Circuit)

	// Test 'd' in detail mode
	view, _ = view.Update(tea.KeyMsg{Type: tea.KeyEnter})
	assert.True(t, view.IsDetail())
	view, cmd2 := view.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'d'}})
	require.NotNil(t, cmd2)
	handoffMsg2 := cmd2()
	require.IsType(t, circuits.DryRunHandoffMsg{}, handoffMsg2)
	assert.Equal(t, "fraud_sentinel", handoffMsg2.(circuits.DryRunHandoffMsg).CircuitID)
}

func TestCircuitsView_LoadCircuitFromDisk_Errors(t *testing.T) {
	// Empty path
	_, err := circuits.LoadCircuitFromDisk("")
	assert.Error(t, err)

	// Missing file
	_, err = circuits.LoadCircuitFromDisk("/path/does/not/exist.json")
	assert.Error(t, err)

	tmpDir := t.TempDir()

	// Invalid syntax
	badJSON := filepath.Join(tmpDir, "bad.json")
	_ = os.WriteFile(badJSON, []byte("{invalid-json"), 0644)
	_, err = circuits.LoadCircuitFromDisk(badJSON)
	assert.Error(t, err)

	// Missing ID
	noID := filepath.Join(tmpDir, "no_id.json")
	_ = os.WriteFile(noID, []byte(`{"root": {"name": "n1"}}`), 0644)
	_, err = circuits.LoadCircuitFromDisk(noID)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing mandatory 'id' field")

	// Missing Root
	noRoot := filepath.Join(tmpDir, "no_root.json")
	_ = os.WriteFile(noRoot, []byte(`{"id": "c1"}`), 0644)
	_, err = circuits.LoadCircuitFromDisk(noRoot)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing mandatory 'root' execution node")
}
