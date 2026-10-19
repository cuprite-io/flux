package circuits_test

import (
	"context"
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
	return &client.StatusResponse{Service: "flux-admin", Version: "0.23.0"}, nil
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
