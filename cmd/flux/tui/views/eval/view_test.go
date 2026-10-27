package eval_test

import (
	"context"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/cuprite-io/flux/cmd/flux/tui/client"
	"github.com/cuprite-io/flux/cmd/flux/tui/views/circuits"
	"github.com/cuprite-io/flux/cmd/flux/tui/views/eval"
	"github.com/cuprite-io/flux/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockClient struct {
	circuits []client.CircuitSummary
}

func (m *mockClient) GetStatus(ctx context.Context) (*client.StatusResponse, error) {
	return &client.StatusResponse{Status: "HEALTHY", ActiveCircuits: len(m.circuits)}, nil
}

func (m *mockClient) GetCircuits(ctx context.Context) ([]client.CircuitSummary, error) {
	return m.circuits, nil
}

func (m *mockClient) GetCircuit(ctx context.Context, id string) (*client.CircuitSummary, error) {
	for _, c := range m.circuits {
		if c.ID == id {
			return &c, nil
		}
	}
	return nil, nil
}

func (m *mockClient) PutCircuit(ctx context.Context, circuit *types.Circuit) (*client.CircuitSummary, error) {
	return nil, nil
}

func (m *mockClient) GetAutopilot(ctx context.Context) ([]client.AutopilotSession, error) {
	return []client.AutopilotSession{
		{
			SessionID: "sess-1",
			Tags:      []string{"payments"},
			AuditLog: &client.AutopilotAuditLog{
				Exemplars: []string{
					`{"amount": 14000, "user_tier": "VIP", "country": "US"}`,
				},
			},
		},
	}, nil
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
	return &client.EvalResponse{Passed: true}, nil
}

func (m *mockClient) GetCacheKeys(ctx context.Context, prefix string, limit int) ([]client.CacheKeyInfo, error) {
	return nil, nil
}

func (m *mockClient) InspectCacheKey(ctx context.Context, key string) (*client.CacheKeyDetail, error) {
	return nil, nil
}

func (m *mockClient) GetClusterTopology(ctx context.Context) (*client.ClusterTopology, error) {
	return &client.ClusterTopology{TotalNodes: 1, HealthyNodes: 1}, nil
}

func (m *mockClient) Close() error {
	return nil
}

func testCircuitDefinition() *types.Circuit {
	return &types.Circuit{
		ID:   "fraud_sentinel",
		Tags: []string{"payments", "fraud"},
		Root: &types.Node{
			Name:      "root_guard",
			Condition: "payload.amount >= 1000.0",
			Steps: []*types.StepDefinition{
				{
					Type:   types.StepVolt,
					Script: "set('tier', payload.amount >= 10000.0 ? 'platinum' : 'gold')",
				},
				{
					Type:      types.StepSink,
					SinkName:  "slack_ops",
					Condition: "payload.amount >= 10000.0",
				},
			},
			Children: []*types.Node{
				{
					Name:      "cross_border_node",
					Condition: "payload.card_country != payload.ip_country",
					Steps: []*types.StepDefinition{
						{
							Type:   types.StepVolt,
							Script: "set('risk_score', 95)",
						},
						{
							Type:     types.StepSink,
							SinkName: "pagerduty_ops",
						},
					},
				},
				{
					Name:      "domestic_node",
					Condition: "payload.card_country == payload.ip_country",
					Steps: []*types.StepDefinition{
						{
							Type:   types.StepVolt,
							Script: "set('risk_score', 10)",
						},
					},
				},
			},
		},
	}
}

func TestEvalRunner_TakenAndPrunedBranches(t *testing.T) {
	circuit := testCircuitDefinition()
	payloadJSON := `{
		"amount": 15000.0,
		"card_country": "US",
		"ip_country": "UA"
	}`

	res, err := eval.RunCircuitEval(context.Background(), circuit, payloadJSON)
	require.NoError(t, err)
	require.NotNil(t, res)

	assert.Equal(t, "fraud_sentinel", res.CircuitID)
	assert.True(t, res.Passed)
	assert.Equal(t, 3, res.TotalNodes)
	assert.Equal(t, 2, res.PassedNodes) // root_guard + cross_border_node
	assert.Equal(t, 1, res.PrunedNodes) // domestic_node

	// Verify VoltScript mutation in state
	assert.Equal(t, "platinum", res.MutatedState["tier"])
	assert.Equal(t, int64(95), res.MutatedState["risk_score"])

	// Verify simulated sinks
	require.Len(t, res.SimulatedSinks, 2)
	assert.Equal(t, "slack_ops", res.SimulatedSinks[0].SinkName)
	assert.Equal(t, "root_guard", res.SimulatedSinks[0].NodeName)
	assert.Equal(t, "pagerduty_ops", res.SimulatedSinks[1].SinkName)
	assert.Equal(t, "cross_border_node", res.SimulatedSinks[1].NodeName)

	// Verify tree rendering
	treeView := eval.RenderEvaluationTree(res, 100)
	assert.Contains(t, treeView, "✓ PASSED")
	assert.Contains(t, treeView, "root_guard")
	assert.Contains(t, treeView, "✓ TAKEN")
	assert.Contains(t, treeView, "cross_border_node")
	assert.Contains(t, treeView, "domestic_node")
	assert.Contains(t, treeView, "✗ PRUNED")
	assert.Contains(t, treeView, "slack_ops")
	assert.Contains(t, treeView, "pagerduty_ops")
	assert.Contains(t, treeView, "Simulated Outbound Sink Inspector")
}

func TestEvalRunner_RootConditionPruned(t *testing.T) {
	circuit := testCircuitDefinition()
	payloadJSON := `{
		"amount": 500.0,
		"card_country": "US",
		"ip_country": "US"
	}`

	res, err := eval.RunCircuitEval(context.Background(), circuit, payloadJSON)
	require.NoError(t, err)
	require.NotNil(t, res)

	assert.False(t, res.Passed)
	assert.Equal(t, 3, res.TotalNodes)
	assert.Equal(t, 0, res.PassedNodes)
	assert.Equal(t, 3, res.PrunedNodes) // All nodes pruned
	assert.Empty(t, res.SimulatedSinks)

	treeView := eval.RenderEvaluationTree(res, 100)
	assert.Contains(t, treeView, "✗ FAILED")
	assert.Contains(t, treeView, "✗ PRUNED")
}

func TestEvalRunner_InvalidJSON(t *testing.T) {
	circuit := testCircuitDefinition()
	_, err := eval.RunCircuitEval(context.Background(), circuit, `{ invalid json`)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid JSON payload")
}

func TestEvalView_SplitScreenAndInteractions(t *testing.T) {
	circDef := testCircuitDefinition()
	mockCli := &mockClient{
		circuits: []client.CircuitSummary{
			{ID: "fraud_sentinel", Tags: []string{"payments"}, Definition: circDef},
			{ID: "rate_limiter", Tags: []string{"api"}, Definition: nil},
		},
	}

	model := eval.New(mockCli)
	model.SetDimensions(120, 30)

	// Populate circuits
	model, _ = model.Update(eval.CircuitsLoadedMsg{
		Circuits: mockCli.circuits,
	})

	// Verify initial view renders split screen panels
	viewStr := model.View()
	assert.Contains(t, viewStr, "Dry-Run Playground")
	assert.Contains(t, viewStr, "Event Payload (JSON)")
	assert.Contains(t, viewStr, "Live Branch Evaluation")

	// 1. Test Tab key toggles focus between panes
	model, _ = model.Update(tea.KeyMsg{Type: tea.KeyTab})
	assert.False(t, model.IsEditorFocused())

	model, _ = model.Update(tea.KeyMsg{Type: tea.KeyTab})
	assert.False(t, model.IsEditorFocused()) // in navigation mode

	// 2. Test 'i' enters Edit Mode
	model, cmd := model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'i'}})
	assert.True(t, model.IsEditorFocused())
	assert.NotNil(t, cmd)

	// 3. Test 'esc' exits Edit Mode
	model, _ = model.Update(tea.KeyMsg{Type: tea.KeyEscape})
	assert.False(t, model.IsEditorFocused())

	// 4. Test 'f' formats JSON
	model, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'f'}})
	assert.Contains(t, model.StatusMsg(), "Beautified JSON")

	// 5. Test 'c' cycles circuits
	model, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'c'}})
	assert.Contains(t, model.StatusMsg(), "Switched to circuit")

	// 6. Test 'r' runs evaluation
	model, runCmd := model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'r'}})
	require.NotNil(t, runCmd)
	msg := runCmd()
	require.IsType(t, eval.EvalCompletedMsg{}, msg)

	model, _ = model.Update(msg)
	assert.Contains(t, model.StatusMsg(), "Dry-run evaluated")

	// 7. Test 'e' pulls Schema Tap exemplar
	model, exCmd := model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'e'}})
	require.NotNil(t, exCmd)
	exMsg := exCmd()
	require.IsType(t, eval.ExemplarLoadedMsg{}, exMsg)

	model, _ = model.Update(exMsg)
	assert.Contains(t, model.StatusMsg(), "Loaded exemplar")

	// 8. Test HandleHandoff from `:circuits` view
	handoffCmd := model.HandleHandoff(circuits.DryRunHandoffMsg{
		CircuitID: "fraud_sentinel",
		Circuit:   circDef,
	})
	assert.NotNil(t, handoffCmd)
	assert.Equal(t, "fraud_sentinel", model.ActiveCircuitID())
	assert.Contains(t, model.StatusMsg(), "Transferred circuit")
}
