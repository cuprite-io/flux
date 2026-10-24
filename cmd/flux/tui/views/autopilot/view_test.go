package autopilot_test

import (
	"context"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/cuprite-io/flux/cmd/flux/tui/client"
	autopilotview "github.com/cuprite-io/flux/cmd/flux/tui/views/autopilot"
	"github.com/cuprite-io/flux/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockClient struct {
	sessions []client.AutopilotSession
	err      error
}

func (m *mockClient) Ping(ctx context.Context) error                             { return nil }
func (m *mockClient) GetStatus(ctx context.Context) (*client.StatusResponse, error) { return nil, nil }
func (m *mockClient) GetCircuits(ctx context.Context) ([]client.CircuitSummary, error) {
	return nil, nil
}
func (m *mockClient) GetCircuit(ctx context.Context, id string) (*client.CircuitSummary, error) {
	return nil, nil
}
func (m *mockClient) PutCircuit(ctx context.Context, circuit *types.Circuit) (*client.CircuitSummary, error) {
	return nil, nil
}
func (m *mockClient) GetAutopilot(ctx context.Context) ([]client.AutopilotSession, error) {
	return m.sessions, m.err
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
func (m *mockClient) GetClusterTopology(ctx context.Context) (*client.ClusterTopology, error) {
	return nil, nil
}
func (m *mockClient) Close() error { return nil }

func sampleLiveCircuit() *types.Circuit {
	return &types.Circuit{
		ID:      "fraud-alert-v1",
		Tags:    []string{"transactions"},
		Version: 1,
		Root: &types.Node{
			Name:      "check_amount",
			Condition: "payload.amount > 5000",
			Steps: []*types.StepDefinition{
				{
					Type:     types.StepSink,
					SinkName: "slack_ops",
				},
			},
		},
	}
}

func sampleCandidateCircuit() *types.Circuit {
	return &types.Circuit{
		ID:      "fraud-alert-v2",
		Tags:    []string{"transactions"},
		Version: 2,
		Root: &types.Node{
			Name:      "check_amount_and_country",
			Condition: "payload.amount > 2000 && payload.country != 'US'",
			Steps: []*types.StepDefinition{
				{
					Type:     types.StepSink,
					SinkName: "slack_ops",
				},
				{
					Type:     types.StepSink,
					SinkName: "pagerduty_security",
				},
			},
		},
	}
}

func TestCircuitDiff_ComputeAndRender(t *testing.T) {
	live := sampleLiveCircuit()
	cand := sampleCandidateCircuit()

	diff := autopilotview.ComputeCircuitDiff(live, cand)
	assert.True(t, diff.HasDiff)
	assert.Greater(t, diff.Additions, 0)
	assert.Greater(t, diff.Deletions, 0)

	// Unified diff rendering
	unified := autopilotview.RenderUnifiedDiff(live, cand, 100)
	require.NotEmpty(t, unified)
	assert.Contains(t, unified, "Circuit Diff: Live [fraud-alert-v1] vs Candidate [fraud-alert-v2]")
	assert.Contains(t, unified, "additions")
	assert.Contains(t, unified, "deletions")
	assert.Contains(t, unified, "slack_ops")
	assert.Contains(t, unified, "pagerduty_security")

	// Side-by-side diff rendering
	sbs := autopilotview.RenderSideBySideDiff(live, cand, 120)
	require.NotEmpty(t, sbs)
	assert.Contains(t, sbs, "Active: fraud-alert-v1")
	assert.Contains(t, sbs, "Candidate: fraud-alert-v2")

	// Same circuit diff -> 0 differences
	sameDiff := autopilotview.ComputeCircuitDiff(live, live)
	assert.False(t, sameDiff.HasDiff)
	sameUnified := autopilotview.RenderUnifiedDiff(live, live, 100)
	assert.Contains(t, sameUnified, "0 differences")
}

func TestAuditLog_Rendering(t *testing.T) {
	sess := client.AutopilotSession{
		SessionID: "autopilot-sess-1",
		Prompt:    "Detect high-value foreign transfers and alert security",
		Status:    "LIVE",
		Tags:      []string{"transactions", "payments"},
		AuditLog: &client.AutopilotAuditLog{
			PromptSent:     "Synthesize an optimal fraud detection circuit DAG for payment streams.",
			RawResponse:    `{"id":"fraud-candidate","root":{"condition":"payload.amount > 1000"}}`,
			InferredSchema: `{"amount":"number","country":"string"}`,
			Exemplars:      []string{`{"amount":1500,"country":"CA"}`},
			Attempts:       2,
			CorrectionHistory: []client.AutopilotCorrection{
				{
					Attempt:   1,
					Candidate: `{"id":"bad-circ"}`,
					Errors: []string{
						"[PARSE]: missing root node in circuit DAG",
						"[LINT:LINT-022]: unregistered sink target 'unknown_sink'",
					},
				},
			},
		},
	}

	rendered := autopilotview.RenderAuditLog(&sess, 120)
	require.NotEmpty(t, rendered)
	assert.Contains(t, rendered, "autopilot-sess-1")
	assert.Contains(t, rendered, "transactions, payments")
	assert.Contains(t, rendered, "Detect high-value foreign transfers")
	assert.Contains(t, rendered, "Inferred Schema Tree")
	assert.Contains(t, rendered, "amount")
	assert.Contains(t, rendered, "Stream Exemplar Payloads")
	assert.Contains(t, rendered, "Prompt Dispatched to AI Model")
	assert.Contains(t, rendered, "Raw AI Model Output")
	assert.Contains(t, rendered, "Reflection & Self-Correction Loop")
	assert.Contains(t, rendered, "Total Reflection Turns: 2")
	assert.Contains(t, rendered, "Reflection Turn #1")
	assert.Contains(t, rendered, "LINT-022")
}

func TestAutopilotView_LifecycleAndInteraction(t *testing.T) {
	liveCirc := sampleLiveCircuit()
	candCirc := sampleCandidateCircuit()

	cli := &mockClient{
		sessions: []client.AutopilotSession{
			{
				SessionID:        "autopilot-1",
				Prompt:           "Flag rapid repeated login attempts",
				Status:           "SAMPLING",
				Tags:             []string{"auth"},
				SampledEvents:    24,
				SampleThreshold:  30,
				CandidateCircuit: candCirc,
				ActiveCircuit:    liveCirc,
			},
			{
				SessionID:       "autopilot-2",
				Prompt:          "Detect anomalous gaming boss kills",
				Status:          "SHADOWING",
				Tags:            []string{"gaming"},
				SampledEvents:   100,
				SampleThreshold: 100,
				StagingResult: &client.AutopilotStagingInfo{
					TotalEvents:  500,
					FiringEvents: 25,
					ObservedRate: 0.05,
					Status:       "EVALUATING",
					Promoted:     false,
				},
				CandidateCircuit: candCirc,
				ActiveCircuit:    liveCirc,
				AuditLog: &client.AutopilotAuditLog{
					PromptSent:  "Synthesize boss kill rules",
					RawResponse: `{"id":"boss-kill"}`,
					Attempts:    1,
				},
			},
			{
				SessionID:        "autopilot-3",
				Prompt:           "Adaptive MFA access anomaly",
				Status:           "LIVE",
				Tags:             []string{"access"},
				CandidateCircuit: candCirc,
				ActiveCircuit:    liveCirc,
			},
		},
	}

	m := autopilotview.New(cli)
	m.SetDimensions(140, 40)
	require.NotNil(t, m.Init())

	// 1. Process loaded sessions
	newM, _ := m.Update(autopilotview.AutopilotLoadedMsg{
		Sessions: cli.sessions,
	})
	model := newM

	assert.Equal(t, 3, len(model.Sessions()))
	require.NotNil(t, model.SelectedSession())
	assert.Equal(t, "autopilot-1", model.SelectedSession().SessionID)

	viewStr := model.View()
	require.NotEmpty(t, viewStr)
	assert.Contains(t, viewStr, "Autopilot Cockpit: 3 Sessions")
	assert.Contains(t, viewStr, "SAMPLING")
	assert.Contains(t, viewStr, "Flag rapid repeated login attempts")
	assert.Contains(t, viewStr, "Sampling Progress: [")
	assert.Contains(t, viewStr, "24/30 events (80%)")

	// 2. Test Down navigation to select session 2 (SHADOWING)
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyDown})
	model = newM
	require.NotNil(t, model.SelectedSession())
	assert.Equal(t, "autopilot-2", model.SelectedSession().SessionID)

	viewStr2 := model.View()
	assert.Contains(t, viewStr2, "Shadow Firing Rate:")
	assert.Contains(t, viewStr2, "0.050")
	assert.Contains(t, viewStr2, "SHADOW EVALUATION")

	// 3. Test Tab key (focus switch between panels)
	assert.Equal(t, 0, model.ActivePanel())
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyTab})
	model = newM
	assert.Equal(t, 1, model.ActivePanel())

	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyTab})
	model = newM
	assert.Equal(t, 0, model.ActivePanel())

	// 4. Test 'v' key (toggle between AI Audit Log and Circuit Diff)
	assert.Equal(t, 0, model.PanelTab())
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'v'}})
	model = newM
	assert.Equal(t, 1, model.PanelTab())
	assert.Contains(t, model.View(), "Circuit Diff: Live")

	// Test 'd' key (toggle diff mode between unified and side-by-side)
	assert.Equal(t, 0, model.DiffMode())
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'d'}})
	model = newM
	assert.Equal(t, 1, model.DiffMode())
	assert.Contains(t, model.View(), "Mode: Side-by-Side")

	// Test 'a' key (jump back to AI Audit Log)
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'a'}})
	model = newM
	assert.Equal(t, 0, model.PanelTab())
	assert.Contains(t, model.View(), "AI Autopilot Audit Log")

	// 5. Test '/' key (Filtering)
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'/'}})
	model = newM
	assert.True(t, model.IsFiltering())

	for _, r := range "gaming" {
		newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
		model = newM
	}
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyEnter})
	model = newM
	assert.False(t, model.IsFiltering())
	assert.Equal(t, "autopilot-2", model.SelectedSession().SessionID)

	// Clear filter with Esc
	newM, _ = model.Update(tea.KeyMsg{Type: tea.KeyEscape})
	model = newM
	assert.Equal(t, 3, len(model.Sessions()))

	// 6. Test 'r' key (Refresh)
	newM, refCmd := model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'r'}})
	model = newM
	assert.Contains(t, model.StatusMsg(), "Refreshing autopilot sessions")
	require.NotNil(t, refCmd)
}
