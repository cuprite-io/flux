package alerts_test

import (
	"context"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/cuprite-io/flux/cmd/flux/tui/client"
	"github.com/cuprite-io/flux/cmd/flux/tui/views/alerts"
	"github.com/cuprite-io/flux/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockAlertClient struct {
	historyAlerts []client.AlertRecord
	submittedReq  *client.FeedbackRequest
	streamCh      chan client.AlertEvent
}

func (m *mockAlertClient) GetStatus(ctx context.Context) (*client.StatusResponse, error) {
	return &client.StatusResponse{Status: "HEALTHY"}, nil
}

func (m *mockAlertClient) GetCircuits(ctx context.Context) ([]client.CircuitSummary, error) {
	return nil, nil
}

func (m *mockAlertClient) GetCircuit(ctx context.Context, id string) (*client.CircuitSummary, error) {
	return nil, nil
}

func (m *mockAlertClient) PutCircuit(ctx context.Context, circuit *types.Circuit) (*client.CircuitSummary, error) {
	return nil, nil
}

func (m *mockAlertClient) GetAutopilot(ctx context.Context) ([]client.AutopilotSession, error) {
	return nil, nil
}

func (m *mockAlertClient) GetAlertHistory(ctx context.Context, circuitID string, limit int) ([]client.AlertRecord, error) {
	return m.historyAlerts, nil
}

func (m *mockAlertClient) StreamAlerts(ctx context.Context) (<-chan client.AlertEvent, <-chan error, error) {
	if m.streamCh == nil {
		m.streamCh = make(chan client.AlertEvent, 10)
	}
	errCh := make(chan error, 1)
	return m.streamCh, errCh, nil
}

func (m *mockAlertClient) SubmitFeedback(ctx context.Context, req client.FeedbackRequest) (*client.FeedbackResponse, error) {
	m.submittedReq = &req
	return &client.FeedbackResponse{
		Status:    "recorded",
		AlertID:   req.AlertID,
		Timestamp: time.Now().UTC(),
	}, nil
}

func (m *mockAlertClient) DryRunEval(ctx context.Context, req client.EvalRequest) (*client.EvalResponse, error) {
	return &client.EvalResponse{Passed: true}, nil
}

func (m *mockAlertClient) GetCacheKeys(ctx context.Context, prefix string, limit int) ([]client.CacheKeyInfo, error) {
	return nil, nil
}

func (m *mockAlertClient) InspectCacheKey(ctx context.Context, key string) (*client.CacheKeyDetail, error) {
	return nil, nil
}

func (m *mockAlertClient) GetClusterTopology(ctx context.Context) (*client.ClusterTopology, error) {
	return &client.ClusterTopology{TotalNodes: 1, HealthyNodes: 1}, nil
}

func (m *mockAlertClient) Close() error {
	return nil
}

func TestFeedbackModal_LifecycleAndSelection(t *testing.T) {
	modal := alerts.NewFeedbackModal()
	modal.SetDimensions(120, 30)

	assert.False(t, modal.IsActive())

	testAlert := client.AlertRecord{
		ID:        "alt-1001",
		CircuitID: "fraud_sentinel",
		NodeName:  "velocity_gate",
		SinkName:  "slack_ops",
		Condition: "payload.amount >= 10000.0",
		Payload: map[string]any{
			"amount": 15000.0,
			"card":   "4111...",
		},
	}

	// 1. Open Modal
	modal.Open(testAlert)
	assert.True(t, modal.IsActive())
	assert.Equal(t, alerts.FeedbackFalsePositive, modal.SelectedType())
	assert.Equal(t, "alt-1001", modal.TargetAlert().ID)

	// 2. Select different classification tags
	modal.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'2'}}, nil)
	assert.Equal(t, alerts.FeedbackValid, modal.SelectedType())

	modal.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'3'}}, nil)
	assert.Equal(t, alerts.FeedbackNoisy, modal.SelectedType())

	modal.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'4'}}, nil)
	assert.Equal(t, alerts.FeedbackMuted, modal.SelectedType())

	modal.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'1'}}, nil)
	assert.Equal(t, alerts.FeedbackFalsePositive, modal.SelectedType())

	// 3. Tab to notes input and type
	modal.Update(tea.KeyMsg{Type: tea.KeyTab}, nil)
	for _, r := range "test whitelisted transaction" {
		modal.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}}, nil)
	}
	assert.Equal(t, "test whitelisted transaction", modal.Notes())

	// 4. Tab to operator input and verify default
	modal.Update(tea.KeyMsg{Type: tea.KeyTab}, nil)
	assert.Equal(t, "operator", modal.Operator())

	// 5. Test rendering contains details
	viewStr := modal.View()
	assert.Contains(t, viewStr, "Submit Operator Feedback on Alert")
	assert.Contains(t, viewStr, "alt-1001")
	assert.Contains(t, viewStr, "fraud_sentinel")
	assert.Contains(t, viewStr, "slack_ops")
	assert.Contains(t, viewStr, "false_positive")
	assert.Contains(t, viewStr, "test whitelisted transaction")

	// 6. Test Esc dismisses modal
	modal.Update(tea.KeyMsg{Type: tea.KeyEscape}, nil)
	assert.False(t, modal.IsActive())
}

func TestAlertsView_StreamAndFeedbackFlow(t *testing.T) {
	mockCli := &mockAlertClient{
		historyAlerts: []client.AlertRecord{
			{
				ID:        "alt-init-1",
				CircuitID: "payment_fraud",
				NodeName:  "amount_check",
				SinkName:  "slack_alerts",
				Condition: "payload.amount > 5000",
				Payload: map[string]any{
					"amount": 7500.0,
					"user":   "alice",
				},
			},
		},
	}

	model := alerts.New(mockCli)
	model.SetDimensions(120, 30)

	// 1. Initial view rendering
	viewStr := model.View()
	assert.Contains(t, viewStr, "Real-Time Alert Stream")
	assert.Contains(t, viewStr, "STREAM LIVE")

	// 2. Load historical alerts
	model, _ = model.Update(alerts.AlertHistoryLoadedMsg{
		Alerts: mockCli.historyAlerts,
	})
	assert.Len(t, model.Alerts(), 1)
	assert.Equal(t, "alt-init-1", model.SelectedAlert().ID)

	// Verify unreviewed badge
	viewStr = model.View()
	assert.Contains(t, viewStr, "alt-init-1")
	assert.Contains(t, viewStr, "payment_fraud")
	assert.Contains(t, viewStr, "UNREVIEWED")

	// 3. Receive Live SSE stream event
	streamEvt := client.AlertEvent{
		ID:        "alt-live-2",
		CircuitID: "rate_limiter",
		NodeName:  "api_gate",
		SinkName:  "pagerduty_ops",
		Condition: "payload.qps > 1000",
		Payload: map[string]any{
			"qps":  1500,
			"host": "gateway-1",
		},
		Timestamp: time.Now(),
	}

	model, _ = model.Update(alerts.AlertStreamEventMsg{
		Event: streamEvt,
	})
	assert.Len(t, model.Alerts(), 2)
	assert.Equal(t, "alt-live-2", model.Alerts()[0].ID) // prepended to top

	// 4. Test Stream Pause/Resume ('p')
	model, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'p'}})
	assert.Contains(t, model.View(), "STREAM PAUSED")

	model, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'p'}})
	assert.Contains(t, model.View(), "STREAM LIVE")

	// 5. Test Filter ('/')
	model, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'/'}})
	assert.True(t, model.IsFiltering())

	for _, r := range "rate_limiter" {
		model, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
	}
	model, _ = model.Update(tea.KeyMsg{Type: tea.KeyEnter})
	assert.False(t, model.IsFiltering())

	filteredView := model.View()
	assert.Contains(t, filteredView, "rate_limiter")
	assert.NotContains(t, filteredView, "payment_fraud")

	// Clear filter with Esc
	model, _ = model.Update(tea.KeyMsg{Type: tea.KeyEscape})
	assert.Contains(t, model.View(), "payment_fraud")

	// 6. Test Feedback Modal Submission ('f')
	model, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'f'}})
	assert.True(t, model.IsModalActive())

	// Type note
	model, _ = model.Update(tea.KeyMsg{Type: tea.KeyTab})
	for _, r := range "known stress test spike" {
		model, _ = model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
	}

	// Submit modal with Enter
	model, subCmd := model.Update(tea.KeyMsg{Type: tea.KeyEnter})
	require.NotNil(t, subCmd)
	subMsg := subCmd()
	require.IsType(t, alerts.FeedbackSubmittedMsg{}, subMsg)

	// Feed response message back to model
	model, _ = model.Update(subMsg)
	assert.False(t, model.IsModalActive())
	assert.Contains(t, model.StatusMsg(), "Recorded feedback")

	// Verify feedback badge updated in table view
	assert.Contains(t, model.View(), "FALSE POSITIVE ✗")

	// Verify client API was actually called
	require.NotNil(t, mockCli.submittedReq)
	assert.Equal(t, "false_positive", mockCli.submittedReq.FeedbackType)
	assert.Equal(t, "known stress test spike", mockCli.submittedReq.Notes)
}
