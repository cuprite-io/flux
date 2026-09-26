package flux_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cuprite-io/flux"
	"github.com/cuprite-io/flux/internal/sink"
	"github.com/cuprite-io/flux/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEngine_AlertHistoryAndFeedback(t *testing.T) {
	ctx := context.Background()

	// 1. Initialize Engine with History enabled
	eng, err := flux.New(
		flux.WithHistory(true),
	)
	require.NoError(t, err)
	defer eng.Close()

	hist := eng.History()
	require.NotNil(t, hist)

	var mu sync.Mutex
	var dispatchedPayload any
	mockSink := sink.FuncSink(func(ctx context.Context, payload any) error {
		mu.Lock()
		dispatchedPayload = payload
		mu.Unlock()
		return nil
	})

	eng.RegisterSink("emergency_alert", mockSink,
		flux.WithSinkDescription("Critical outage handler"),
		flux.WithSinkSeverity(flux.SinkSeverityCritical),
	)

	// Build circuit with StepSink triggered when statusCode >= 500
	root := types.NewNode("http_eval").
		Step(&types.StepDefinition{
			Type:      types.StepSink,
			SinkName:  "emergency_alert",
			Condition: "payload.status_code >= 500",
		})
	circuit := types.NewCircuit("circuit_api_gateway").
		WithTags("stream:api").
		WithRoot(root)
	require.NoError(t, eng.Registry().Put(ctx, circuit))

	// Ingest 200 OK event -> StepSink should NOT fire
	res1, err := eng.Spark(ctx, map[string]any{
		"status_code": 200,
		"user_email":  "user@example.com",
	}, "stream:api")
	require.NoError(t, err)
	assert.True(t, res1.Passed)
	mu.Lock()
	p1 := dispatchedPayload
	mu.Unlock()
	assert.Nil(t, p1)

	// Ingest 503 ERROR event -> StepSink FIRES
	res2, err := eng.Spark(ctx, map[string]any{
		"status_code": 503,
		"user_email":  "victim@example.com",
		"password":    "secret_token",
		"service":     "checkout",
	}, "stream:api")
	require.NoError(t, err)
	assert.True(t, res2.Passed)

	// Sinks are dispatched asynchronously by SinkRegistry workers
	time.Sleep(25 * time.Millisecond)
	mu.Lock()
	p2 := dispatchedPayload
	mu.Unlock()
	assert.NotNil(t, p2)

	// Flush async history queue
	require.NoError(t, hist.Flush())

	// Query recorded alert history
	alerts, err := eng.GetAlertHistory(ctx, "circuit_api_gateway", 10)
	require.NoError(t, err)
	require.Len(t, alerts, 1)

	alert := alerts[0]
	assert.NotEmpty(t, alert.ID)
	assert.Equal(t, "circuit_api_gateway", alert.CircuitID)
	assert.Equal(t, "http_eval", alert.NodeName)
	assert.Equal(t, "emergency_alert", alert.SinkName)
	assert.Equal(t, "payload.status_code >= 500", alert.Condition)

	// Verify PII was sanitized before storing in CacheBackend
	email, _ := alert.Payload["user_email"].(string)
	pwd, _ := alert.Payload["password"].(string)
	assert.Contains(t, email, "****@example.com")
	assert.Equal(t, "[REDACTED]", pwd)

	// Verify alert stats
	stats, err := eng.GetAlertStats(ctx, "circuit_api_gateway")
	require.NoError(t, err)
	assert.Equal(t, uint64(1), stats.TotalFired)
	assert.Equal(t, uint64(1), stats.BySink["emergency_alert"])

	// Submit operator feedback
	feedback := flux.AlertFeedback{
		Classification: flux.FeedbackFalsePositive,
		Reason:         "Transient upstream network glitch during switch restart",
		Author:         "noc-engineer",
	}
	err = eng.RecordAlertFeedback(ctx, "circuit_api_gateway", alert.ID, feedback)
	require.NoError(t, err)

	// Re-query alert to verify feedback attached
	updatedAlerts, err := eng.GetAlertHistory(ctx, "circuit_api_gateway", 10)
	require.NoError(t, err)
	require.Len(t, updatedAlerts, 1)
	require.NotNil(t, updatedAlerts[0].Feedback)
	assert.Equal(t, flux.FeedbackFalsePositive, updatedAlerts[0].Feedback.Classification)
	assert.Equal(t, "Transient upstream network glitch during switch restart", updatedAlerts[0].Feedback.Reason)
	assert.Equal(t, "noc-engineer", updatedAlerts[0].Feedback.Author)
}

func TestEngine_AlertHistory_DisabledByDefault(t *testing.T) {
	ctx := context.Background()
	eng, err := flux.New()
	require.NoError(t, err)
	defer eng.Close()

	assert.Nil(t, eng.History())

	// Calling history methods should return clear error when disabled
	_, err = eng.GetAlertHistory(ctx, "c1", 5)
	assert.Error(t, err)

	err = eng.RecordAlertFeedback(ctx, "c1", "a1", flux.AlertFeedback{})
	assert.Error(t, err)

	_, err = eng.GetAlertStats(ctx, "c1")
	assert.Error(t, err)
}
