package history_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cuprite-io/flux/internal/cache"
	"github.com/cuprite-io/flux/internal/history"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHistoryStore_RecordAndQuery(t *testing.T) {
	ctx := context.Background()
	backend := cache.NewMemoryCache()
	defer backend.Close()

	cfg := history.DefaultConfig()
	cfg.Async = false // synchronous for direct verification
	store, err := history.New(backend, cfg)
	require.NoError(t, err)
	defer store.Close()

	rec := history.AlertRecord{
		CircuitID: "circuit_fraud_detector",
		NodeName:  "flag_high_risk",
		SinkName:  "emergency_webhook",
		Condition: "risk_score > 85",
		Payload: map[string]any{
			"user_email":  "john.doe@example.com",
			"credit_card": "4111-2222-3333-4444",
			"password":    "supersecret123",
			"amount":      1250.50,
			"risk_score":  92,
		},
	}

	err = store.Record(ctx, rec)
	require.NoError(t, err)

	// Query alert records
	alerts, err := store.GetAlerts(ctx, "circuit_fraud_detector", 10)
	require.NoError(t, err)
	require.Len(t, alerts, 1)

	saved := alerts[0]
	assert.NotEmpty(t, saved.ID)
	assert.Equal(t, "circuit_fraud_detector", saved.CircuitID)
	assert.Equal(t, "flag_high_risk", saved.NodeName)
	assert.Equal(t, "emergency_webhook", saved.SinkName)
	assert.Equal(t, "risk_score > 85", saved.Condition)

	// Verify PII sanitization
	email, _ := saved.Payload["user_email"].(string)
	card, _ := saved.Payload["credit_card"].(string)
	pwd, _ := saved.Payload["password"].(string)
	assert.Contains(t, email, "****@example.com")
	assert.Contains(t, card, "****")
	assert.Equal(t, "[REDACTED]", pwd)
	assert.Equal(t, 1250.50, saved.Payload["amount"])

	// Verify stats
	stats, err := store.GetStats(ctx, "circuit_fraud_detector")
	require.NoError(t, err)
	assert.Equal(t, uint64(1), stats.TotalFired)
	assert.Equal(t, uint64(1), stats.BySink["emergency_webhook"])

	// Verify circuit list
	circuits, err := store.ListCircuitsWithAlerts(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"circuit_fraud_detector"}, circuits)
}

func TestHistoryStore_RecordFeedback(t *testing.T) {
	ctx := context.Background()
	backend := cache.NewMemoryCache()
	defer backend.Close()

	cfg := history.DefaultConfig()
	cfg.Async = false
	store, err := history.New(backend, cfg)
	require.NoError(t, err)
	defer store.Close()

	rec := history.AlertRecord{
		CircuitID: "circuit_log_monitor",
		NodeName:  "alert_fatal_error",
		SinkName:  "pager_oncall",
		Condition: "status_code >= 500",
		Payload: map[string]any{
			"status_code": 503,
			"service":     "inventory",
		},
	}
	require.NoError(t, store.Record(ctx, rec))

	alerts, err := store.GetAlerts(ctx, "circuit_log_monitor", 1)
	require.NoError(t, err)
	require.Len(t, alerts, 1)
	alertID := alerts[0].ID

	// Attach operator feedback
	feedback := history.Feedback{
		Classification: history.FeedbackFalsePositive,
		Reason:         "Transient 503 during canary deployment rollover",
		Author:         "sre-oncall",
	}
	err = store.RecordFeedback(ctx, "circuit_log_monitor", alertID, feedback)
	require.NoError(t, err)

	// Fetch updated record and verify feedback
	updated, err := store.GetAlert(ctx, "circuit_log_monitor", alertID)
	require.NoError(t, err)
	require.NotNil(t, updated.Feedback)
	assert.Equal(t, history.FeedbackFalsePositive, updated.Feedback.Classification)
	assert.Equal(t, "Transient 503 during canary deployment rollover", updated.Feedback.Reason)
	assert.Equal(t, "sre-oncall", updated.Feedback.Author)
	assert.False(t, updated.Feedback.UpdatedAt.IsZero())
}

func TestHistoryStore_BoundedCapacityEviction(t *testing.T) {
	ctx := context.Background()
	backend := cache.NewMemoryCache()
	defer backend.Close()

	cfg := history.DefaultConfig()
	cfg.Async = false
	cfg.MaxRecordsPerCircuit = 5 // Cap at 5
	store, err := history.New(backend, cfg)
	require.NoError(t, err)
	defer store.Close()

	// Ingest 8 alerts with sequential timestamps
	for i := 0; i < 8; i++ {
		rec := history.AlertRecord{
			ID:        fmt.Sprintf("alert_%02d", i),
			Timestamp: time.Now().UTC().Add(time.Duration(i) * time.Second),
			CircuitID: "circuit_capacity_test",
			NodeName:  "node_alert",
			SinkName:  "test_sink",
			Payload:   map[string]any{"index": i},
		}
		require.NoError(t, store.Record(ctx, rec))
	}

	// Verify only 5 records remain
	alerts, err := store.GetAlerts(ctx, "circuit_capacity_test", 10)
	require.NoError(t, err)
	assert.Len(t, alerts, 5)

	// Oldest alerts (00, 01, 02) should have been evicted; newest (07 down to 03) should remain
	assert.Equal(t, "alert_07", alerts[0].ID)
	assert.Equal(t, "alert_03", alerts[4].ID)
}

func TestHistoryStore_AsyncQueueAndFlush(t *testing.T) {
	ctx := context.Background()
	backend := cache.NewMemoryCache()
	defer backend.Close()

	cfg := history.DefaultConfig()
	cfg.Async = true
	cfg.QueueSize = 1024
	store, err := history.New(backend, cfg)
	require.NoError(t, err)
	defer store.Close()

	for i := 0; i < 50; i++ {
		rec := history.AlertRecord{
			CircuitID: "circuit_async_test",
			NodeName:  "node_worker",
			SinkName:  "sink_stream",
			Payload:   map[string]any{"seq": i},
		}
		require.NoError(t, store.Record(ctx, rec))
	}

	// Flush and verify all 50 were processed
	require.NoError(t, store.Flush())

	stats, err := store.GetStats(ctx, "circuit_async_test")
	require.NoError(t, err)
	assert.Equal(t, uint64(50), stats.TotalFired)
}
