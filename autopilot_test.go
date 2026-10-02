package flux_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cuprite-io/flux"
	"github.com/cuprite-io/flux/autopilot"
	"github.com/cuprite-io/flux/internal/cache"
	"github.com/cuprite-io/flux/internal/sink"
)

type mockAIProvider struct {
	response string
	err      error
	calls    int64
}

func (m *mockAIProvider) Generate(ctx context.Context, messages []autopilot.Message) (string, error) {
	atomic.AddInt64(&m.calls, 1)
	return m.response, m.err
}

const sampleCircuitJSON = `{
  "id": "log_error_detector",
  "tags": ["stream:logs"],
  "root": {
    "name": "check_log_level",
    "condition": "payload.level == 'error'",
    "steps": [
      {
        "type": "sink",
        "sink": "mock_slack",
        "payload": "payload"
      }
    ]
  }
}`

func TestAutopilot_Validation(t *testing.T) {
	eng, err := flux.New()
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	defer eng.Close()

	ctx := context.Background()
	provider := &mockAIProvider{response: sampleCircuitJSON}

	// 1. Empty prompt
	_, err = eng.Autopilot(ctx, "", provider, []string{"stream:logs"})
	if err == nil {
		t.Errorf("expected error for empty prompt")
	}

	// 2. Nil provider
	_, err = eng.Autopilot(ctx, "monitor errors", nil, []string{"stream:logs"})
	if err == nil {
		t.Errorf("expected error for nil provider")
	}

	// 3. Empty tags
	_, err = eng.Autopilot(ctx, "monitor errors", provider, nil)
	if err == nil {
		t.Errorf("expected error for empty tags")
	}
}

func TestAutopilot_BatchEvaluation_PromotedToLive(t *testing.T) {
	memCache := cache.NewMemoryCache()
	eng, err := flux.New(
		flux.WithCache(memCache),
	)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	defer eng.Close()

	var sinkCalls int64
	eng.RegisterSink("mock_slack", sink.FuncSink(func(ctx context.Context, payload any) error {
		atomic.AddInt64(&sinkCalls, 1)
		return nil
	}))

	provider := &mockAIProvider{response: sampleCircuitJSON}

	// Prepare evaluation batch: 1 error event, 99 info events -> 1.0% firing rate
	batch := make([]any, 100)
	batch[0] = map[string]any{"level": "error", "message": "database connection timeout"}
	for i := 1; i < 100; i++ {
		batch[i] = map[string]any{"level": "info", "message": fmt.Sprintf("request %d ok", i)}
	}

	ctx := context.Background()
	handle, err := eng.Autopilot(
		ctx,
		"alert on critical error logs",
		provider,
		[]string{"stream:logs"},
		flux.WithSampleThreshold(0), // bypass cold-start sampling
		flux.WithEvaluationBatch(batch),
		flux.WithMinTargetFiringRate(0.005), // 0.5%
		flux.WithMaxTargetFiringRate(0.05),  // 5.0%
	)
	if err != nil {
		t.Fatalf("failed to launch autopilot: %v", err)
	}
	defer handle.Stop(context.Background())

	// Wait until promoted to Live
	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := handle.WaitUntil(waitCtx, flux.StatusLive); err != nil {
		t.Fatalf("failed waiting for StatusLive: %v (lastErr=%v)", err, handle.Error())
	}

	if handle.Status() != flux.StatusLive {
		t.Fatalf("expected StatusLive, got %v", handle.Status())
	}

	if len(handle.ActiveCircuits()) != 1 || handle.ActiveCircuits()[0] != "log_error_detector" {
		t.Errorf("expected active circuit [log_error_detector], got %v", handle.ActiveCircuits())
	}

	circuit := handle.CurrentCircuit()
	if circuit == nil || circuit.ID != "log_error_detector" {
		t.Errorf("expected circuit log_error_detector, got %v", circuit)
	}

	stgRes := handle.StagingResult()
	if stgRes == nil || !stgRes.Promoted {
		t.Errorf("expected staging result to be promoted, got %v", stgRes)
	}

	// Now send a live event through Spark on stream:logs -> circuit should fire to sink
	res, err := eng.Spark(context.Background(), map[string]any{"level": "error", "message": "fatal payment fail"}, "stream:logs")
	if err != nil {
		t.Fatalf("Spark failed: %v", err)
	}
	if !res.Passed {
		t.Errorf("expected Spark to pass")
	}

	// Give async sink worker a moment
	time.Sleep(50 * time.Millisecond)
	if atomic.LoadInt64(&sinkCalls) == 0 {
		t.Errorf("expected promoted circuit to dispatch to mock_slack sink")
	}
}

func TestAutopilot_LiveStreaming_ColdStartAndAutoPromote(t *testing.T) {
	memCache := cache.NewMemoryCache()
	eng, err := flux.New(
		flux.WithCache(memCache),
	)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	defer eng.Close()

	eng.RegisterSink("mock_slack", sink.FuncSink(func(ctx context.Context, payload any) error {
		return nil
	}))

	provider := &mockAIProvider{response: sampleCircuitJSON}

	ctx := context.Background()
	handle, err := eng.Autopilot(
		ctx,
		"alert on critical error logs",
		provider,
		[]string{"stream:logs"},
		flux.WithSampleThreshold(5),        // collect 5 samples
		flux.WithShadowEvaluationEvents(5), // 5 shadow events
		flux.WithMinTargetFiringRate(0.10), // 10%
		flux.WithMaxTargetFiringRate(0.50), // 50%
		flux.WithAlertStormThreshold(0.60), // 60%
	)
	if err != nil {
		t.Fatalf("failed to launch autopilot: %v", err)
	}
	defer handle.Stop(context.Background())

	// Initially in StatusSampling
	if handle.Status() != flux.StatusSampling {
		t.Fatalf("expected StatusSampling initially, got %v", handle.Status())
	}

	// Stream 5 events to satisfy cold-start sampling threshold
	for i := 0; i < 5; i++ {
		_, _ = eng.Spark(context.Background(), map[string]any{"level": "info", "step": i}, "stream:logs")
		time.Sleep(5 * time.Millisecond)
	}

	// Wait for transition to StatusShadowing (or StatusLive if shadow events arrive fast)
	waitShadow, cancelShadow := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelShadow()
	_ = handle.WaitUntil(waitShadow, flux.StatusShadowing)

	// Stream 5 shadow observation events: 1 error (20%), 4 info
	_, _ = eng.Spark(context.Background(), map[string]any{"level": "error", "step": "err"}, "stream:logs")
	for i := 0; i < 4; i++ {
		_, _ = eng.Spark(context.Background(), map[string]any{"level": "info", "step": i}, "stream:logs")
		time.Sleep(5 * time.Millisecond)
	}

	// Should now be promoted to Live!
	waitLive, cancelLive := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelLive()
	if err := handle.WaitUntil(waitLive, flux.StatusLive); err != nil {
		t.Fatalf("failed waiting for StatusLive: %v (lastErr=%v)", err, handle.Error())
	}

	if handle.Status() != flux.StatusLive {
		t.Errorf("expected StatusLive after shadow observation, got %v", handle.Status())
	}
}

func TestAutopilot_AlertStorm_Rejection(t *testing.T) {
	memCache := cache.NewMemoryCache()
	eng, err := flux.New(
		flux.WithCache(memCache),
	)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	defer eng.Close()

	eng.RegisterSink("mock_slack", sink.FuncSink(func(ctx context.Context, payload any) error {
		return nil
	}))

	provider := &mockAIProvider{response: sampleCircuitJSON}

	// Prepare an alert storm: 60 out of 100 events trigger errors (60% firing rate)
	batch := make([]any, 100)
	for i := 0; i < 60; i++ {
		batch[i] = map[string]any{"level": "error", "msg": "fail"}
	}
	for i := 60; i < 100; i++ {
		batch[i] = map[string]any{"level": "info", "msg": "ok"}
	}

	ctx := context.Background()
	handle, err := eng.Autopilot(
		ctx,
		"alert on errors",
		provider,
		[]string{"stream:logs"},
		flux.WithSampleThreshold(0),
		flux.WithEvaluationBatch(batch),
		flux.WithAlertStormThreshold(0.05), // 5% max alert storm limit
	)
	if err != nil {
		t.Fatalf("failed to launch autopilot: %v", err)
	}
	defer handle.Stop(context.Background())

	// Wait until StatusFailed
	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := handle.WaitUntil(waitCtx, flux.StatusLive); err == nil {
		t.Fatalf("expected candidate circuit to be rejected, but it succeeded")
	}

	if handle.Status() != flux.StatusFailed {
		t.Errorf("expected StatusFailed, got %v", handle.Status())
	}

	if handle.Error() == nil {
		t.Errorf("expected handle.Error() to contain rejection reason")
	}
}

func TestAutopilot_RecordFeedback_And_ManualRefinement(t *testing.T) {
	memCache := cache.NewMemoryCache()
	eng, err := flux.New(
		flux.WithCache(memCache),
	)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	defer eng.Close()

	eng.RegisterSink("mock_slack", sink.FuncSink(func(ctx context.Context, payload any) error {
		return nil
	}))

	provider := &mockAIProvider{response: sampleCircuitJSON}

	batch := []any{
		map[string]any{"level": "error", "msg": "test error"},
	}
	for i := 0; i < 99; i++ {
		batch = append(batch, map[string]any{"level": "info", "msg": "ok"})
	}

	ctx := context.Background()
	handle, err := eng.Autopilot(
		ctx,
		"alert on logs",
		provider,
		[]string{"stream:logs"},
		flux.WithSampleThreshold(0),
		flux.WithEvaluationBatch(batch),
		flux.WithMinTargetFiringRate(0.001),
		flux.WithMaxTargetFiringRate(0.10),
	)
	if err != nil {
		t.Fatalf("failed to launch autopilot: %v", err)
	}
	defer handle.Stop(context.Background())

	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := handle.WaitUntil(waitCtx, flux.StatusLive); err != nil {
		t.Fatalf("failed waiting for StatusLive: %v", err)
	}

	// 1. Generate an alert via live Spark event
	_, err = eng.Spark(context.Background(), map[string]any{"level": "error", "msg": "test error event"}, "stream:logs")
	if err != nil {
		t.Fatalf("Spark failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)
	_ = eng.History().Flush()

	alerts, err := eng.GetAlertHistory(context.Background(), "log_error_detector", 10)
	if err != nil {
		t.Fatalf("GetAlertHistory error: %v", err)
	}
	if len(alerts) == 0 {
		t.Fatalf("expected at least 1 alert recorded in history")
	}

	// Record feedback on the recorded alert
	alertID := alerts[0].ID
	if err := handle.RecordFeedback(context.Background(), alertID, flux.FeedbackFalsePositive, "expected transient spike"); err != nil {
		t.Errorf("RecordFeedback error: %v", err)
	}

	_ = eng.History().Flush()
	updatedAlerts, _ := eng.GetAlertHistory(context.Background(), "log_error_detector", 10)
	if len(updatedAlerts) > 0 && updatedAlerts[0].Feedback != nil {
		if updatedAlerts[0].Feedback.Classification != flux.FeedbackFalsePositive {
			t.Errorf("expected feedback classification %s, got %s", flux.FeedbackFalsePositive, updatedAlerts[0].Feedback.Classification)
		}
	}

	// 2. Trigger manual refinement
	if err := handle.TriggerRefinement(context.Background(), flux.TriggerManual, "operator requested tuning"); err != nil {
		t.Errorf("TriggerRefinement error: %v", err)
	}

	// 3. Stop session
	if err := handle.Stop(context.Background()); err != nil {
		t.Errorf("Stop error: %v", err)
	}
	if handle.Status() != flux.StatusStopped {
		t.Errorf("expected StatusStopped, got %v", handle.Status())
	}
}

func TestAutopilot_SynthesisFailure(t *testing.T) {
	memCache := cache.NewMemoryCache()
	eng, err := flux.New(flux.WithCache(memCache))
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	defer eng.Close()

	// Provider returns broken unparseable output
	provider := &mockAIProvider{response: "this is completely invalid non-json content"}

	ctx := context.Background()
	handle, err := eng.Autopilot(
		ctx,
		"synthesize bad rule",
		provider,
		[]string{"stream:logs"},
		flux.WithSampleThreshold(0),
		flux.WithMaxReflectionRetries(1),
	)
	if err != nil {
		t.Fatalf("Autopilot launch failed: %v", err)
	}
	defer handle.Stop(context.Background())

	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := handle.WaitUntil(waitCtx, flux.StatusLive); err == nil {
		t.Errorf("expected session to fail synthesis, but it succeeded")
	}

	if handle.Status() != flux.StatusFailed {
		t.Errorf("expected StatusFailed, got %v", handle.Status())
	}
	if handle.Error() == nil {
		t.Errorf("expected non-nil handle.Error()")
	}
}
