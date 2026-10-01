package autopilot_test

import (
	"context"
	"sync"
	"testing"

	"github.com/cuprite-io/flux/internal/autopilot"
	"github.com/cuprite-io/flux/internal/compiler"
	"github.com/cuprite-io/flux/internal/engine"
	"github.com/cuprite-io/flux/internal/pool"
	"github.com/cuprite-io/flux/internal/registry"
	"github.com/cuprite-io/flux/types"
)

func buildCandidateCircuit(id, condition string) *types.Circuit {
	root := types.NewNode("filter").
		Step(&types.StepDefinition{
			Type:      types.StepSink,
			SinkName:  "slack_alerts",
			Condition: condition,
		})
	return types.NewCircuit(id).WithRoot(root)
}

func TestStager_SafeBounds_PromotedToLive(t *testing.T) {
	reg := registry.New(nil)
	stager := autopilot.NewStager(reg,
		autopilot.WithEvaluationEvents(200),
		autopilot.WithMinTargetFiringRate(0.001), // 0.1%
		autopilot.WithMaxTargetFiringRate(0.02),  // 2.0%
		autopilot.WithAutoPromote(true),
	)

	circuit := buildCandidateCircuit("rule_latency", "payload.latency_ms > 1000")
	targetTags := []string{"stream:http_logs"}

	sess, err := stager.StartSession(context.Background(), circuit, targetTags)
	if err != nil {
		t.Fatalf("failed to start session: %v", err)
	}

	// Verify circuit registered with shadow tag
	staged, err := reg.Get(context.Background(), circuit.ID)
	if err != nil {
		t.Fatalf("expected circuit in registry: %v", err)
	}
	var hasShadowTag bool
	for _, tag := range staged.Tags {
		if tag == "shadow:rule_latency" {
			hasShadowTag = true
		}
	}
	if !hasShadowTag {
		t.Errorf("expected staged circuit to have tag shadow:rule_latency, got %v", staged.Tags)
	}

	// Stream 200 events:
	// - 198 normal requests (latency 100ms) -> no firing
	// - 2 slow requests (latency 1500ms, status 500) -> 2 firings (1.0% rate)
	for i := 0; i < 198; i++ {
		done, res, errObs := sess.RecordObservation(context.Background(), map[string]any{
			"latency_ms": 100,
			"status":     200,
		}, false)
		if errObs != nil || done || res != nil {
			t.Fatalf("unexpected early completion at event %d", i)
		}
	}

	// 1st firing event (latency 1500ms, error log)
	done, res, errObs := sess.RecordObservation(context.Background(), map[string]any{
		"latency_ms": 1500,
		"status":     504,
	}, true)
	if errObs != nil || done {
		t.Fatalf("unexpected completion at event 199")
	}

	// 2nd firing event (reaches 200)
	done, res, errObs = sess.RecordObservation(context.Background(), map[string]any{
		"latency_ms": 2000,
		"status":     500,
	}, true)
	if errObs != nil {
		t.Fatalf("unexpected error: %v", errObs)
	}
	if !done || res == nil {
		t.Fatalf("expected session to finish at event 200")
	}

	if res.Status != autopilot.StatusPromoted {
		t.Errorf("expected StatusPromoted, got %s (reason: %s)", res.Status, res.RejectionReason)
	}
	if !res.Promoted {
		t.Errorf("expected res.Promoted == true")
	}
	if res.FiringEvents != 2 {
		t.Errorf("expected 2 firing events, got %d", res.FiringEvents)
	}
	if res.ObservedRate != 0.01 {
		t.Errorf("expected 1.0%% firing rate, got %f", res.ObservedRate)
	}

	// Verify circuit promoted to Live: shadow tag removed from Registry!
	promoted, err := reg.Get(context.Background(), circuit.ID)
	if err != nil {
		t.Fatalf("expected promoted circuit in registry: %v", err)
	}
	for _, tag := range promoted.Tags {
		if tag == "shadow:rule_latency" {
			t.Errorf("promoted circuit should NOT contain shadow tag, got %v", promoted.Tags)
		}
	}
	if len(promoted.Tags) != 1 || promoted.Tags[0] != "stream:http_logs" {
		t.Errorf("expected clean target tags [stream:http_logs], got %v", promoted.Tags)
	}
}

func TestStager_AlertStorm_RejectedAndDeleted(t *testing.T) {
	reg := registry.New(nil)
	stager := autopilot.NewStager(reg,
		autopilot.WithEvaluationEvents(100),
		autopilot.WithAlertStormThreshold(0.05), // 5%
	)

	circuit := buildCandidateCircuit("storm_rule", "payload.val > 0")
	targetTags := []string{"stream:events"}

	sess, err := stager.StartSession(context.Background(), circuit, targetTags)
	if err != nil {
		t.Fatalf("start session failed: %v", err)
	}

	// Stream 100 events where 10 fire (10% firing rate > 5% threshold)
	var finalRes *autopilot.StagingResult
	for i := 0; i < 100; i++ {
		fired := i < 10
		done, res, _ := sess.RecordObservation(context.Background(), map[string]any{"val": i}, fired)
		if done {
			finalRes = res
		}
	}

	if finalRes == nil {
		t.Fatalf("expected session to finish")
	}
	if finalRes.Status != autopilot.StatusAlertStorm {
		t.Errorf("expected StatusAlertStorm, got %s", finalRes.Status)
	}
	if finalRes.Promoted {
		t.Errorf("expected Promoted == false on alert storm")
	}

	// Verify rejected circuit was deleted from Registry!
	_, err = reg.Get(context.Background(), circuit.ID)
	if err == nil {
		t.Errorf("expected candidate circuit to be deleted from registry on rejection")
	}
}

func TestStager_ZeroFiringOnErrors_Rejected(t *testing.T) {
	reg := registry.New(nil)
	stager := autopilot.NewStager(reg,
		autopilot.WithEvaluationEvents(50),
	)

	circuit := buildCandidateCircuit("blind_rule", "payload.nonexistent == true")
	targetTags := []string{"stream:errors"}

	sess, err := stager.StartSession(context.Background(), circuit, targetTags)
	if err != nil {
		t.Fatalf("start session failed: %v", err)
	}

	// Stream 50 events where 5 are errors, but firing is 0
	var finalRes *autopilot.StagingResult
	for i := 0; i < 50; i++ {
		isErr := i%10 == 0
		var payload map[string]any
		if isErr {
			payload = map[string]any{"level": "error", "error": "fatal database timeout"}
		} else {
			payload = map[string]any{"level": "info", "msg": "ok"}
		}
		done, res, _ := sess.RecordObservation(context.Background(), payload, false)
		if done {
			finalRes = res
		}
	}

	if finalRes == nil {
		t.Fatalf("expected session to finish")
	}
	if finalRes.Status != autopilot.StatusZeroFiring {
		t.Errorf("expected StatusZeroFiring, got %s (reason: %s)", finalRes.Status, finalRes.RejectionReason)
	}
	if finalRes.Promoted {
		t.Errorf("expected Promoted == false")
	}
}

func TestStager_EvaluateBatch_WithExecutor(t *testing.T) {
	comp, err := compiler.NewCompiler(nil)
	if err != nil {
		t.Fatalf("failed to create compiler: %v", err)
	}
	exec := engine.NewExecutor(comp, pool.GetDefaultPool(), nil)

	reg := registry.New(nil)
	stager := autopilot.NewStager(reg,
		autopilot.WithEvaluationEvents(50),
		autopilot.WithMinTargetFiringRate(0.01),
		autopilot.WithMaxTargetFiringRate(0.05),
	)

	circuit := buildCandidateCircuit("cpu_monitor", "payload.cpu_percent > 90.0")
	targetTags := []string{"stream:metrics"}

	// Generate 50 events: 49 normal (cpu 20-50%), 1 spike (cpu 95%) -> 1/50 = 2% firing
	events := make([]any, 50)
	for i := 0; i < 49; i++ {
		events[i] = map[string]any{"cpu_percent": 35.0, "status": 200}
	}
	events[49] = map[string]any{"cpu_percent": 95.5, "status": 500}

	res, err := stager.EvaluateBatch(context.Background(), circuit, targetTags, exec, events)
	if err != nil {
		t.Fatalf("EvaluateBatch failed: %v", err)
	}

	if res.Status != autopilot.StatusPromoted {
		t.Errorf("expected StatusPromoted, got %s (reason: %s)", res.Status, res.RejectionReason)
	}
	if res.FiringEvents != 1 {
		t.Errorf("expected 1 firing event, got %d", res.FiringEvents)
	}
	if res.ObservedRate != 0.02 {
		t.Errorf("expected 0.02 observed rate, got %f", res.ObservedRate)
	}

	// Verify circuit promoted in registry
	live, err := reg.Get(context.Background(), circuit.ID)
	if err != nil {
		t.Fatalf("expected circuit in registry: %v", err)
	}
	if len(live.Tags) != 1 || live.Tags[0] != "stream:metrics" {
		t.Errorf("expected clean tags [stream:metrics], got %v", live.Tags)
	}
}

func TestStager_ConcurrentObservations(t *testing.T) {
	reg := registry.New(nil)
	stager := autopilot.NewStager(reg,
		autopilot.WithEvaluationEvents(100),
		autopilot.WithMinTargetFiringRate(0.01),
		autopilot.WithMaxTargetFiringRate(0.05),
	)

	circuit := buildCandidateCircuit("concurrency_rule", "payload.fired == true")
	targetTags := []string{"stream:traffic"}

	sess, err := stager.StartSession(context.Background(), circuit, targetTags)
	if err != nil {
		t.Fatalf("start session failed: %v", err)
	}

	var wg sync.WaitGroup
	workers := 10
	eventsPerWorker := 10

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < eventsPerWorker; i++ {
				fired := (workerID == 0 && i < 2) // 2 firings total across 100 events = 2%
				_, _, _ = sess.RecordObservation(context.Background(), map[string]any{"worker": workerID}, fired)
			}
		}(w)
	}

	wg.Wait()

	res, err := sess.Finish(context.Background())
	if err != nil {
		t.Fatalf("finish failed: %v", err)
	}

	if res.TotalEvents != 100 {
		t.Errorf("expected 100 total events, got %d", res.TotalEvents)
	}
	if res.FiringEvents != 2 {
		t.Errorf("expected 2 firings, got %d", res.FiringEvents)
	}
	if res.Status != autopilot.StatusPromoted {
		t.Errorf("expected StatusPromoted, got %s", res.Status)
	}
}

func TestIsErrorEvent(t *testing.T) {
	tests := []struct {
		name     string
		payload  any
		expected bool
	}{
		{"nil payload", nil, false},
		{"healthy 200 map", map[string]any{"status": 200, "msg": "ok"}, false},
		{"error 500 map", map[string]any{"status_code": 500}, true},
		{"error 503 map", map[string]any{"http_status": 503}, true},
		{"log level error", map[string]any{"level": "error"}, true},
		{"log level fatal", map[string]any{"severity": "fatal"}, true},
		{"error message present", map[string]any{"error": "connection reset by peer"}, true},
		{"empty error string", map[string]any{"error": ""}, false},
		{"json string with 500", `{"status_code": 502, "msg": "bad gateway"}`, true},
		{"json bytes with error", []byte(`{"level": "critical"}`), true},
		{"struct with error field", struct {
			StatusCode int
			Message    string
		}{StatusCode: 504, Message: "timeout"}, true},
		{"struct healthy", struct {
			Status  int
			Message string
		}{Status: 200, Message: "success"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := autopilot.IsErrorEvent(tt.payload)
			if got != tt.expected {
				t.Errorf("IsErrorEvent(%v) = %v; want %v", tt.name, got, tt.expected)
			}
		})
	}
}
