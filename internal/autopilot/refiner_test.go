package autopilot_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cuprite-io/assay"
	"github.com/cuprite-io/flux/autopilot"
	internalauto "github.com/cuprite-io/flux/internal/autopilot"
	"github.com/cuprite-io/flux/internal/cache"
	"github.com/cuprite-io/flux/internal/cluster"
	"github.com/cuprite-io/flux/internal/compiler"
	"github.com/cuprite-io/flux/internal/engine"
	"github.com/cuprite-io/flux/internal/history"
	"github.com/cuprite-io/flux/internal/pool"
	"github.com/cuprite-io/flux/internal/registry"
	"github.com/cuprite-io/flux/types"
)

type mockHistoryReader struct {
	alerts []history.AlertRecord
}

func (m *mockHistoryReader) GetAlerts(ctx context.Context, circuitID string, limit int) ([]history.AlertRecord, error) {
	return m.alerts, nil
}

func TestRefiner_UserFeedback_RefinesAndHotSwaps(t *testing.T) {
	reg := registry.New(nil)

	// 1. Initial Active Circuit (triggers alert when status_code >= 500)
	initialRoot := types.NewNode("http_alert").
		Step(&types.StepDefinition{
			Type:      types.StepSink,
			SinkName:  "slack_ops",
			Condition: "payload.status_code >= 500",
		})
	initialCircuit := types.NewCircuit("web_monitor").
		WithTags("stream:web").
		WithRoot(initialRoot)
	initialCircuit.Version = 1

	if err := reg.Put(context.Background(), initialCircuit); err != nil {
		t.Fatalf("failed to register initial circuit: %v", err)
	}

	// 2. Mock AI Provider that refines circuit by adding path != '/health' check
	var capturedPrompt string
	mockAI := autopilot.ProviderFunc(func(ctx context.Context, messages []autopilot.Message) (string, error) {
		for _, m := range messages {
			if m.Role == autopilot.RoleUser {
				capturedPrompt = m.Content
			}
		}
		// Return refined circuit DAG
		return `{
			"id": "web_monitor",
			"tags": ["stream:web"],
			"root": {
				"name": "http_alert_refined",
				"condition": "payload.status_code >= 500 && payload.path != '/health'",
				"steps": [
					{
						"type": "sink",
						"sink": "slack_ops",
						"condition": "payload.status_code >= 500 && payload.path != '/health'"
					}
				]
			}
		}`, nil
	})

	hist := &mockHistoryReader{
		alerts: []history.AlertRecord{
			{
				ID:        "al-101",
				CircuitID: "web_monitor",
				NodeName:  "http_alert",
				SinkName:  "slack_ops",
				Condition: "payload.status_code >= 500",
				Payload: map[string]any{
					"status_code": 503,
					"path":        "/health",
				},
				Feedback: &history.Feedback{
					Classification: "false_positive",
					Reason:         "health check probe returns 503 during restart, please exclude /health",
					Author:         "devops-alice",
					UpdatedAt:      time.Now(),
				},
			},
		},
	}

	refiner := internalauto.NewRefiner(mockAI, reg, hist)

	// Trigger refinement on user feedback
	res, err := refiner.RefineOnFeedback(context.Background(), "web_monitor", hist.alerts[0])
	if err != nil {
		t.Fatalf("RefineOnFeedback failed: %v", err)
	}

	if res.Trigger != internalauto.TriggerUserFeedback {
		t.Errorf("expected TriggerUserFeedback, got %s", res.Trigger)
	}
	if res.RefinedCircuit.Version != 2 {
		t.Errorf("expected version 2, got %d", res.RefinedCircuit.Version)
	}

	// Verify feedback was included in the prompt
	if !strings.Contains(capturedPrompt, "false_positive") {
		t.Errorf("expected prompt to contain false_positive, got:\n%s", capturedPrompt)
	}
	if !strings.Contains(capturedPrompt, "health check probe") {
		t.Errorf("expected prompt to contain user feedback reason, got:\n%s", capturedPrompt)
	}

	// Verify atomically hot-swapped into Registry
	activeInReg, err := reg.Get(context.Background(), "web_monitor")
	if err != nil {
		t.Fatalf("failed to get circuit from registry: %v", err)
	}
	if activeInReg.Version != 2 {
		t.Errorf("expected circuit version 2 in registry, got %d", activeInReg.Version)
	}
	if activeInReg.Root.Name != "http_alert_refined" {
		t.Errorf("expected root name http_alert_refined, got %s", activeInReg.Root.Name)
	}
}

func TestRefiner_SchemaDrift_Tuning(t *testing.T) {
	reg := registry.New(nil)

	circuit := types.NewCircuit("drift_detector").
		WithTags("stream:events").
		WithRoot(types.NewNode("root").Step(&types.StepDefinition{
			Type:     types.StepReturn,
			ReturnMap: map[string]any{"ok": true},
		}))

	_ = reg.Put(context.Background(), circuit)

	var promptCaptured string
	mockAI := autopilot.ProviderFunc(func(ctx context.Context, messages []autopilot.Message) (string, error) {
		for _, m := range messages {
			if m.Role == autopilot.RoleUser {
				promptCaptured = m.Content
			}
		}
		return `{
			"id": "drift_detector",
			"tags": ["stream:events"],
			"root": {
				"name": "tuned_for_drift",
				"steps": [{"type": "return", "data": {"ok": true, "adapted": true}}]
			}
		}`, nil
	})

	refiner := internalauto.NewRefiner(mockAI, reg, nil)

	newSchema := &assay.SchemaNode{
		Name: "root",
		Type: "object",
		Children: map[string]*assay.SchemaNode{
			"priority_level": {
				Name: "priority_level",
				Type: "string",
			},
		},
	}

	res, err := refiner.RefineOnSchemaDrift(context.Background(), "drift_detector", "stream:events", newSchema)
	if err != nil {
		t.Fatalf("RefineOnSchemaDrift failed: %v", err)
	}

	if res.Trigger != internalauto.TriggerSchemaDrift {
		t.Errorf("expected TriggerSchemaDrift, got %s", res.Trigger)
	}
	if !strings.Contains(promptCaptured, "priority_level") {
		t.Errorf("expected prompt to contain new schema field priority_level")
	}
}

func TestRefiner_FiringRateAnomaly_Tuning(t *testing.T) {
	reg := registry.New(nil)

	circuit := types.NewCircuit("storm_circuit").
		WithTags("stream:logs").
		WithRoot(types.NewNode("root").Step(&types.StepDefinition{
			Type:      types.StepSink,
			SinkName:  "slack",
			Condition: "payload.val > 0",
		}))

	_ = reg.Put(context.Background(), circuit)

	var promptCaptured string
	mockAI := autopilot.ProviderFunc(func(ctx context.Context, messages []autopilot.Message) (string, error) {
		for _, m := range messages {
			if m.Role == autopilot.RoleUser {
				promptCaptured = m.Content
			}
		}
		return `{
			"id": "storm_circuit",
			"tags": ["stream:logs"],
			"root": {
				"name": "tightened_rule",
				"steps": [{"type": "sink", "sink": "slack", "condition": "payload.val > 100"}]
			}
		}`, nil
	})

	refiner := internalauto.NewRefiner(mockAI, reg, nil)

	res, err := refiner.RefineOnFiringAnomaly(context.Background(), "storm_circuit", 0.15, "firing rate spiked to 15% alert storm")
	if err != nil {
		t.Fatalf("RefineOnFiringAnomaly failed: %v", err)
	}

	if res.Trigger != internalauto.TriggerFiringRate {
		t.Errorf("expected TriggerFiringRate, got %s", res.Trigger)
	}
	if !strings.Contains(promptCaptured, "15.00%") {
		t.Errorf("expected prompt to contain anomaly rate 15.00%%%%")
	}
}

func TestRefiner_ShadowVerification_DuringRefinement(t *testing.T) {
	reg := registry.New(nil)
	comp, _ := compiler.NewCompiler(nil)
	exec := engine.NewExecutor(comp, pool.GetDefaultPool(), nil)

	circuit := types.NewCircuit("candidate_rule").
		WithTags("stream:metrics").
		WithRoot(types.NewNode("root").Step(&types.StepDefinition{
			Type:      types.StepSink,
			SinkName:  "pager",
			Condition: "payload.cpu > 50",
		}))

	_ = reg.Put(context.Background(), circuit)

	// Refined circuit that fires only on cpu > 90
	mockAI := autopilot.ProviderFunc(func(ctx context.Context, messages []autopilot.Message) (string, error) {
		return `{
			"id": "candidate_rule",
			"tags": ["stream:metrics"],
			"root": {
				"name": "root",
				"steps": [{"type": "sink", "sink": "pager", "condition": "payload.cpu > 90"}]
			}
		}`, nil
	})

	stager := internalauto.NewStager(reg,
		internalauto.WithEvaluationEvents(50),
		internalauto.WithMinTargetFiringRate(0.01),
		internalauto.WithMaxTargetFiringRate(0.05),
	)

	refiner := internalauto.NewRefiner(mockAI, reg, nil,
		internalauto.WithRefinerStager(stager),
		internalauto.WithRefinerExecutor(exec),
	)

	// Generate 50 sample telemetry events: 1 firing (cpu 95%) -> 1/50 = 2% safe rate
	events := make([]any, 50)
	for i := 0; i < 49; i++ {
		events[i] = map[string]any{"cpu": 40}
	}
	events[49] = map[string]any{"cpu": 95, "status": 500}

	req := internalauto.RefinementRequest{
		CircuitID:       "candidate_rule",
		Trigger:         internalauto.TriggerScheduled,
		TelemetryEvents: events,
	}

	res, err := refiner.Refine(context.Background(), req)
	if err != nil {
		t.Fatalf("Refine failed: %v", err)
	}

	if res.StagingResult == nil {
		t.Fatalf("expected StagingResult from shadow batch evaluation")
	}
	if res.StagingResult.Status != internalauto.StatusPromoted {
		t.Errorf("expected StatusPromoted, got %s", res.StagingResult.Status)
	}
}

func TestRefiner_LeaderGating_BackgroundLoop(t *testing.T) {
	backend := cache.NewMemoryCache()
	reg := registry.New(backend)

	var calls int64
	mockAI := autopilot.ProviderFunc(func(ctx context.Context, messages []autopilot.Message) (string, error) {
		atomic.AddInt64(&calls, 1)
		return `{
			"id": "leader_rule",
			"tags": ["stream:test"],
			"root": {"name": "n", "steps": []}
		}`, nil
	})

	circuit := types.NewCircuit("leader_rule").
		WithTags("stream:test").
		WithRoot(types.NewNode("n"))
	_ = reg.Put(context.Background(), circuit)

	// Follower elector (not leader)
	followerElector := cluster.NewLeaderElector(backend,
		cluster.WithNodeID("node-2-follower"),
		cluster.WithHeartbeatTTL(10*time.Second),
	)

	// Leader elector
	leaderElector := cluster.NewLeaderElector(backend,
		cluster.WithNodeID("node-1-leader"),
		cluster.WithHeartbeatTTL(10*time.Second),
	)
	_, _ = leaderElector.Evaluate(context.Background())

	// Refiner with follower elector: should NOT trigger background refinement
	refinerFollower := internalauto.NewRefiner(mockAI, reg, nil,
		internalauto.WithRefinementInterval(50*time.Millisecond),
		internalauto.WithRefinerElector(followerElector),
	)
	refinerFollower.TrackCircuit("leader_rule", "monitor", []string{"stream:test"})

	ctxFollower, cancelFollower := context.WithCancel(context.Background())
	_ = refinerFollower.Start(ctxFollower)
	time.Sleep(120 * time.Millisecond)
	_ = refinerFollower.Stop(context.Background())
	cancelFollower()

	if atomic.LoadInt64(&calls) != 0 {
		t.Errorf("follower node should NOT execute scheduled refinements, got %d calls", calls)
	}

	// Refiner with leader elector: SHOULD trigger background refinement
	refinerLeader := internalauto.NewRefiner(mockAI, reg, nil,
		internalauto.WithRefinementInterval(50*time.Millisecond),
		internalauto.WithRefinerElector(leaderElector),
	)
	refinerLeader.TrackCircuit("leader_rule", "monitor", []string{"stream:test"})

	ctxLeader, cancelLeader := context.WithCancel(context.Background())
	_ = refinerLeader.Start(ctxLeader)
	time.Sleep(120 * time.Millisecond)
	_ = refinerLeader.Stop(context.Background())
	cancelLeader()

	if atomic.LoadInt64(&calls) == 0 {
		t.Errorf("leader node SHOULD execute scheduled refinements, got 0 calls")
	}
}
