package engine_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/cuprite-io/flux/internal/compiler"
	"github.com/cuprite-io/flux/internal/engine"
	"github.com/cuprite-io/flux/internal/pool"
	"github.com/cuprite-io/flux/internal/state"
	"github.com/cuprite-io/flux/types"
)

func TestEngine_ShadowMode_SuppressesExternalSink(t *testing.T) {
	comp, err := compiler.NewCompiler(nil)
	if err != nil {
		t.Fatalf("failed to create compiler: %v", err)
	}

	var liveSinkCalled int64
	liveSink := func(name string, payload any) error {
		atomic.AddInt64(&liveSinkCalled, 1)
		return nil
	}

	var shadowHookCalled int64
	var capturedSinkName string
	shadowHook := func(ctx context.Context, circuitID, nodeName, sinkName, condition string, payload any) {
		atomic.AddInt64(&shadowHookCalled, 1)
		capturedSinkName = sinkName
	}

	exec := engine.NewExecutor(comp, pool.GetDefaultPool(), liveSink, engine.WithShadowHook(shadowHook))

	root := types.NewNode("shadow_alert_node").
		Step(&types.StepDefinition{
			Type:   types.StepVolt,
			Script: `set('enriched', payload.val * 2)`,
		}).
		Step(&types.StepDefinition{
			Type:      types.StepSink,
			SinkName:  "pagerduty_critical",
			Condition: "state.enriched > 50",
		}).
		Step(&types.StepDefinition{
			Type:      types.StepReturn,
			ReturnMap: map[string]any{"enriched_val": "$enriched"},
		})

	circuit := types.NewCircuit("candidate_fraud_detector").WithRoot(root)

	// Case 1: Normal mode (ShadowMode = false)
	sctxNormal := state.NewContext(context.Background(), map[string]any{"val": 60})
	resNormal, err := exec.ExecuteCircuit(context.Background(), circuit, sctxNormal)
	if err != nil {
		t.Fatalf("execute normal circuit failed: %v", err)
	}
	if !resNormal.Passed {
		t.Errorf("expected circuit to pass, errors: %v", resNormal.Errors)
	}
	if atomic.LoadInt64(&liveSinkCalled) != 1 {
		t.Errorf("expected live sink to be called 1 time, got %d", liveSinkCalled)
	}
	if atomic.LoadInt64(&shadowHookCalled) != 0 {
		t.Errorf("expected shadow hook NOT to be called in normal mode, got %d", shadowHookCalled)
	}
	if resNormal.ShadowFirings != 0 {
		t.Errorf("expected 0 shadow firings, got %d", resNormal.ShadowFirings)
	}
	if resNormal.ReturnedData["enriched_val"] != int64(120) {
		t.Errorf("expected enriched_val=120, got %v", resNormal.ReturnedData["enriched_val"])
	}

	// Reset counters
	atomic.StoreInt64(&liveSinkCalled, 0)
	atomic.StoreInt64(&shadowHookCalled, 0)

	// Case 2: Shadow mode (sctx.SetShadowMode(true))
	sctxShadow := state.NewContext(context.Background(), map[string]any{"val": 60})
	sctxShadow.SetShadowMode(true)

	resShadow, err := exec.ExecuteCircuit(context.Background(), circuit, sctxShadow)
	if err != nil {
		t.Fatalf("execute shadow circuit failed: %v", err)
	}
	if !resShadow.Passed {
		t.Errorf("expected circuit to pass in shadow mode")
	}
	// Verify external network sink dispatch was SUPPRESSED!
	if atomic.LoadInt64(&liveSinkCalled) != 0 {
		t.Errorf("expected live sink to be SUPPRESSED in shadow mode, got %d calls", liveSinkCalled)
	}
	// Verify shadow hook was invoked
	if atomic.LoadInt64(&shadowHookCalled) != 1 {
		t.Errorf("expected shadow hook to be called 1 time, got %d", shadowHookCalled)
	}
	if capturedSinkName != "pagerduty_critical" {
		t.Errorf("expected captured sink pagerduty_critical, got %s", capturedSinkName)
	}
	// Verify shadow firings recorded
	if resShadow.ShadowFirings != 1 {
		t.Errorf("expected 1 shadow firing, got %d", resShadow.ShadowFirings)
	}
	if sctxShadow.ShadowFirings() != 1 {
		t.Errorf("expected sctx.ShadowFirings() == 1, got %d", sctxShadow.ShadowFirings())
	}
	// Verify calculations and return data executed completely
	if resShadow.ReturnedData["enriched_val"] != int64(120) {
		t.Errorf("expected enriched_val=120 in shadow mode, got %v", resShadow.ReturnedData["enriched_val"])
	}
}

func TestEngine_ShadowMode_TagOrIDDetection(t *testing.T) {
	comp, err := compiler.NewCompiler(nil)
	if err != nil {
		t.Fatalf("failed to create compiler: %v", err)
	}

	var liveSinkCalled int64
	liveSink := func(name string, payload any) error {
		atomic.AddInt64(&liveSinkCalled, 1)
		return nil
	}

	var alertHookCalled int64
	alertHook := func(ctx context.Context, circuitID, nodeName, sinkName, condition string, payload any) {
		atomic.AddInt64(&alertHookCalled, 1)
	}

	exec := engine.NewExecutor(comp, pool.GetDefaultPool(), liveSink, engine.WithAlertHook(alertHook))

	root := types.NewNode("root").
		Step(&types.StepDefinition{
			Type:      types.StepSink,
			SinkName:  "slack_channel",
			Condition: "payload.score > 80",
		})

	// Circuit with shadow: prefix ID
	shadowCircuit1 := types.NewCircuit("shadow:candidate_rule_1").WithRoot(root)
	sctx1 := state.NewContext(context.Background(), map[string]any{"score": 95})

	res1, err := exec.ExecuteCircuit(context.Background(), shadowCircuit1, sctx1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if atomic.LoadInt64(&liveSinkCalled) != 0 {
		t.Errorf("expected live sink to be suppressed for shadow: circuit ID, got %d", liveSinkCalled)
	}
	if res1.ShadowFirings != 1 {
		t.Errorf("expected 1 shadow firing, got %d", res1.ShadowFirings)
	}
	if atomic.LoadInt64(&alertHookCalled) != 1 {
		t.Errorf("expected fallback alertHook to be called when shadowHook is nil, got %d", alertHookCalled)
	}

	// Reset counters
	atomic.StoreInt64(&liveSinkCalled, 0)
	atomic.StoreInt64(&alertHookCalled, 0)

	// Circuit with shadow tag
	shadowCircuit2 := types.NewCircuit("rule_2").WithTags("shadow:test", "logs").WithRoot(root)
	sctx2 := state.NewContext(context.Background(), map[string]any{"score": 95})

	res2, err := exec.ExecuteCircuit(context.Background(), shadowCircuit2, sctx2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if atomic.LoadInt64(&liveSinkCalled) != 0 {
		t.Errorf("expected live sink to be suppressed for circuit with shadow tag, got %d", liveSinkCalled)
	}
	if res2.ShadowFirings != 1 {
		t.Errorf("expected 1 shadow firing, got %d", res2.ShadowFirings)
	}
	if atomic.LoadInt64(&alertHookCalled) != 1 {
		t.Errorf("expected alertHook to be called, got %d", alertHookCalled)
	}
}

func TestEngine_ShadowMode_ParallelBranches(t *testing.T) {
	comp, err := compiler.NewCompiler(nil)
	if err != nil {
		t.Fatalf("failed to create compiler: %v", err)
	}

	var liveSinkCalled int64
	liveSink := func(name string, payload any) error {
		atomic.AddInt64(&liveSinkCalled, 1)
		return nil
	}

	exec := engine.NewExecutor(comp, pool.GetDefaultPool(), liveSink)

	// Root dispatches to Child A and Child B in parallel
	childA := types.NewNode("child_a").
		Step(&types.StepDefinition{
			Type:      types.StepSink,
			SinkName:  "sink_a",
			Condition: "payload.amount > 100",
		})
	childB := types.NewNode("child_b").
		Step(&types.StepDefinition{
			Type:      types.StepSink,
			SinkName:  "sink_b",
			Condition: "payload.amount > 50",
		})

	root := types.NewNode("root").AddChildren(childA, childB)
	circuit := types.NewCircuit("parallel_shadow").WithRoot(root)

	sctx := state.NewContext(context.Background(), map[string]any{"amount": 200})
	sctx.SetShadowMode(true)

	res, err := exec.ExecuteCircuit(context.Background(), circuit, sctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if atomic.LoadInt64(&liveSinkCalled) != 0 {
		t.Errorf("expected 0 live sink calls, got %d", liveSinkCalled)
	}
	// Both branches satisfied conditions -> 2 shadow firings aggregated across goroutines
	if res.ShadowFirings != 2 {
		t.Errorf("expected 2 aggregated shadow firings, got %d", res.ShadowFirings)
	}
	if sctx.ShadowFirings() != 2 {
		t.Errorf("expected sctx.ShadowFirings() == 2, got %d", sctx.ShadowFirings())
	}
}
