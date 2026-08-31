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

func TestEngine_ParallelTreeExecution(t *testing.T) {
	comp, err := compiler.NewCompiler(nil)
	if err != nil {
		t.Fatalf("failed to create compiler: %v", err)
	}

	var sinkCount int64
	mockSink := func(name string, payload any) error {
		atomic.AddInt64(&sinkCount, 1)
		return nil
	}

	exec := engine.NewExecutor(comp, pool.GetDefaultPool(), mockSink)

	// Build hierarchical tree:
	// Root Node -> Child A (Parallel) & Child B (Parallel)
	root := types.NewNode("parse_event").
		Step(&types.StepDefinition{Type: types.StepVolt, Script: `amount >= 1000.0`})

	// Child A: High value check
	nodeA := types.NewNode("high_value_branch").
		WithCondition(`amount >= 1000.0`).
		Step(&types.StepDefinition{
			Type:     types.StepSink,
			SinkName: "slack_alerts",
		}).
		Step(&types.StepDefinition{
			Type:      types.StepReturn,
			ReturnMap: map[string]any{"status": "FLAGGED_HIGH_VALUE"},
		})

	// Child B: Fast path check
	nodeB := types.NewNode("standard_branch").
		WithCondition(`amount < 1000.0`).
		Step(&types.StepDefinition{
			Type:      types.StepReturn,
			ReturnMap: map[string]any{"status": "STANDARD"},
		})

	root.AddChildren(nodeA, nodeB)

	circuit := types.NewCircuit("payment_flow").
		WithRoot(root)

	// Run with high amount (4500.0) -> Node A should pass & trigger sink, Node B pruned
	sctx := state.NewContext(context.Background(), map[string]any{"amount": 4500.0})
	res, err := exec.ExecuteCircuit(context.Background(), circuit, sctx)
	if err != nil {
		t.Fatalf("execute circuit failed: %v", err)
	}

	if !res.Passed {
		t.Errorf("expected circuit to pass")
	}
	if atomic.LoadInt64(&sinkCount) != 1 {
		t.Errorf("expected 1 sink call, got %d", sinkCount)
	}
	if res.ReturnedData["status"] != "FLAGGED_HIGH_VALUE" {
		t.Errorf("expected status FLAGGED_HIGH_VALUE, got %v", res.ReturnedData["status"])
	}
}

func TestEngine_DeeplyNestedPruning(t *testing.T) {
	comp, _ := compiler.NewCompiler(nil)
	exec := engine.NewExecutor(comp, pool.GetDefaultPool(), nil)

	// Root -> Child (Condition: false) -> SubChild (should NEVER execute)
	var subChildExecuted bool
	subChild := types.NewNode("unreachable_subchild").
		Step(&types.StepDefinition{
			Type: types.StepVolt,
			Script: `amount > 0.0`,
		})

	child := types.NewNode("failing_condition_node").
		WithCondition(`amount > 100000.0`).
		AddChildren(subChild)

	root := types.NewNode("root").
		AddChildren(child)

	circuit := types.NewCircuit("nested_pruning").
		WithRoot(root)

	sctx := state.NewContext(context.Background(), map[string]any{"amount": 500.0})
	res, err := exec.ExecuteCircuit(context.Background(), circuit, sctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	if subChildExecuted {
		t.Errorf("subchild should have been pruned")
	}
	if !res.Passed {
		t.Errorf("circuit should pass cleanly with pruned nodes")
	}
}

func BenchmarkEngine_TreeExecution(b *testing.B) {
	comp, _ := compiler.NewCompiler(nil)
	exec := engine.NewExecutor(comp, pool.GetDefaultPool(), nil)

	root := types.NewNode("root").
		Step(&types.StepDefinition{Type: types.StepVolt, Script: `amount > 100.0`})

	childA := types.NewNode("child_a").
		WithCondition(`amount > 500.0`).
		Step(&types.StepDefinition{
			Type:      types.StepReturn,
			ReturnMap: map[string]any{"tier": "GOLD"},
		})

	root.AddChildren(childA)
	circuit := types.NewCircuit("bench_circuit").WithRoot(root)
	input := map[string]any{"amount": 1000.0}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sctx := state.NewContext(context.Background(), input)
		_, _ = exec.ExecuteCircuit(context.Background(), circuit, sctx)
	}
}
