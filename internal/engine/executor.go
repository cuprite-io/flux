package engine

import (
	"context"
	"errors"
	"fmt"

	"github.com/cuprite-io/flux/internal/compiler"
	"github.com/cuprite-io/flux/internal/pool"
	"github.com/cuprite-io/flux/internal/state"
	"github.com/cuprite-io/flux/types"
)

var (
	ErrCircuitAborted = errors.New("flux engine: circuit execution aborted")
)

// SinkDispatcher defines the callback function to dispatch payloads to external sinks.
type SinkDispatcher func(sinkName string, payload any) error

// Executor coordinates the execution of Circuit trees.
type Executor struct {
	compiler   *compiler.Compiler
	workerPool *pool.WorkerPool
	sinkFn     SinkDispatcher
}

// NewExecutor creates a new Circuit tree Executor.
func NewExecutor(comp *compiler.Compiler, wp *pool.WorkerPool, sinkFn SinkDispatcher) *Executor {
	if wp == nil {
		wp = pool.GetDefaultPool()
	}
	return &Executor{
		compiler:   comp,
		workerPool: wp,
		sinkFn:     sinkFn,
	}
}

// ExecuteCircuit evaluates a single Circuit tree from root to leaves against the provided StateContext.
func (e *Executor) ExecuteCircuit(ctx context.Context, circuit *types.Circuit, sctx *state.Context) (*types.SparkResult, error) {
	if circuit == nil || circuit.Root == nil {
		return &types.SparkResult{
			OriginalInput:    sctx.OriginalInput,
			Passed:           true,
			ExecutedCircuits: nil,
		}, nil
	}

	// Start execution from root node at bit index 0
	e.executeNode(ctx, circuit.Root, sctx, 1<<0)

	if sctx.IsAborted() {
		return &types.SparkResult{
			OriginalInput:    sctx.OriginalInput,
			Passed:           false,
			ExecutedCircuits: []string{circuit.ID},
			Errors:           sctx.Errors(),
		}, ErrCircuitAborted
	}

	return &types.SparkResult{
		OriginalInput:    sctx.OriginalInput,
		ReturnedData:     sctx.ReturnData(),
		Passed:           len(sctx.Errors()) == 0,
		ExecutedCircuits: []string{circuit.ID},
		Errors:           sctx.Errors(),
	}, nil
}

// executeNode evaluates a node's condition, executes its sequential steps, and dispatches children.
func (e *Executor) executeNode(ctx context.Context, node *types.Node, sctx *state.Context, nodeBit uint64) {
	// 1. Bitmask Pruning Check: If this node is already marked dead, skip entire sub-tree
	if sctx.IsPruned(nodeBit) || sctx.IsAborted() {
		return
	}

	// 2. Evaluate Node Guard Condition
	if node.Condition != "" {
		passed, err := e.evalCondition(ctx, node.Condition, sctx)
		if err != nil {
			sctx.AddError(fmt.Errorf("node %s condition error: %w", node.Name, err))
			sctx.MarkPruned(nodeBit)
			return
		}
		if !passed {
			// Condition failed -> Mark this node bit as dead in O(1) time
			sctx.MarkPruned(nodeBit)
			return
		}
	}

	// 3. Execute Node's Sequential Steps
	for _, step := range node.Steps {
		if sctx.IsAborted() {
			return
		}
		if err := e.executeStep(ctx, step, sctx); err != nil {
			sctx.AddError(fmt.Errorf("node %s step error: %w", node.Name, err))
		}
	}

	// 4. Adaptive Dual-Path Dispatch for Children Nodes
	numChildren := len(node.Children)
	if numChildren == 0 || sctx.IsAborted() {
		return
	}

	if numChildren == 1 {
		// --- Inline Fast-Path (0 goroutine overhead, ~10ns) ---
		childBit := nodeBit << 1
		if childBit == 0 {
			childBit = 1
		}
		e.executeNode(ctx, node.Children[0], sctx, childBit)
		return
	}

	// --- Multi-Child Work-Stealing Parallel Dispatch ---
	latch := pool.NewLatch(numChildren)
	childContexts := make([]*state.Context, numChildren)

	for i, child := range node.Children {
		childIdx := i
		childNode := child
		childBit := (nodeBit << uint(childIdx+1))
		if childBit == 0 {
			childBit = uint64(1 << (childIdx % 64))
		}

		// Fork state snapshot for each child branch
		childCtx := sctx.Fork()
		childContexts[childIdx] = childCtx

		e.workerPool.Submit(func() {
			defer latch.CountDown()
			e.executeNode(ctx, childNode, childCtx, childBit)
		})
	}

	// Wait for all parallel child branches to finish
	latch.Wait()

	// Merge child states back into parent context
	for _, childCtx := range childContexts {
		sctx.MergeChild(childCtx)
	}
}

// executeStep executes a single StepDefinition within a node.
func (e *Executor) executeStep(ctx context.Context, step *types.StepDefinition, sctx *state.Context) error {
	switch step.Type {
	case types.StepVolt:
		if step.Script == "" {
			return nil
		}
		prog, err := e.compiler.Compile(step.Script)
		if err != nil {
			return err
		}
		// Evaluate CEL / Volt script against current snapshot
		snapshot := sctx.Snapshot()
		out, _, err := prog.Eval(snapshot)
		if err != nil {
			return err
		}
		// If script called set(), reflect variables into scratchpad
		_ = out
		return nil

	case types.StepSink:
		if step.Condition != "" {
			passed, err := e.evalCondition(ctx, step.Condition, sctx)
			if err != nil || !passed {
				return err
			}
		}
		if e.sinkFn != nil && step.SinkName != "" {
			payload := sctx.Snapshot()
			return e.sinkFn(step.SinkName, payload)
		}
		return nil

	case types.StepReturn:
		if len(step.ReturnMap) > 0 {
			res := make(map[string]any, len(step.ReturnMap))
			for outKey, stateKeyOrExpr := range step.ReturnMap {
				if keyStr, ok := stateKeyOrExpr.(string); ok {
					if val, found := sctx.Get(keyStr); found {
						res[outKey] = val
					} else {
						res[outKey] = keyStr
					}
				} else {
					res[outKey] = stateKeyOrExpr
				}
			}
			sctx.SetReturn(res)
		}
		return nil

	case types.StepAbort:
		sctx.Abort()
		return nil

	default:
		return nil
	}
}

// evalCondition evaluates a boolean Volt guard expression.
func (e *Executor) evalCondition(ctx context.Context, expr string, sctx *state.Context) (bool, error) {
	prog, err := e.compiler.Compile(expr)
	if err != nil {
		return false, err
	}
	snapshot := sctx.Snapshot()
	out, _, err := prog.Eval(snapshot)
	if err != nil {
		return false, err
	}
	if b, ok := out.Value().(bool); ok {
		return b, nil
	}
	return false, fmt.Errorf("condition %q did not evaluate to boolean", expr)
}
