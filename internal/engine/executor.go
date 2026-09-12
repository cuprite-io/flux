package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/cuprite-io/flux/internal/cache"
	"github.com/cuprite-io/flux/internal/compiler"
	"github.com/cuprite-io/flux/internal/pool"
	"github.com/cuprite-io/flux/internal/state"
	"github.com/cuprite-io/flux/internal/telemetry"
	"github.com/cuprite-io/flux/internal/vm"
	"github.com/cuprite-io/flux/types"
	"github.com/google/cel-go/cel"
	celtypes "github.com/google/cel-go/common/types"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var (
	ErrCircuitAborted = errors.New("flux engine: circuit execution aborted")
)

// SinkDispatcher defines the callback function to dispatch payloads to external sinks.
type SinkDispatcher func(sinkName string, payload any) error

type setCompiledStmt struct {
	key  string
	prog cel.Program
}

type compiledVoltScript struct {
	setStatements []setCompiledStmt
	mainProg      cel.Program
}

// Executor coordinates the execution of Circuit trees.
type Executor struct {
	compiler    *compiler.Compiler
	workerPool  *pool.WorkerPool
	sinkFn      SinkDispatcher
	scriptCache *cache.BoundedCache[string, *compiledVoltScript]
	tracer      *telemetry.Tracer
}

// NewExecutor creates a new Circuit tree Executor.
func NewExecutor(comp *compiler.Compiler, wp *pool.WorkerPool, sinkFn SinkDispatcher, opts ...ExecutorOption) *Executor {
	if wp == nil {
		wp = pool.GetDefaultPool()
	}
	exec := &Executor{
		compiler:    comp,
		workerPool:  wp,
		sinkFn:      sinkFn,
		scriptCache: cache.NewBoundedCache[string, *compiledVoltScript](4096),
	}
	for _, opt := range opts {
		opt(exec)
	}
	return exec
}

// ExecutorOption configures an Executor instance.
type ExecutorOption func(*Executor)

// WithExecutorTracer configures an OpenTelemetry tracer on the executor.
func WithExecutorTracer(tr *telemetry.Tracer) ExecutorOption {
	return func(e *Executor) {
		e.tracer = tr
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

	var span trace.Span
	if e.tracer.IsEnabled() {
		ctx, span = e.tracer.Start(ctx, "flux.circuit:"+circuit.ID,
			trace.WithAttributes(
				telemetry.AttrCircuitID.String(circuit.ID),
				telemetry.AttrCircuitTags.StringSlice(circuit.Tags),
			),
		)
	}

	// Start execution from root node at bit index 0
	e.executeNode(ctx, circuit.Root, sctx, 1<<0)

	if sctx.IsAborted() {
		err := ErrCircuitAborted
		if span != nil {
			span.SetAttributes(telemetry.AttrPassed.Bool(false))
			telemetry.EndSpan(span, err)
		}
		return &types.SparkResult{
			OriginalInput:    sctx.OriginalInput,
			ReturnedData:     sctx.ReturnData(),
			Passed:           false,
			ExecutedCircuits: []string{circuit.ID},
			Errors:           sctx.Errors(),
		}, err
	}

	passed := len(sctx.Errors()) == 0
	if span != nil {
		span.SetAttributes(telemetry.AttrPassed.Bool(passed))
		telemetry.EndSpan(span, nil)
	}

	return &types.SparkResult{
		OriginalInput:    sctx.OriginalInput,
		ReturnedData:     sctx.ReturnData(),
		Passed:           passed,
		ExecutedCircuits: []string{circuit.ID},
		Errors:           sctx.Errors(),
	}, nil
}

// executeNode evaluates a node's condition, executes its sequential steps, and dispatches children.
func (e *Executor) executeNode(ctx context.Context, node *types.Node, sctx *state.Context, nodeBit uint64) {
	gid := compiler.SetCurrentContext(ctx)
	defer compiler.ClearCurrentContext(gid)

	if ctx.Err() != nil {
		sctx.Abort()
		return
	}

	// 1. Bitmask Pruning Check: If this node is already marked dead, skip entire sub-tree
	if sctx.IsPruned(nodeBit) || sctx.IsAborted() {
		return
	}

	var span trace.Span
	if e.tracer.IsEnabled() {
		ctx, span = e.tracer.Start(ctx, "flux.node:"+node.Name,
			trace.WithAttributes(
				telemetry.AttrNodeName.String(node.Name),
				telemetry.AttrNodeCondition.String(node.Condition),
			),
		)
		defer func() {
			if span != nil {
				span.SetAttributes(telemetry.AttrNodePruned.Bool(sctx.IsPruned(nodeBit)))
				telemetry.EndSpan(span, nil)
			}
		}()
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
		if ctx.Err() != nil || sctx.IsAborted() {
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
		var childBit uint64
		if childIdx < 62 {
			childBit = (nodeBit << uint(childIdx+1))
		}
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

func (e *Executor) getOrCompileScript(script string) (*compiledVoltScript, error) {
	if v, ok := e.scriptCache.Get(script); ok {
		return v, nil
	}

	compiled := &compiledVoltScript{}
	setStatements, remainder := compiler.ExtractSetStatementsAndRemainder(script)
	if len(setStatements) > 0 {
		for _, stmt := range setStatements {
			p, err := e.compiler.Compile(stmt.Expr)
			if err != nil {
				return nil, err
			}
			compiled.setStatements = append(compiled.setStatements, setCompiledStmt{
				key:  stmt.Key,
				prog: p,
			})
		}
	}

	if remainder != "" {
		p, err := e.compiler.Compile(remainder)
		if err != nil {
			return nil, err
		}
		compiled.mainProg = p
	}

	actual, _ := e.scriptCache.LoadOrStore(script, compiled)
	return actual, nil
}

// executeStep executes a single StepDefinition within a node.
func (e *Executor) executeStep(ctx context.Context, step *types.StepDefinition, sctx *state.Context) error {
	var span trace.Span
	if e.tracer.IsEnabled() && step.Type != types.StepSink { // sink steps manage their own span with payload info
		spanName := "flux.step:" + strings.ToLower(step.Type.String())
		var attrs []attribute.KeyValue
		attrs = append(attrs, telemetry.AttrStepType.String(strings.ToLower(step.Type.String())))
		if step.Script != "" {
			attrs = append(attrs, telemetry.AttrStepScript.String(step.Script))
		}
		ctx, span = e.tracer.Start(ctx, spanName, trace.WithAttributes(attrs...))
		defer func() {
			telemetry.EndSpan(span, nil)
		}()
	}

	switch step.Type {
	case types.StepVolt:
		if step.Script == "" {
			return nil
		}
		compiled, err := e.getOrCompileScript(step.Script)
		if err != nil {
			if span != nil {
				telemetry.EndSpan(span, err)
			}
			return err
		}

		if len(compiled.setStatements) > 0 {
			for _, stmt := range compiled.setStatements {
				out, _, errEval := stmt.prog.ContextEval(ctx, sctx)
				if errEval != nil {
					return fmt.Errorf("set %s: %w", stmt.key, errEval)
				}
				if out != nil {
					sctx.Set(stmt.key, out.Value())
				}
			}
		}

		if compiled.mainProg != nil {
			out, _, err := compiled.mainProg.ContextEval(ctx, sctx)
			if err != nil {
				return err
			}
			if out == celtypes.False || (out != nil && out.Value() == false) {
				sctx.Abort()
			}
			return nil
		}
		return nil

	case types.StepVM:
		if prog, ok := step.Program.(*vm.Program); ok && prog != nil {
			arena := vm.AcquireFrame()
			defer vm.ReleaseFrame(arena)
			adapter := &state.VMContextAdapter{Ctx: sctx}
			_, errVM := vm.Run(prog, arena, adapter)
			return errVM
		}
		return nil

	case types.StepSink:
		if step.Condition != "" {
			passed, err := e.evalCondition(ctx, step.Condition, sctx)
			if err != nil || !passed {
				return err
			}
		}
		if e.sinkFn != nil && step.SinkName != "" {
			var payload any
			if step.Payload != "" {
				if val, found := sctx.Get(step.Payload); found {
					payload = val
				} else if m, ok := sctx.OriginalInput.(map[string]any); ok {
					if val, found := m[step.Payload]; found {
						payload = val
					}
				}
			}
			if payload == nil {
				payload = sctx.Snapshot()
			}

			if e.tracer.IsEnabled() {
				_, span := e.tracer.Start(ctx, "flux.sink:"+step.SinkName,
					trace.WithAttributes(
						telemetry.AttrSinkName.String(step.SinkName),
						telemetry.AttrStepType.String("sink"),
					),
				)
				err := e.sinkFn(step.SinkName, payload)
				telemetry.EndSpan(span, err)
				return err
			}

			return e.sinkFn(step.SinkName, payload)
		}
		return nil

	case types.StepReturn:
		if len(step.ReturnMap) > 0 {
			res := make(map[string]any, len(step.ReturnMap))
			for outKey, stateKeyOrExpr := range step.ReturnMap {
				if keyStr, ok := stateKeyOrExpr.(string); ok {
					if strings.HasPrefix(keyStr, "$") {
						// Explicit state variable lookup prefix
						rawKey := strings.TrimPrefix(keyStr, "$")
						if val, found := sctx.Get(rawKey); found {
							res[outKey] = val
						} else {
							res[outKey] = nil
						}
					} else if val, found := sctx.Get(keyStr); found {
						res[outKey] = val
					} else if m, ok := sctx.OriginalInput.(map[string]any); ok {
						if val, found := m[keyStr]; found {
							res[outKey] = val
						} else {
							res[outKey] = keyStr
						}
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
	if err := ctx.Err(); err != nil {
		return false, err
	}
	prog, err := e.compiler.Compile(expr)
	if err != nil {
		return false, err
	}
	out, _, err := prog.ContextEval(ctx, sctx)
	if err != nil {
		return false, err
	}
	if out == celtypes.True {
		return true, nil
	}
	if out == celtypes.False {
		return false, nil
	}
	if b, ok := out.Value().(bool); ok {
		return b, nil
	}
	return false, fmt.Errorf("condition %q did not evaluate to boolean", expr)
}
