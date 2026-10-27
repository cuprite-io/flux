package eval

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/cuprite-io/flux/cmd/flux/tui/client"
	"github.com/cuprite-io/flux/internal/compiler"
	"github.com/cuprite-io/flux/internal/state"
	"github.com/cuprite-io/flux/types"
	"github.com/google/cel-go/cel"
	celtypes "github.com/google/cel-go/common/types"
)

// NodeStatus defines the branch evaluation outcome for a circuit DAG node.
type NodeStatus string

const (
	NodeStatusPassed  NodeStatus = "PASSED"
	NodeStatusPruned  NodeStatus = "PRUNED"
	NodeStatusFailed  NodeStatus = "FAILED"
	NodeStatusSkipped NodeStatus = "SKIPPED"
)

// StepTrace captures fine-grained execution trace of a single step.
type StepTrace struct {
	Type            types.StepType `json:"type"`
	Script          string         `json:"script,omitempty"`
	Condition       string         `json:"condition,omitempty"`
	SinkName        string         `json:"sink_name,omitempty"`
	Executed        bool           `json:"executed"`
	Passed          bool           `json:"passed"`
	Mutations       map[string]any `json:"mutations,omitempty"`
	OutboundPayload any            `json:"outbound_payload,omitempty"`
	Error           string         `json:"error,omitempty"`
}

// NodeTrace captures evaluation state for a single node in the circuit DAG.
type NodeTrace struct {
	Name            string       `json:"name"`
	Condition       string       `json:"condition,omitempty"`
	Status          NodeStatus   `json:"status"`
	ConditionResult bool         `json:"condition_result"`
	Steps           []StepTrace  `json:"steps,omitempty"`
	Error           string       `json:"error,omitempty"`
	Children        []*NodeTrace `json:"children,omitempty"`
}

// SimulatedSinkDispatch records an outbound sink firing without executing network I/O.
type SimulatedSinkDispatch struct {
	SinkName  string    `json:"sink_name"`
	NodeName  string    `json:"node_name"`
	Condition string    `json:"condition,omitempty"`
	Payload   any       `json:"payload"`
	Timestamp time.Time `json:"timestamp"`
}

// EvalResult encapsulates the complete dry-run evaluation outcome and traces.
type EvalResult struct {
	CircuitID      string                  `json:"circuit_id"`
	Passed         bool                    `json:"passed"`
	Duration       time.Duration           `json:"duration"`
	TotalNodes     int                     `json:"total_nodes"`
	PassedNodes    int                     `json:"passed_nodes"`
	PrunedNodes    int                     `json:"pruned_nodes"`
	RootTrace      *NodeTrace              `json:"root_trace,omitempty"`
	MutatedState   map[string]any          `json:"mutated_state,omitempty"`
	SimulatedSinks []SimulatedSinkDispatch `json:"simulated_sinks,omitempty"`
	Errors         []string                `json:"errors,omitempty"`
}

type evalRunContext struct {
	ctx            context.Context
	comp           *compiler.Compiler
	sctx           *state.Context
	payloadMap     map[string]any
	res            *EvalResult
	simulatedSinks []SimulatedSinkDispatch
}

// RunCircuitEval executes a Circuit definition against a JSON payload in dry-run mode,
// tracing branch conditions, VoltScript mutations, and simulated sink dispatches.
func RunCircuitEval(ctx context.Context, circuit *types.Circuit, payloadJSON string) (*EvalResult, error) {
	if circuit == nil || circuit.Root == nil {
		return nil, fmt.Errorf("flux eval: circuit definition or root node is missing")
	}

	cleanJSON := strings.TrimSpace(payloadJSON)
	if cleanJSON == "" {
		cleanJSON = "{}"
	}

	var payloadMap map[string]any
	if err := json.Unmarshal([]byte(cleanJSON), &payloadMap); err != nil {
		return nil, fmt.Errorf("invalid JSON payload: %w", err)
	}

	start := time.Now()
	res := &EvalResult{
		CircuitID:    circuit.ID,
		Passed:       true,
		MutatedState: make(map[string]any),
	}

	comp, err := compiler.NewCompiler(nil)
	if err != nil {
		return nil, fmt.Errorf("flux eval: compiler init failed: %w", err)
	}

	sctx := state.AcquireContext(ctx, payloadMap)
	sctx.SetShadowMode(true) // Ensure sink side-effects are suppressed
	defer state.ReleaseContext(sctx)

	evalCtx := &evalRunContext{
		ctx:            ctx,
		comp:           comp,
		sctx:           sctx,
		payloadMap:     payloadMap,
		res:            res,
		simulatedSinks: make([]SimulatedSinkDispatch, 0),
	}

	res.RootTrace = evalCtx.evalNode(circuit.Root, false)
	res.SimulatedSinks = evalCtx.simulatedSinks
	res.Duration = time.Since(start)
	res.Passed = res.Passed && len(res.Errors) == 0 && (res.RootTrace != nil && res.RootTrace.Status == NodeStatusPassed)

	for k, v := range sctx.Snapshot() {
		res.MutatedState[k] = v
	}

	return res, nil
}

func (c *evalRunContext) evalNode(node *types.Node, ancestorPruned bool) *NodeTrace {
	c.res.TotalNodes++
	trace := &NodeTrace{
		Name:      node.Name,
		Condition: node.Condition,
	}

	if ancestorPruned {
		trace.Status = NodeStatusPruned
		trace.ConditionResult = false
		c.res.PrunedNodes++
		for _, child := range node.Children {
			trace.Children = append(trace.Children, c.evalNode(child, true))
		}
		return trace
	}

	passed := true
	if node.Condition != "" {
		condPassed, err := c.evalCond(node.Condition)
		if err != nil {
			trace.Status = NodeStatusFailed
			trace.Error = err.Error()
			c.res.Errors = append(c.res.Errors, fmt.Sprintf("node %q: %v", node.Name, err))
			c.res.Passed = false
			passed = false
		} else {
			trace.ConditionResult = condPassed
			passed = condPassed
		}
	} else {
		trace.ConditionResult = true
	}

	if !passed {
		if trace.Status != NodeStatusFailed {
			trace.Status = NodeStatusPruned
		}
		c.res.PrunedNodes++
		for _, child := range node.Children {
			trace.Children = append(trace.Children, c.evalNode(child, true))
		}
		return trace
	}

	trace.Status = NodeStatusPassed
	c.res.PassedNodes++

	for _, step := range node.Steps {
		st := c.evalStep(node.Name, step)
		trace.Steps = append(trace.Steps, st)
	}

	for _, child := range node.Children {
		trace.Children = append(trace.Children, c.evalNode(child, false))
	}

	return trace
}

func (c *evalRunContext) evalStep(nodeName string, step *types.StepDefinition) StepTrace {
	st := StepTrace{
		Type:      step.Type,
		Script:    step.Script,
		Condition: step.Condition,
		SinkName:  step.SinkName,
		Executed:  true,
		Passed:    true,
	}

	switch step.Type {
	case types.StepVolt:
		if step.Script == "" {
			return st
		}
		setStmts, remainder := compiler.ExtractSetStatementsAndRemainder(step.Script)
		if len(setStmts) > 0 {
			st.Mutations = make(map[string]any)
			for _, stmt := range setStmts {
				p, err := c.compileExpr(stmt.Expr)
				if err != nil {
					st.Error = err.Error()
					st.Passed = false
					c.res.Errors = append(c.res.Errors, fmt.Sprintf("volt step compile error: %v", err))
					continue
				}
				out, _, errEval := p.ContextEval(c.ctx, c.sctx)
				if errEval != nil {
					st.Error = errEval.Error()
					st.Passed = false
					c.res.Errors = append(c.res.Errors, fmt.Sprintf("volt eval %s: %v", stmt.Key, errEval))
					continue
				}
				if out != nil {
					val := out.Value()
					c.sctx.Set(stmt.Key, val)
					st.Mutations[stmt.Key] = val
					c.res.MutatedState[stmt.Key] = val
				}
			}
		}
		if remainder != "" {
			p, err := c.compileExpr(remainder)
			if err == nil {
				_, _, _ = p.ContextEval(c.ctx, c.sctx)
			}
		}

	case types.StepSink:
		sinkPassed := true
		if step.Condition != "" {
			p, err := c.evalCond(step.Condition)
			if err != nil {
				st.Error = err.Error()
				st.Passed = false
				sinkPassed = false
			} else {
				sinkPassed = p
			}
		}
		st.Passed = sinkPassed
		if sinkPassed {
			var payload any
			if step.Payload != "" {
				if val, found := c.sctx.Get(step.Payload); found {
					payload = val
				} else if m, ok := c.sctx.OriginalInput.(map[string]any); ok {
					if val, found := m[step.Payload]; found {
						payload = val
					}
				}
			}
			if payload == nil {
				payload = c.sctx.Snapshot()
			}
			st.OutboundPayload = payload
			c.simulatedSinks = append(c.simulatedSinks, SimulatedSinkDispatch{
				SinkName:  step.SinkName,
				NodeName:  nodeName,
				Condition: step.Condition,
				Payload:   payload,
				Timestamp: time.Now(),
			})
		}
	}

	return st
}

func (c *evalRunContext) compileExpr(expr string) (cel.Program, error) {
	p, err := c.comp.Compile(expr)
	if err == nil {
		return p, nil
	}

	// Dynamic fallback for expressions referencing top-level JSON fields
	var vars []cel.EnvOption
	vars = append(vars,
		cel.Variable("payload", cel.MapType(cel.StringType, cel.AnyType)),
		cel.Variable("state", cel.MapType(cel.StringType, cel.AnyType)),
		cel.Variable("entity", cel.MapType(cel.StringType, cel.AnyType)),
		cel.Variable("user", cel.MapType(cel.StringType, cel.AnyType)),
		cel.Variable("item", cel.MapType(cel.StringType, cel.AnyType)),
		cel.Variable("context", cel.MapType(cel.StringType, cel.AnyType)),
	)
	for k := range c.payloadMap {
		vars = append(vars, cel.Variable(k, cel.AnyType))
	}
	for k := range c.res.MutatedState {
		vars = append(vars, cel.Variable(k, cel.AnyType))
	}

	env, envErr := cel.NewEnv(vars...)
	if envErr == nil {
		ast, iss := env.Compile(expr)
		if iss == nil || iss.Err() == nil {
			if dynProg, pErr := env.Program(ast); pErr == nil {
				return dynProg, nil
			}
		}
	}

	return nil, err
}

func (c *evalRunContext) evalCond(expr string) (bool, error) {
	p, err := c.compileExpr(expr)
	if err != nil {
		return false, err
	}
	out, _, err := p.ContextEval(c.ctx, c.sctx)
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

// FetchOrGenerateExemplar retrieves the latest exemplar from Autopilot/Schema Tap or synthesizes a representative sample.
func FetchOrGenerateExemplar(ctx context.Context, cli client.Client, circuit *types.Circuit) (string, error) {
	if cli != nil {
		// 1. Try pulling from active Autopilot sessions
		if sessions, err := cli.GetAutopilot(ctx); err == nil {
			for _, s := range sessions {
				if s.AuditLog != nil && len(s.AuditLog.Exemplars) > 0 {
					tagMatches := false
					if circuit != nil && len(circuit.Tags) > 0 {
						for _, t := range circuit.Tags {
							if slices.Contains(s.Tags, t) {
								tagMatches = true
								break
							}
						}
					} else {
						tagMatches = true
					}
					if tagMatches {
						var parsed any
						if json.Unmarshal([]byte(s.AuditLog.Exemplars[0]), &parsed) == nil {
							if pretty, err := json.MarshalIndent(parsed, "", "  "); err == nil {
								return string(pretty), nil
							}
						}
						return s.AuditLog.Exemplars[0], nil
					}
				}
			}
		}
	}

	// 2. Synthesize representative exemplar matched to circuit domain
	sample := generateSampleForCircuit(circuit)
	pretty, err := json.MarshalIndent(sample, "", "  ")
	if err != nil {
		return "{\n  \"amount\": 12500,\n  \"user_tier\": \"VIP\"\n}", nil
	}
	return string(pretty), nil
}

func generateSampleForCircuit(circuit *types.Circuit) map[string]any {
	tags := []string{}
	id := ""
	if circuit != nil {
		tags = circuit.Tags
		id = strings.ToLower(circuit.ID)
	}

	hasTag := func(keywords ...string) bool {
		for _, kw := range keywords {
			if strings.Contains(id, kw) {
				return true
			}
			for _, t := range tags {
				if strings.Contains(strings.ToLower(t), kw) {
					return true
				}
			}
		}
		return false
	}

	if hasTag("payment", "fraud", "checkout", "billing", "trans") {
		return map[string]any{
			"amount":       12500.0,
			"currency":     "USD",
			"user_tier":    "VIP",
			"card_country": "US",
			"ip_country":   "UA",
			"velocity_1h":  4,
			"action":       "checkout",
			"risk_score":   85,
		}
	}

	if hasTag("security", "auth", "iam", "login", "mfa") {
		return map[string]any{
			"user_id":         "usr_9981",
			"ip_address":      "198.51.100.24",
			"failed_attempts": 5,
			"mfa_enabled":     false,
			"country":         "RO",
			"action":          "login",
		}
	}

	if hasTag("metric", "iot", "cpu", "temp", "telemetry", "monitor") {
		return map[string]any{
			"host":            "prod-worker-04",
			"cpu_utilization": 94.2,
			"memory_pct":      88.5,
			"temperature":     82.0,
			"status":          "WARN",
			"environment":     "production",
		}
	}

	return map[string]any{
		"id":          "evt-7729",
		"event_type":  "transaction",
		"amount":      15000,
		"tier":        "enterprise",
		"status":      "active",
		"country":     "US",
		"environment": "production",
	}
}
