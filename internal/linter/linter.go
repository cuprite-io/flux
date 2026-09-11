package linter

import (
	"fmt"
	"strings"

	"github.com/cuprite-io/flux/internal/compiler"
	"github.com/cuprite-io/flux/types"
)

// Severity represents the severity level of a lint issue.
type Severity string

const (
	SeverityError   Severity = "ERROR"
	SeverityWarning Severity = "WARNING"
	SeverityInfo    Severity = "INFO"
)

// Issue represents a diagnostic reported by the linter.
type Issue struct {
	RuleID   string   `json:"rule_id"`
	Severity Severity `json:"severity"`
	Node     string   `json:"node,omitempty"`
	Message  string   `json:"message"`
	Detail   string   `json:"detail,omitempty"`
}

func (i Issue) String() string {
	nodeCtx := ""
	if i.Node != "" {
		nodeCtx = fmt.Sprintf(" [node: %s]", i.Node)
	}
	detail := ""
	if i.Detail != "" {
		detail = fmt.Sprintf(" - %s", i.Detail)
	}
	return fmt.Sprintf("[%s] %s%s: %s%s", i.Severity, i.RuleID, nodeCtx, i.Message, detail)
}

// Result aggregates all issues detected in a Circuit.
type Result struct {
	CircuitID string   `json:"circuit_id"`
	FilePath  string   `json:"file_path,omitempty"`
	Issues    []Issue  `json:"issues"`
	NodeCount int      `json:"node_count"`
	StepCount int      `json:"step_count"`
}

// HasErrors returns true if any issue has severity ERROR.
func (r *Result) HasErrors() bool {
	for _, iss := range r.Issues {
		if iss.Severity == SeverityError {
			return true
		}
	}
	return false
}

// HasWarnings returns true if any issue has severity WARNING.
func (r *Result) HasWarnings() bool {
	for _, iss := range r.Issues {
		if iss.Severity == SeverityWarning {
			return true
		}
	}
	return false
}

// Options configures linter rules and thresholds.
type Options struct {
	MaxTreeDepth  int
	MaxStepCount  int
	CheckSinks    bool
	Strict        bool
}

// DefaultOptions provides sensible defaults for production Circuit DAGs.
func DefaultOptions() Options {
	return Options{
		MaxTreeDepth: 10,
		MaxStepCount: 50,
		CheckSinks:   true,
		Strict:       false,
	}
}

// Linter performs deep static analysis and AST validation on Flux circuits.
type Linter struct {
	compiler *compiler.Compiler
	opts     Options
}

// New creates a new Circuit Linter.
func New(opts Options) (*Linter, error) {
	comp, err := compiler.NewCompiler(nil)
	if err != nil {
		return nil, fmt.Errorf("linter: failed to initialize compiler: %w", err)
	}
	return &Linter{
		compiler: comp,
		opts:     opts,
	}, nil
}

// Lint runs static analysis across a Circuit DAG and returns the diagnostic Result.
func (l *Linter) Lint(circuit *types.Circuit) *Result {
	res := &Result{
		CircuitID: "",
		Issues:    make([]Issue, 0),
	}

	if circuit == nil {
		res.Issues = append(res.Issues, Issue{
			RuleID:   "LINT-000",
			Severity: SeverityError,
			Message:  "circuit definition is nil",
		})
		return res
	}

	res.CircuitID = circuit.ID
	if strings.TrimSpace(circuit.ID) == "" {
		res.Issues = append(res.Issues, Issue{
			RuleID:   "LINT-001",
			Severity: SeverityError,
			Message:  "circuit is missing mandatory 'id' property",
		})
	}

	if circuit.Root == nil {
		res.Issues = append(res.Issues, Issue{
			RuleID:   "LINT-002",
			Severity: SeverityError,
			Message:  "circuit root node is nil",
		})
		return res
	}

	seenNodeNames := make(map[string]bool)
	knownVariables := make(map[string]bool)

	// Built-in map keys typically available from inputs
	knownVariables["payload"] = true
	knownVariables["state"] = true
	knownVariables["entity"] = true
	knownVariables["user"] = true
	knownVariables["item"] = true
	knownVariables["context"] = true

	l.lintNode(circuit.Root, res, seenNodeNames, knownVariables, nil, 1)
	return res
}

func (l *Linter) lintNode(
	node *types.Node,
	res *Result,
	seenNames map[string]bool,
	varsInScope map[string]bool,
	parentConditions []string,
	depth int,
) {
	if node == nil {
		return
	}

	res.NodeCount++

	// 1. Node Name Checks
	if strings.TrimSpace(node.Name) == "" {
		res.Issues = append(res.Issues, Issue{
			RuleID:   "LINT-010",
			Severity: SeverityWarning,
			Node:     node.Name,
			Message:  "node has empty name identifier",
		})
	} else {
		if seenNames[node.Name] {
			res.Issues = append(res.Issues, Issue{
				RuleID:   "LINT-011",
				Severity: SeverityError,
				Node:     node.Name,
				Message:  fmt.Sprintf("duplicate node name %q detected in DAG (node names must be globally unique within a circuit)", node.Name),
			})
		}
		seenNames[node.Name] = true
	}

	// 2. Tree Depth Check
	if l.opts.MaxTreeDepth > 0 && depth > l.opts.MaxTreeDepth {
		res.Issues = append(res.Issues, Issue{
			RuleID:   "LINT-012",
			Severity: SeverityWarning,
			Node:     node.Name,
			Message:  fmt.Sprintf("node tree depth (%d) exceeds recommended limit (%d)", depth, l.opts.MaxTreeDepth),
		})
	}

	// 3. Node Guard Condition Validation
	currentConditions := append([]string{}, parentConditions...)
	if node.Condition != "" {
		trimmedCond := strings.TrimSpace(node.Condition)

		// Literal false check
		if trimmedCond == "false" || trimmedCond == "FALSE" {
			res.Issues = append(res.Issues, Issue{
				RuleID:   "LINT-020",
				Severity: SeverityError,
				Node:     node.Name,
				Message:  "node condition is literal false; this branch is completely dead and will never execute",
			})
		}

		// Literal true warning (guard is redundant)
		if trimmedCond == "true" || trimmedCond == "TRUE" {
			res.Issues = append(res.Issues, Issue{
				RuleID:   "LINT-021",
				Severity: SeverityInfo,
				Node:     node.Name,
				Message:  "node condition is literal true; guard is redundant",
			})
		}

		// CEL Expression Compile & Syntax Verification
		if err := l.compiler.Validate(node.Condition); err != nil {
			res.Issues = append(res.Issues, Issue{
				RuleID:   "LINT-022",
				Severity: SeverityError,
				Node:     node.Name,
				Message:  fmt.Sprintf("syntax error in node condition: %v", err),
				Detail:   node.Condition,
			})
		}

		// Contradiction / Conflict Checks with Parent Conditions
		for _, parentCond := range parentConditions {
			if isContradictoryCondition(parentCond, trimmedCond) {
				res.Issues = append(res.Issues, Issue{
					RuleID:   "LINT-023",
					Severity: SeverityWarning,
					Node:     node.Name,
					Message:  fmt.Sprintf("child condition %q appears contradictory with parent guard %q", trimmedCond, parentCond),
				})
			}
		}

		currentConditions = append(currentConditions, trimmedCond)
	}

	// 4. Empty Node Check
	if len(node.Steps) == 0 && len(node.Children) == 0 {
		res.Issues = append(res.Issues, Issue{
			RuleID:   "LINT-030",
			Severity: SeverityWarning,
			Node:     node.Name,
			Message:  "empty node with 0 steps and 0 children",
		})
	}

	// 5. Steps Evaluation & Analysis
	hasTerminalStep := false
	localVars := make(map[string]bool)
	for k, v := range varsInScope {
		localVars[k] = v
	}

	res.StepCount += len(node.Steps)
	if l.opts.MaxStepCount > 0 && len(node.Steps) > l.opts.MaxStepCount {
		res.Issues = append(res.Issues, Issue{
			RuleID:   "LINT-031",
			Severity: SeverityWarning,
			Node:     node.Name,
			Message:  fmt.Sprintf("node has %d steps, which exceeds the recommended limit of %d", len(node.Steps), l.opts.MaxStepCount),
		})
	}

	for sIdx, step := range node.Steps {
		if hasTerminalStep {
			res.Issues = append(res.Issues, Issue{
				RuleID:   "LINT-032",
				Severity: SeverityWarning,
				Node:     node.Name,
				Message:  fmt.Sprintf("unreachable step at index %d (preceded by terminal return/abort step in the same node)", sIdx),
			})
		}

		if step == nil {
			res.Issues = append(res.Issues, Issue{
				RuleID:   "LINT-033",
				Severity: SeverityError,
				Node:     node.Name,
				Message:  fmt.Sprintf("step at index %d is nil", sIdx),
			})
			continue
		}

		// Validate Step Condition if present
		if step.Condition != "" {
			if err := l.compiler.Validate(step.Condition); err != nil {
				res.Issues = append(res.Issues, Issue{
					RuleID:   "LINT-040",
					Severity: SeverityError,
					Node:     node.Name,
					Message:  fmt.Sprintf("syntax error in step %d condition: %v", sIdx, err),
					Detail:   step.Condition,
				})
			}
		}

		switch step.Type {
		case types.StepVolt:
			if strings.TrimSpace(step.Script) == "" {
				res.Issues = append(res.Issues, Issue{
					RuleID:   "LINT-041",
					Severity: SeverityWarning,
					Node:     node.Name,
					Message:  fmt.Sprintf("step %d (VOLT) has empty script", sIdx),
				})
				continue
			}

			// Extract set(...) statements and validate each individually
			setStmts, remainder := compiler.ExtractSetStatementsAndRemainder(step.Script)
			for _, stmt := range setStmts {
				if strings.TrimSpace(stmt.Key) == "" {
					res.Issues = append(res.Issues, Issue{
						RuleID:   "LINT-042",
						Severity: SeverityError,
						Node:     node.Name,
						Message:  fmt.Sprintf("step %d contains set() with empty variable key", sIdx),
					})
				} else {
					localVars[stmt.Key] = true
				}

				if strings.TrimSpace(stmt.Expr) == "" {
					res.Issues = append(res.Issues, Issue{
						RuleID:   "LINT-043",
						Severity: SeverityError,
						Node:     node.Name,
						Message:  fmt.Sprintf("step %d contains set(%q) with empty expression", sIdx, stmt.Key),
					})
				} else if err := l.compiler.Validate(stmt.Expr); err != nil {
					res.Issues = append(res.Issues, Issue{
						RuleID:   "LINT-044",
						Severity: SeverityError,
						Node:     node.Name,
						Message:  fmt.Sprintf("step %d set(%q) expression syntax error: %v", sIdx, stmt.Key, err),
						Detail:   stmt.Expr,
					})
				}
			}

			if remainder != "" {
				if err := l.compiler.Validate(remainder); err != nil {
					res.Issues = append(res.Issues, Issue{
						RuleID:   "LINT-045",
						Severity: SeverityError,
						Node:     node.Name,
						Message:  fmt.Sprintf("step %d volt script remainder syntax error: %v", sIdx, err),
						Detail:   remainder,
					})
				}
			}

		case types.StepSink:
			if strings.TrimSpace(step.SinkName) == "" {
				res.Issues = append(res.Issues, Issue{
					RuleID:   "LINT-050",
					Severity: SeverityError,
					Node:     node.Name,
					Message:  fmt.Sprintf("step %d (SINK) is missing mandatory sink name", sIdx),
				})
			}

		case types.StepReturn:
			if len(step.ReturnMap) == 0 {
				res.Issues = append(res.Issues, Issue{
					RuleID:   "LINT-060",
					Severity: SeverityInfo,
					Node:     node.Name,
					Message:  fmt.Sprintf("step %d (RETURN) has empty data projection map", sIdx),
				})
			} else {
				// Check for unresolved $variable references
				for outKey, val := range step.ReturnMap {
					if strVal, ok := val.(string); ok && strings.HasPrefix(strVal, "$") {
						varName := strings.TrimPrefix(strVal, "$")
						// Check if varName has dot notation (e.g. $payload.email) or direct variable
						rootVar := strings.Split(varName, ".")[0]
						if !localVars[rootVar] && !localVars[varName] {
							res.Issues = append(res.Issues, Issue{
								RuleID:   "LINT-061",
								Severity: SeverityWarning,
								Node:     node.Name,
								Message:  fmt.Sprintf("return key %q references variable %q which was never set in prior steps", outKey, strVal),
							})
						}
					}
				}
			}
			// StepReturn sets return data but does not stop execution of subsequent steps (e.g. abort or sinks)
			// Only StepAbort halts execution unconditionally.

		case types.StepAbort:
			if step.Condition == "" {
				hasTerminalStep = true
			}

		case types.StepVM:
			if step.Program == nil {
				res.Issues = append(res.Issues, Issue{
					RuleID:   "LINT-070",
					Severity: SeverityError,
					Node:     node.Name,
					Message:  fmt.Sprintf("step %d (VM) has nil program instance", sIdx),
				})
			}
		}
	}

	// 6. Child Nodes after unconditional Terminal Step
	if hasTerminalStep && len(node.Children) > 0 {
		res.Issues = append(res.Issues, Issue{
			RuleID:   "LINT-080",
			Severity: SeverityWarning,
			Node:     node.Name,
			Message:  "node has children but terminates unconditionally with StepReturn/StepAbort",
		})
	}

	// Recurse into children
	for _, child := range node.Children {
		l.lintNode(child, res, seenNames, localVars, currentConditions, depth+1)
	}
}

// isContradictoryCondition checks for simple conflicting condition patterns.
func isContradictoryCondition(p, c string) bool {
	p = strings.TrimSpace(p)
	c = strings.TrimSpace(c)

	// Negation pattern: "condition" vs "!condition" or "!(condition)"
	if c == "!"+p || c == "!("+p+")" || p == "!"+c || p == "!("+c+")" {
		return true
	}

	// Basic inequality contradiction pattern (e.g., state.score > 80 vs state.score < 50)
	if strings.Contains(p, ">") && strings.Contains(c, "<") {
		pParts := strings.Split(p, ">")
		cParts := strings.Split(c, "<")
		if len(pParts) == 2 && len(cParts) == 2 {
			pVar := strings.TrimSpace(pParts[0])
			cVar := strings.TrimSpace(cParts[0])
			if pVar == cVar && pVar != "" {
				var pNum, cNum float64
				if _, err1 := fmt.Sscanf(strings.TrimSpace(pParts[1]), "%f", &pNum); err1 == nil {
					if _, err2 := fmt.Sscanf(strings.TrimSpace(cParts[1]), "%f", &cNum); err2 == nil {
						if pNum >= cNum {
							return true
						}
					}
				}
			}
		}
	}

	return false
}
