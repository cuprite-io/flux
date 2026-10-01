package autopilot

import (
	"fmt"
	"strings"

	"github.com/cuprite-io/flux/internal/declarative"
	"github.com/cuprite-io/flux/internal/linter"
	"github.com/cuprite-io/flux/internal/sink"
	"github.com/cuprite-io/flux/types"
)

// ValidationError represents a specific static analysis, parse, or compilation failure.
type ValidationError struct {
	Stage   string `json:"stage"`             // "PARSE", "LINT", "COMPILE", "SINK"
	RuleID  string `json:"rule_id,omitempty"` // Diagnostic rule identifier (e.g. "LINT-022", "LINT-051")
	Node    string `json:"node,omitempty"`    // Node name context if applicable
	Message string `json:"message"`           // Primary error description
	Detail  string `json:"detail,omitempty"`  // Additional context or diagnostic
}

func (e ValidationError) String() string {
	nodeCtx := ""
	if e.Node != "" {
		nodeCtx = fmt.Sprintf(" in node %q", e.Node)
	}
	detail := ""
	if e.Detail != "" {
		detail = fmt.Sprintf(" (%s)", e.Detail)
	}
	rule := e.Stage
	if e.RuleID != "" {
		rule = fmt.Sprintf("%s:%s", e.Stage, e.RuleID)
	}
	return fmt.Sprintf("[%s]%s: %s%s", rule, nodeCtx, e.Message, detail)
}

// ValidationResult summarizes the outcome of checking a candidate Circuit DAG.
type ValidationResult struct {
	Circuit   *types.Circuit    `json:"-"`
	CleanJSON string            `json:"clean_json,omitempty"`
	Errors    []ValidationError `json:"errors,omitempty"`
	Warnings  []string          `json:"warnings,omitempty"`
	Valid     bool              `json:"valid"`
}

// FormatErrors returns a human-readable list of all validation errors suitable for LLM reflection.
func (r *ValidationResult) FormatErrors() string {
	if len(r.Errors) == 0 {
		return ""
	}
	var sb strings.Builder
	for _, err := range r.Errors {
		sb.WriteString("- ")
		sb.WriteString(err.String())
		sb.WriteString("\n")
	}
	return strings.TrimSpace(sb.String())
}

// CircuitValidator runs multi-stage verification on candidate Circuit JSON using the
// canonical declarative parser and linter subsystems with zero code duplication.
type CircuitValidator struct {
	linterOpts linter.Options
}

// NewCircuitValidator creates a new CircuitValidator.
func NewCircuitValidator() (*CircuitValidator, error) {
	return &CircuitValidator{
		linterOpts: linter.DefaultOptions(),
	}, nil
}

// Validate executes the canonical parsing and static analysis pipeline on raw Circuit JSON.
func (v *CircuitValidator) Validate(rawJSON string, allowedSinks []sink.Descriptor) *ValidationResult {
	res := &ValidationResult{Valid: false}

	// Stage 1: Clean & Extract JSON
	cleanJSON, err := CleanJSON(rawJSON)
	if err != nil {
		res.Errors = append(res.Errors, ValidationError{
			Stage:   "PARSE",
			Message: fmt.Sprintf("Failed to extract JSON: %v", err),
		})
		return res
	}
	res.CleanJSON = cleanJSON

	// Stage 2: Parse Circuit Tree via canonical declarative parser
	circuit, err := declarative.LoadCircuitJSON([]byte(cleanJSON))
	if err != nil {
		res.Errors = append(res.Errors, ValidationError{
			Stage:   "PARSE",
			Message: fmt.Sprintf("Invalid Circuit structure: %v", err),
		})
		return res
	}
	res.Circuit = circuit

	// Stage 3: Deep Static Analysis, AST Compilation & Sink Integrity via internal/linter
	opts := v.linterOpts
	if len(allowedSinks) > 0 {
		opts.AllowedSinks = make([]string, 0, len(allowedSinks))
		for _, s := range allowedSinks {
			opts.AllowedSinks = append(opts.AllowedSinks, s.Name)
		}
	}

	lnt, err := linter.New(opts)
	if err != nil {
		res.Errors = append(res.Errors, ValidationError{
			Stage:   "LINT",
			Message: fmt.Sprintf("Failed to initialize circuit linter: %v", err),
		})
		return res
	}

	lintResult := lnt.Lint(circuit)
	for _, iss := range lintResult.Issues {
		stage := "LINT"
		if strings.HasPrefix(iss.RuleID, "LINT-022") || strings.HasPrefix(iss.RuleID, "LINT-040") || strings.HasPrefix(iss.RuleID, "LINT-044") || strings.HasPrefix(iss.RuleID, "LINT-045") {
			stage = "COMPILE"
		} else if strings.HasPrefix(iss.RuleID, "LINT-050") || strings.HasPrefix(iss.RuleID, "LINT-051") {
			stage = "SINK"
		}

		if iss.Severity == linter.SeverityError {
			res.Errors = append(res.Errors, ValidationError{
				Stage:   stage,
				RuleID:  iss.RuleID,
				Node:    iss.Node,
				Message: iss.Message,
				Detail:  iss.Detail,
			})
		} else if iss.Severity == linter.SeverityWarning {
			res.Warnings = append(res.Warnings, iss.String())
		}
	}

	res.Valid = len(res.Errors) == 0
	return res
}
