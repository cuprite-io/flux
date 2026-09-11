package linter

import (
	"testing"

	"github.com/cuprite-io/flux/types"
)

func TestLinter_ValidCircuit(t *testing.T) {
	c := &types.Circuit{
		ID: "valid_circuit_01",
		Root: &types.Node{
			Name: "root_node",
			Steps: []*types.StepDefinition{
				{
					Type:   types.StepVolt,
					Script: "set('x', payload.amount * 2.0)",
				},
				{
					Type:     types.StepSink,
					SinkName: "alert_sink",
				},
			},
			Children: []*types.Node{
				{
					Name:      "child_branch",
					Condition: "state.x > 100.0",
					Steps: []*types.StepDefinition{
						{
							Type: types.StepReturn,
							ReturnMap: map[string]any{
								"result": "$x",
							},
						},
					},
				},
			},
		},
	}

	l, err := New(DefaultOptions())
	if err != nil {
		t.Fatalf("failed to create linter: %v", err)
	}

	res := l.Lint(c)
	if res.HasErrors() {
		t.Fatalf("expected 0 errors, got: %v", res.Issues)
	}
	if len(res.Issues) != 0 {
		t.Logf("informational/warning issues: %v", res.Issues)
	}
}

func TestLinter_DuplicateNodeNames(t *testing.T) {
	c := &types.Circuit{
		ID: "duplicate_nodes",
		Root: &types.Node{
			Name: "my_node",
			Children: []*types.Node{
				{
					Name: "my_node", // Duplicate!
				},
			},
		},
	}

	l, err := New(DefaultOptions())
	if err != nil {
		t.Fatalf("failed to create linter: %v", err)
	}

	res := l.Lint(c)
	if !res.HasErrors() {
		t.Fatalf("expected duplicate node name error, got none")
	}

	found := false
	for _, iss := range res.Issues {
		if iss.RuleID == "LINT-011" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected LINT-011 issue, got: %v", res.Issues)
	}
}

func TestLinter_DeadBranch_LiteralFalse(t *testing.T) {
	c := &types.Circuit{
		ID: "dead_branch",
		Root: &types.Node{
			Name: "root",
			Children: []*types.Node{
				{
					Name:      "dead_node",
					Condition: "false",
				},
			},
		},
	}

	l, err := New(DefaultOptions())
	if err != nil {
		t.Fatalf("failed to create linter: %v", err)
	}

	res := l.Lint(c)
	found := false
	for _, iss := range res.Issues {
		if iss.RuleID == "LINT-020" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected LINT-020 issue for literal false, got: %v", res.Issues)
	}
}

func TestLinter_MissingSinkName(t *testing.T) {
	c := &types.Circuit{
		ID: "missing_sink",
		Root: &types.Node{
			Name: "root",
			Steps: []*types.StepDefinition{
				{
					Type:     types.StepSink,
					SinkName: "", // Empty!
				},
			},
		},
	}

	l, err := New(DefaultOptions())
	if err != nil {
		t.Fatalf("failed to create linter: %v", err)
	}

	res := l.Lint(c)
	found := false
	for _, iss := range res.Issues {
		if iss.RuleID == "LINT-050" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected LINT-050 issue for empty sink name, got: %v", res.Issues)
	}
}

func TestLinter_UndeclaredVariableInReturn(t *testing.T) {
	c := &types.Circuit{
		ID: "undeclared_var",
		Root: &types.Node{
			Name: "root",
			Steps: []*types.StepDefinition{
				{
					Type: types.StepReturn,
					ReturnMap: map[string]any{
						"bad_ref": "$unproduced_secret_key",
					},
				},
			},
		},
	}

	l, err := New(DefaultOptions())
	if err != nil {
		t.Fatalf("failed to create linter: %v", err)
	}

	res := l.Lint(c)
	found := false
	for _, iss := range res.Issues {
		if iss.RuleID == "LINT-061" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected LINT-061 issue for undeclared variable, got: %v", res.Issues)
	}
}

func TestLinter_ContradictoryConditions(t *testing.T) {
	c := &types.Circuit{
		ID: "contradiction",
		Root: &types.Node{
			Name:      "parent_node",
			Condition: "payload.amount > 100",
			Children: []*types.Node{
				{
					Name:      "child_node",
					Condition: "payload.amount < 50", // Impossible if parent passed!
				},
			},
		},
	}

	l, err := New(DefaultOptions())
	if err != nil {
		t.Fatalf("failed to create linter: %v", err)
	}

	res := l.Lint(c)
	found := false
	for _, iss := range res.Issues {
		if iss.RuleID == "LINT-023" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected LINT-023 issue for contradictory conditions, got: %v", res.Issues)
	}
}

func BenchmarkLinter_LintDAG(b *testing.B) {
	c := &types.Circuit{
		ID: "bench_circuit",
		Root: &types.Node{
			Name: "root_node",
			Steps: []*types.StepDefinition{
				{
					Type:   types.StepVolt,
					Script: "set('masked_email', mask.email(payload.email)) && set('recent_fails', window.count('auth:failed:' + payload.user_id, '5m'))",
				},
				{
					Type:     types.StepSink,
					SinkName: "user_security_sink",
				},
			},
			Children: []*types.Node{
				{
					Name:      "critical_threat_branch",
					Condition: "state.risk_score >= 80.0",
					Steps: []*types.StepDefinition{
						{
							Type: types.StepReturn,
							ReturnMap: map[string]any{
								"status": "CHALLENGE_REQUIRED",
								"email":  "$masked_email",
							},
						},
					},
				},
				{
					Name:      "low_risk_branch",
					Condition: "state.risk_score < 40.0",
					Steps: []*types.StepDefinition{
						{
							Type: types.StepReturn,
							ReturnMap: map[string]any{
								"status": "PASSED",
								"email":  "$masked_email",
							},
						},
					},
				},
			},
		},
	}

	l, err := New(DefaultOptions())
	if err != nil {
		b.Fatalf("failed to create linter: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_ = l.Lint(c)
	}
}
