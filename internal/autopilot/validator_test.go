package autopilot

import (
	"strings"
	"testing"

	"github.com/cuprite-io/flux/internal/declarative"
	"github.com/cuprite-io/flux/internal/sink"
	"github.com/cuprite-io/flux/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseCircuitJSON(t *testing.T) {
	t.Run("valid complete circuit", func(t *testing.T) {
		raw := `{
			"id": "c1",
			"tags": ["stream:a", "stream:b"],
			"root": {
				"name": "root_node",
				"condition": "payload.count > 0",
				"steps": [
					{
						"type": "volt",
						"script": "set('x', 10)"
					},
					{
						"type": "sink",
						"sink": "alerts",
						"payload": "payload"
					},
					{
						"type": "return",
						"data": {"status": "OK"}
					}
				],
				"children": [
					{
						"name": "child_node",
						"steps": [
							{"type": "abort"}
						]
					}
				]
			}
		}`

		circuit, err := declarative.LoadCircuitJSON([]byte(raw))
		require.NoError(t, err)
		assert.Equal(t, "c1", circuit.ID)
		assert.Equal(t, []string{"stream:a", "stream:b"}, circuit.Tags)
		assert.Equal(t, "root_node", circuit.Root.Name)
		assert.Equal(t, "payload.count > 0", circuit.Root.Condition)
		require.Len(t, circuit.Root.Steps, 3)
		assert.Equal(t, types.StepVolt, circuit.Root.Steps[0].Type)
		assert.Equal(t, types.StepSink, circuit.Root.Steps[1].Type)
		assert.Equal(t, types.StepReturn, circuit.Root.Steps[2].Type)
		require.Len(t, circuit.Root.Children, 1)
		assert.Equal(t, "child_node", circuit.Root.Children[0].Name)
		assert.Equal(t, types.StepAbort, circuit.Root.Children[0].Steps[0].Type)
	})

	t.Run("missing ID error", func(t *testing.T) {
		raw := `{"root": {"name": "test"}}`
		_, err := declarative.LoadCircuitJSON([]byte(raw))
		assert.ErrorContains(t, err, "circuit 'id' is required")
	})

	t.Run("missing root error", func(t *testing.T) {
		raw := `{"id": "test"}`
		_, err := declarative.LoadCircuitJSON([]byte(raw))
		assert.ErrorContains(t, err, "circuit 'root' node is required")
	})

	t.Run("node missing name error", func(t *testing.T) {
		raw := `{"id": "test", "root": {"steps": []}}`
		_, err := declarative.LoadCircuitJSON([]byte(raw))
		assert.ErrorContains(t, err, "node name cannot be empty")
	})

	t.Run("unknown step type error", func(t *testing.T) {
		raw := `{
			"id": "test",
			"root": {
				"name": "root",
				"steps": [{"type": "nonexistent"}]
			}
		}`
		_, err := declarative.LoadCircuitJSON([]byte(raw))
		assert.ErrorContains(t, err, "unknown step type")
	})
}

func TestCircuitValidator_Validate(t *testing.T) {
	val, err := NewCircuitValidator()
	require.NoError(t, err)

	allowedSinks := []sink.Descriptor{
		{Name: "pagerduty", Severity: sink.SeverityCritical},
		{Name: "slack_alerts", Severity: sink.SeverityWarning},
	}

	t.Run("valid circuit passes cleanly", func(t *testing.T) {
		raw := `{
			"id": "valid_circuit",
			"tags": ["stream:logs"],
			"root": {
				"name": "check_errors",
				"condition": "payload.status_code >= 500",
				"steps": [
					{
						"type": "volt",
						"script": "set('cnt', window.count('5xx', '60s'))"
					},
					{
						"type": "sink",
						"sink": "pagerduty",
						"payload": "payload"
					}
				]
			}
		}`

		res := val.Validate(raw, allowedSinks)
		assert.True(t, res.Valid)
		assert.Empty(t, res.Errors)
		assert.NotNil(t, res.Circuit)
		assert.NotEmpty(t, res.CleanJSON)
	})

	t.Run("invalid JSON parse error", func(t *testing.T) {
		res := val.Validate("not a json string at all", allowedSinks)
		assert.False(t, res.Valid)
		require.NotEmpty(t, res.Errors)
		assert.Equal(t, "PARSE", res.Errors[0].Stage)
	})

	t.Run("unregistered sink error", func(t *testing.T) {
		raw := `{
			"id": "invalid_sink_circuit",
			"root": {
				"name": "check",
				"steps": [
					{
						"type": "sink",
						"sink": "discord_webhook",
						"payload": "payload"
					}
				]
			}
		}`

		res := val.Validate(raw, allowedSinks)
		assert.False(t, res.Valid)
		require.NotEmpty(t, res.Errors)
		foundSinkErr := false
		for _, e := range res.Errors {
			if e.Stage == "SINK" {
				foundSinkErr = true
				assert.Contains(t, e.Message, "discord_webhook")
				assert.Contains(t, e.Message, "pagerduty, slack_alerts")
			}
		}
		assert.True(t, foundSinkErr)
	})

	t.Run("missing sink name on sink step", func(t *testing.T) {
		raw := `{
			"id": "empty_sink_circuit",
			"root": {
				"name": "check",
				"steps": [
					{
						"type": "sink",
						"payload": "payload"
					}
				]
			}
		}`

		res := val.Validate(raw, allowedSinks)
		assert.False(t, res.Valid)
		require.NotEmpty(t, res.Errors)
		foundSinkErr := false
		for _, e := range res.Errors {
			if e.Stage == "SINK" && strings.Contains(e.Message, "missing mandatory sink name") {
				foundSinkErr = true
			}
		}
		assert.True(t, foundSinkErr)
	})

	t.Run("invalid VoltScript compilation error", func(t *testing.T) {
		raw := `{
			"id": "bad_volt_circuit",
			"root": {
				"name": "transform",
				"steps": [
					{
						"type": "volt",
						"script": "set('x', 1 + )"
					}
				]
			}
		}`

		res := val.Validate(raw, allowedSinks)
		assert.False(t, res.Valid)
		require.NotEmpty(t, res.Errors)
		foundCompileErr := false
		for _, e := range res.Errors {
			if e.Stage == "COMPILE" {
				foundCompileErr = true
				assert.Equal(t, "transform", e.Node)
				assert.Contains(t, e.Message, "expression syntax error")
			}
		}
		assert.True(t, foundCompileErr)
	})

	t.Run("invalid node guard condition compilation error", func(t *testing.T) {
		raw := `{
			"id": "bad_condition_circuit",
			"root": {
				"name": "guard",
				"condition": "payload.count = 10",
				"steps": []
			}
		}`

		res := val.Validate(raw, allowedSinks)
		assert.False(t, res.Valid)
		require.NotEmpty(t, res.Errors)
		foundCompileErr := false
		for _, e := range res.Errors {
			if e.Stage == "COMPILE" {
				foundCompileErr = true
				assert.Equal(t, "guard", e.Node)
				assert.Contains(t, e.Message, "syntax error in node condition")
			}
		}
		assert.True(t, foundCompileErr)
	})

	t.Run("format errors output", func(t *testing.T) {
		res := &ValidationResult{
			Errors: []ValidationError{
				{Stage: "SINK", Node: "notify", Message: "unknown sink 'abc'"},
				{Stage: "COMPILE", Node: "filter", Message: "syntax error"},
			},
		}
		formatted := res.FormatErrors()
		assert.Contains(t, formatted, "- [SINK] in node \"notify\": unknown sink 'abc'")
		assert.Contains(t, formatted, "- [COMPILE] in node \"filter\": syntax error")
	})
}
