package autopilot

import (
	"testing"
	"time"

	"github.com/cuprite-io/assay"
	"github.com/cuprite-io/flux/internal/profiler"
	"github.com/cuprite-io/flux/internal/sink"
	"github.com/stretchr/testify/assert"
)

func TestPromptBuilder_BuildSystemPrompt(t *testing.T) {
	b := NewPromptBuilder()
	sys := b.BuildSystemPrompt()

	assert.Contains(t, sys, "FLUX CIRCUIT SCHEMA SPECIFICATION")
	assert.Contains(t, sys, "set('var_name'")
	assert.Contains(t, sys, "window.count")
	assert.Contains(t, sys, "is_pii")
	assert.Contains(t, sys, "mask.email")
	assert.Contains(t, sys, "crypto.sha256")
	assert.Contains(t, sys, "crypto.encrypt")
	assert.Contains(t, sys, "geo.dist_km")
	assert.Contains(t, sys, "ml.anomaly")
	assert.Contains(t, sys, "math.stats")
	assert.Contains(t, sys, "uuid()")
	assert.Contains(t, sys, "CRITICAL REQUIREMENTS & CONSTRAINTS")
}

func TestPromptBuilder_BuildUserPrompt_EmptyContext(t *testing.T) {
	b := NewPromptBuilder()
	userPrompt := b.BuildUserPrompt("Alert if error rate is high", nil, nil, nil, nil, "")

	assert.Contains(t, userPrompt, "### USER OBJECTIVE")
	assert.Contains(t, userPrompt, "Alert if error rate is high")
	assert.Contains(t, userPrompt, "None specified (use default 'stream:default')")
	assert.Contains(t, userPrompt, "No schema observed yet for these tags")
	assert.Contains(t, userPrompt, "No value profile recorded yet")
	assert.Contains(t, userPrompt, "No external sinks registered")
}

func TestPromptBuilder_BuildUserPrompt_RichContext(t *testing.T) {
	b := NewPromptBuilder()

	// 1. Schemas
	schemas := map[string]*assay.SchemaNode{
		"stream:logs:ERROR": {
			Name: "root",
			Path: "payload",
			Type: "object",
			Children: map[string]*assay.SchemaNode{
				"level": {
					Name:     "level",
					Path:     "payload.level",
					Type:     "string",
					Required: true,
				},
				"error_code": {
					Name:     "error_code",
					Path:     "payload.error_code",
					Type:     "number",
					Required: true,
				},
				"metadata": {
					Name: "metadata",
					Path: "payload.metadata",
					Type: "object",
					Children: map[string]*assay.SchemaNode{
						"trace_id": {
							Name:     "trace_id",
							Path:     "payload.metadata.trace_id",
							Type:     "string",
							Required: false,
						},
					},
				},
			},
		},
	}

	// 2. Profiles
	profiles := map[string]*profiler.StreamProfile{
		"stream:logs": {
			Tag:          "stream:logs",
			TotalSampled: 1500,
			Categoricals: map[string]map[string]uint64{
				"level": {
					"ERROR": 50,
					"WARN":  120,
					"INFO":  1330,
				},
			},
			Ranges: map[string]profiler.NumberRange{
				"error_code": {
					Min:   500,
					Max:   503,
					Count: 50,
				},
			},
			Exemplars: []profiler.Exemplar{
				{
					Fingerprint: "fp1",
					Timestamp:   time.Now(),
					Payload: map[string]any{
						"level":      "ERROR",
						"error_code": 500,
						"service":    "auth",
					},
				},
			},
		},
	}

	// 3. Sinks
	sinks := []sink.Descriptor{
		{
			Name:        "ops_pagerduty",
			Description: "Paging primary on-call for outages",
			Severity:    sink.SeverityCritical,
			SinkType:    "webhook",
		},
		{
			Name:        "slack_alerts",
			Description: "Team channel alert for warnings",
			Severity:    sink.SeverityWarning,
			SinkType:    "webhook",
		},
	}

	prompt := b.BuildUserPrompt(
		"Page on-call when 500 errors occur more than 5 times in 60s",
		[]string{"stream:logs"},
		schemas,
		profiles,
		sinks,
		"circuit_log_monitor",
	)

	// Assertions
	assert.Contains(t, prompt, "Page on-call when 500 errors occur more than 5 times in 60s")
	assert.Contains(t, prompt, "- stream:logs")
	assert.Contains(t, prompt, `Please use the ID: "circuit_log_monitor"`)

	// Schema checks
	assert.Contains(t, prompt, "Tag / Variant: stream:logs:ERROR")
	assert.Contains(t, prompt, "payload.level: string (required)")
	assert.Contains(t, prompt, "payload.error_code: number (required)")
	assert.Contains(t, prompt, "payload.metadata.trace_id: string (optional)")

	// Profile checks
	assert.Contains(t, prompt, "Profile for Tag: stream:logs (Total Events Sampled: 1500)")
	assert.Contains(t, prompt, "payload.level: [\"ERROR\": 50, \"INFO\": 1330, \"WARN\": 120]")
	assert.Contains(t, prompt, "payload.error_code: min=500.00, max=503.00 (samples: 50)")
	assert.Contains(t, prompt, `"service":"auth"`)

	// Sinks checks
	assert.Contains(t, prompt, `Sink Name: "ops_pagerduty"`)
	assert.Contains(t, prompt, "Severity: critical")
	assert.Contains(t, prompt, `Sink Name: "slack_alerts"`)
	assert.Contains(t, prompt, "Severity: warning")
}
