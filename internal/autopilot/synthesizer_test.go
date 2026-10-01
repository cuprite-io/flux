package autopilot

import (
	"context"
	"errors"
	"testing"

	"github.com/cuprite-io/flux/autopilot"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCleanJSON(t *testing.T) {
	t.Run("empty string error", func(t *testing.T) {
		_, err := CleanJSON("")
		assert.ErrorContains(t, err, "empty response")
	})

	t.Run("clean raw JSON object", func(t *testing.T) {
		raw := `{"id": "test_circuit", "tags": ["stream:logs"]}`
		cleaned, err := CleanJSON(raw)
		require.NoError(t, err)
		assert.Equal(t, raw, cleaned)
	})

	t.Run("markdown json fences", func(t *testing.T) {
		raw := "```json\n{\n  \"id\": \"test_circuit\"\n}\n```"
		cleaned, err := CleanJSON(raw)
		require.NoError(t, err)
		assert.JSONEq(t, `{"id": "test_circuit"}`, cleaned)
	})

	t.Run("markdown generic fences", func(t *testing.T) {
		raw := "```\n{\n  \"id\": \"test_circuit\"\n}\n```"
		cleaned, err := CleanJSON(raw)
		require.NoError(t, err)
		assert.JSONEq(t, `{"id": "test_circuit"}`, cleaned)
	})

	t.Run("conversational preamble and postscript", func(t *testing.T) {
		raw := `Here is the synthesized Flux circuit based on your specification:
{
  "id": "production_alert",
  "tags": ["stream:telemetry"],
  "root": {
    "name": "evaluator"
  }
}
Let me know if you would like me to adjust any conditions!`
		cleaned, err := CleanJSON(raw)
		require.NoError(t, err)
		assert.JSONEq(t, `{"id": "production_alert", "tags": ["stream:telemetry"], "root": {"name": "evaluator"}}`, cleaned)
	})

	t.Run("invalid non-JSON text", func(t *testing.T) {
		raw := "I cannot synthesize a circuit because the request is ambiguous."
		_, err := CleanJSON(raw)
		assert.ErrorContains(t, err, "no valid JSON object found")
	})

	t.Run("broken JSON syntax", func(t *testing.T) {
		raw := `{ "id": "test", broken }`
		_, err := CleanJSON(raw)
		assert.ErrorContains(t, err, "invalid JSON syntax")
	})
}

func TestSynthesizer_Synthesize(t *testing.T) {
	t.Run("nil provider error", func(t *testing.T) {
		_, err := NewSynthesizer(nil)
		assert.ErrorContains(t, err, "provider cannot be nil")
	})

	t.Run("empty prompt error", func(t *testing.T) {
		mock := autopilot.ProviderFunc(func(ctx context.Context, messages []autopilot.Message) (string, error) {
			return "{}", nil
		})
		synth, err := NewSynthesizer(mock)
		require.NoError(t, err)

		_, err = synth.Synthesize(context.Background(), SynthesisRequest{Prompt: "   "})
		assert.ErrorContains(t, err, "prompt cannot be empty")
	})

	t.Run("successful synthesis with message validation", func(t *testing.T) {
		var receivedMessages []autopilot.Message

		expectedCircuitJSON := `{
  "id": "alert_circuit",
  "tags": ["stream:logs"],
  "root": {
    "name": "check_errors",
    "condition": "payload.status_code >= 500",
    "steps": [
      {
        "type": "sink",
        "sink": "pagerduty",
        "payload": "payload"
      }
    ]
  }
}`

		mock := autopilot.ProviderFunc(func(ctx context.Context, messages []autopilot.Message) (string, error) {
			receivedMessages = messages
			// Return with markdown fence
			return "```json\n" + expectedCircuitJSON + "\n```", nil
		})

		synth, err := NewSynthesizer(mock)
		require.NoError(t, err)

		req := SynthesisRequest{
			Prompt:    "Alert pagerduty if status_code >= 500",
			Tags:      []string{"stream:logs"},
			CircuitID: "alert_circuit",
		}

		result, err := synth.Synthesize(context.Background(), req)
		require.NoError(t, err)
		assert.JSONEq(t, expectedCircuitJSON, result)

		require.Len(t, receivedMessages, 2)
		assert.Equal(t, autopilot.RoleSystem, receivedMessages[0].Role)
		assert.Contains(t, receivedMessages[0].Content, "FLUX CIRCUIT SCHEMA SPECIFICATION")

		assert.Equal(t, autopilot.RoleUser, receivedMessages[1].Role)
		assert.Contains(t, receivedMessages[1].Content, "Alert pagerduty if status_code >= 500")
		assert.Contains(t, receivedMessages[1].Content, "- stream:logs")
	})

	t.Run("provider returns error", func(t *testing.T) {
		mock := autopilot.ProviderFunc(func(ctx context.Context, messages []autopilot.Message) (string, error) {
			return "", errors.New("rate limited by upstream")
		})

		synth, err := NewSynthesizer(mock)
		require.NoError(t, err)

		_, err = synth.Synthesize(context.Background(), SynthesisRequest{Prompt: "some prompt"})
		assert.ErrorContains(t, err, "AI generation failed")
		assert.ErrorContains(t, err, "rate limited by upstream")
	})
}
