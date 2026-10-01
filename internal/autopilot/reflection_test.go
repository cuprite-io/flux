package autopilot

import (
	"context"
	"testing"

	"github.com/cuprite-io/flux/autopilot"
	"github.com/cuprite-io/flux/internal/sink"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSynthesizer_SynthesizeWithReflection(t *testing.T) {
	allowedSinks := []sink.Descriptor{
		{Name: "pagerduty", Severity: sink.SeverityCritical},
		{Name: "slack_alerts", Severity: sink.SeverityWarning},
	}

	t.Run("succeeds on first attempt without reflection", func(t *testing.T) {
		validCircuit := `{
			"id": "circuit_turn1",
			"tags": ["stream:logs"],
			"root": {
				"name": "check",
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

		provider := autopilot.ProviderFunc(func(ctx context.Context, messages []autopilot.Message) (string, error) {
			return validCircuit, nil
		})

		synth, err := NewSynthesizer(provider)
		require.NoError(t, err)

		req := SynthesisRequest{
			Prompt: "Alert on 500s",
			Tags:   []string{"stream:logs"},
			Sinks:  allowedSinks,
		}

		res, err := synth.SynthesizeWithReflection(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, 1, res.Attempts)
		assert.Empty(t, res.CorrectionHistory)
		assert.Equal(t, "circuit_turn1", res.Circuit.ID)
	})

	t.Run("self-corrects on second attempt after syntax and sink error", func(t *testing.T) {
		attemptCounter := 0
		var capturedFeedback string

		// Attempt 1: Has unknown sink "unregistered_discord" and broken CEL condition "payload.status = 500"
		turn1Invalid := `{
			"id": "broken_turn1",
			"tags": ["stream:logs"],
			"root": {
				"name": "check",
				"condition": "payload.status = 500",
				"steps": [
					{
						"type": "sink",
						"sink": "unregistered_discord",
						"payload": "payload"
					}
				]
			}
		}`

		// Attempt 2: Fixed sink and condition
		turn2Valid := `{
			"id": "fixed_turn2",
			"tags": ["stream:logs"],
			"root": {
				"name": "check",
				"condition": "payload.status == 500",
				"steps": [
					{
						"type": "sink",
						"sink": "pagerduty",
						"payload": "payload"
					}
				]
			}
		}`

		provider := autopilot.ProviderFunc(func(ctx context.Context, messages []autopilot.Message) (string, error) {
			attemptCounter++
			if attemptCounter == 1 {
				return turn1Invalid, nil
			}
			// On attempt 2, verify reflection message was received
			lastMsg := messages[len(messages)-1]
			capturedFeedback = lastMsg.Content
			return turn2Valid, nil
		})

		synth, err := NewSynthesizer(provider)
		require.NoError(t, err)

		req := SynthesisRequest{
			Prompt: "Alert on 500s",
			Tags:   []string{"stream:logs"},
			Sinks:  allowedSinks,
		}

		res, err := synth.SynthesizeWithReflection(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, 2, res.Attempts)
		require.Len(t, res.CorrectionHistory, 1)
		assert.Equal(t, 1, res.CorrectionHistory[0].Attempt)
		assert.NotEmpty(t, res.CorrectionHistory[0].Errors)
		assert.Equal(t, "fixed_turn2", res.Circuit.ID)

		// Verify feedback prompt contents
		assert.Contains(t, capturedFeedback, "failed compiler and guardrail verification")
		assert.Contains(t, capturedFeedback, "unregistered_discord")
		assert.Contains(t, capturedFeedback, "pagerduty, slack_alerts")
		assert.Contains(t, capturedFeedback, "payload.status = 500")
	})

	t.Run("exhausts max retries and returns descriptive error", func(t *testing.T) {
		alwaysBroken := `{
			"id": "permanently_broken",
			"root": {
				"name": "check",
				"steps": [
					{
						"type": "volt",
						"script": "set('x', 1 + )"
					}
				]
			}
		}`

		callCount := 0
		provider := autopilot.ProviderFunc(func(ctx context.Context, messages []autopilot.Message) (string, error) {
			callCount++
			return alwaysBroken, nil
		})

		synth, err := NewSynthesizer(provider, WithMaxRetries(3))
		require.NoError(t, err)

		req := SynthesisRequest{
			Prompt: "Alert on something",
			Sinks:  allowedSinks,
		}

		_, err = synth.SynthesizeWithReflection(context.Background(), req)
		require.Error(t, err)
		assert.Equal(t, 3, callCount)
		assert.Contains(t, err.Error(), "failed to synthesize valid circuit after 3 attempts")
		assert.Contains(t, err.Error(), "expression syntax error")
	})

	t.Run("respects context cancellation during reflection", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		callCount := 0

		provider := autopilot.ProviderFunc(func(c context.Context, messages []autopilot.Message) (string, error) {
			callCount++
			if callCount >= 2 {
				cancel()
			}
			return `{"id": "bad", "root": {"name": "n", "steps": [{"type": "bad_type"}]}}`, nil
		})

		synth, err := NewSynthesizer(provider, WithMaxRetries(5))
		require.NoError(t, err)

		req := SynthesisRequest{
			Prompt: "Alert",
			Sinks:  allowedSinks,
		}

		_, err = synth.SynthesizeWithReflection(ctx, req)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "synthesis context canceled")
	})
}
