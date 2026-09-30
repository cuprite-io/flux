package autopilot

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRoleConstantsAndConstructors(t *testing.T) {
	assert.Equal(t, Role("system"), RoleSystem)
	assert.Equal(t, Role("user"), RoleUser)
	assert.Equal(t, Role("assistant"), RoleAssistant)

	userMsg := UserMessage("hello user")
	assert.Equal(t, RoleUser, userMsg.Role)
	assert.Equal(t, "hello user", userMsg.Content)

	sysMsg := SystemMessage("hello system")
	assert.Equal(t, RoleSystem, sysMsg.Role)
	assert.Equal(t, "hello system", sysMsg.Content)

	asstMsg := AssistantMessage("hello assistant")
	assert.Equal(t, RoleAssistant, asstMsg.Role)
	assert.Equal(t, "hello assistant", asstMsg.Content)
}

func TestProviderFunc(t *testing.T) {
	called := false
	var capturedMessages []Message

	mock := ProviderFunc(func(ctx context.Context, messages []Message) (string, error) {
		called = true
		capturedMessages = messages
		return "mocked response", nil
	})

	ctx := context.Background()
	msgs := []Message{
		SystemMessage("act as an assistant"),
		UserMessage("generate circuit"),
	}

	resp, err := mock.Generate(ctx, msgs)
	require.NoError(t, err)
	assert.True(t, called)
	assert.Equal(t, "mocked response", resp)
	assert.Equal(t, msgs, capturedMessages)
}

func TestGeneratePrompt(t *testing.T) {
	t.Run("nil provider error", func(t *testing.T) {
		_, err := GeneratePrompt(context.Background(), nil, "sys", "usr")
		assert.ErrorContains(t, err, "provider cannot be nil")
	})

	t.Run("system and user prompts", func(t *testing.T) {
		var received []Message
		p := ProviderFunc(func(ctx context.Context, messages []Message) (string, error) {
			received = messages
			return "ok", nil
		})

		resp, err := GeneratePrompt(context.Background(), p, "sys prompt", "usr prompt")
		require.NoError(t, err)
		assert.Equal(t, "ok", resp)
		require.Len(t, received, 2)
		assert.Equal(t, SystemMessage("sys prompt"), received[0])
		assert.Equal(t, UserMessage("usr prompt"), received[1])
	})

	t.Run("user prompt only", func(t *testing.T) {
		var received []Message
		p := ProviderFunc(func(ctx context.Context, messages []Message) (string, error) {
			received = messages
			return "ok", nil
		})

		resp, err := GeneratePrompt(context.Background(), p, "", "usr prompt")
		require.NoError(t, err)
		assert.Equal(t, "ok", resp)
		require.Len(t, received, 1)
		assert.Equal(t, UserMessage("usr prompt"), received[0])
	})

	t.Run("provider returns error", func(t *testing.T) {
		p := ProviderFunc(func(ctx context.Context, messages []Message) (string, error) {
			return "", errors.New("upstream failed")
		})

		_, err := GeneratePrompt(context.Background(), p, "sys", "usr")
		assert.ErrorContains(t, err, "upstream failed")
	})
}
