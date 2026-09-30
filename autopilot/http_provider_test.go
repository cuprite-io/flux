package autopilot

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewOpenAICompatible_Validation(t *testing.T) {
	_, err := NewOpenAICompatible(HTTPConfig{})
	assert.ErrorContains(t, err, "endpoint cannot be empty")

	_, err = NewOpenAICompatible(HTTPConfig{Endpoint: "http://localhost"})
	assert.ErrorContains(t, err, "model cannot be empty")

	p, err := NewOpenAICompatible(HTTPConfig{
		Endpoint: "http://localhost:8080",
		Model:    "test-model",
	})
	require.NoError(t, err)
	assert.NotNil(t, p)
	assert.Equal(t, defaultTimeout, p.cfg.Timeout)
	assert.NotNil(t, p.client)
}

func TestConvenienceConstructors(t *testing.T) {
	t.Run("NewOpenAI", func(t *testing.T) {
		p, err := NewOpenAI("sk-test", "gpt-4o",
			WithTimeout(10*time.Second),
			WithTemperature(0.2),
			WithHeader("X-Custom", "val"),
		)
		require.NoError(t, err)
		assert.Equal(t, openAIEndpoint, p.cfg.Endpoint)
		assert.Equal(t, "gpt-4o", p.cfg.Model)
		assert.Equal(t, "sk-test", p.cfg.APIKey)
		assert.Equal(t, 10*time.Second, p.cfg.Timeout)
		require.NotNil(t, p.cfg.Temperature)
		assert.Equal(t, 0.2, *p.cfg.Temperature)
		assert.Equal(t, "val", p.cfg.Headers["X-Custom"])
	})

	t.Run("NewOpenRouter", func(t *testing.T) {
		p, err := NewOpenRouter("sk-or-test", "anthropic/claude-3.7-sonnet")
		require.NoError(t, err)
		assert.Equal(t, openRouterEndpoint, p.cfg.Endpoint)
		assert.Equal(t, "anthropic/claude-3.7-sonnet", p.cfg.Model)
		assert.Equal(t, "sk-or-test", p.cfg.APIKey)
	})

	t.Run("NewOllama default host", func(t *testing.T) {
		p, err := NewOllama("", "qwen2.5-coder")
		require.NoError(t, err)
		assert.Equal(t, "http://localhost:11434/v1/chat/completions", p.cfg.Endpoint)
		assert.Equal(t, "qwen2.5-coder", p.cfg.Model)
	})

	t.Run("NewOllama custom host with formatting", func(t *testing.T) {
		p, err := NewOllama("http://192.168.1.100:11434/", "llama3")
		require.NoError(t, err)
		assert.Equal(t, "http://192.168.1.100:11434/v1/chat/completions", p.cfg.Endpoint)

		// already formatted
		p2, err := NewOllama("http://192.168.1.100:11434/v1/chat/completions", "llama3")
		require.NoError(t, err)
		assert.Equal(t, "http://192.168.1.100:11434/v1/chat/completions", p2.cfg.Endpoint)
	})

	t.Run("NewGemini", func(t *testing.T) {
		p, err := NewGemini("gemini-key", "gemini-2.5-pro")
		require.NoError(t, err)
		assert.Equal(t, geminiEndpoint, p.cfg.Endpoint)
		assert.Equal(t, "gemini-2.5-pro", p.cfg.Model)
		assert.Equal(t, "gemini-key", p.cfg.APIKey)
	})

	t.Run("WithHTTPClient", func(t *testing.T) {
		customClient := &http.Client{Timeout: 5 * time.Second}
		p, err := NewOpenAI("key", "model", WithHTTPClient(customClient))
		require.NoError(t, err)
		assert.Same(t, customClient, p.client)
	})
}

func TestHTTPProvider_Generate(t *testing.T) {
	t.Run("empty messages error", func(t *testing.T) {
		p, err := NewOpenAICompatible(HTTPConfig{
			Endpoint: "http://localhost",
			Model:    "test",
		})
		require.NoError(t, err)

		_, err = p.Generate(context.Background(), nil)
		assert.ErrorContains(t, err, "at least one message is required")
	})

	t.Run("successful generation with headers, temperature, and multi-turn", func(t *testing.T) {
		var capturedReq chatRequest
		var capturedAuth string
		var capturedCustomHeader string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			capturedAuth = r.Header.Get("Authorization")
			capturedCustomHeader = r.Header.Get("X-Test-Trace")
			assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
			assert.Equal(t, "flux-autopilot", r.Header.Get("User-Agent"))

			body, err := io.ReadAll(r.Body)
			assert.NoError(t, err)

			err = json.Unmarshal(body, &capturedReq)
			assert.NoError(t, err)

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{
				"choices": [
					{
						"index": 0,
						"message": {
							"role": "assistant",
							"content": "{\"circuit\": \"synthesized\"}"
						},
						"finish_reason": "stop"
					}
				]
			}`))
		}))
		defer server.Close()

		temp := 0.0
		provider, err := NewOpenAICompatible(HTTPConfig{
			Endpoint:    server.URL,
			Model:       "deepseek-coder",
			APIKey:      "secret-token-123",
			Temperature: &temp,
			Headers: map[string]string{
				"X-Test-Trace": "req-987",
			},
		})
		require.NoError(t, err)

		messages := []Message{
			SystemMessage("system instructions"),
			UserMessage("first user prompt"),
			AssistantMessage("previous response"),
			UserMessage("compilation error: please fix"),
		}

		reply, err := provider.Generate(context.Background(), messages)
		require.NoError(t, err)
		assert.Equal(t, `{"circuit": "synthesized"}`, reply)

		assert.Equal(t, "Bearer secret-token-123", capturedAuth)
		assert.Equal(t, "req-987", capturedCustomHeader)
		assert.Equal(t, "deepseek-coder", capturedReq.Model)
		require.NotNil(t, capturedReq.Temperature)
		assert.Equal(t, 0.0, *capturedReq.Temperature)
		assert.Equal(t, messages, capturedReq.Messages)
	})

	t.Run("API error with OpenAI JSON error payload", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{
				"error": {
					"message": "Incorrect API key provided: sk-invalid",
					"type": "invalid_request_error",
					"code": "invalid_api_key"
				}
			}`))
		}))
		defer server.Close()

		provider, err := NewOpenAICompatible(HTTPConfig{
			Endpoint: server.URL,
			Model:    "gpt-4o",
			APIKey:   "sk-invalid",
		})
		require.NoError(t, err)

		_, err = provider.Generate(context.Background(), []Message{UserMessage("test")})
		require.Error(t, err)
		assert.ErrorContains(t, err, "status 401")
		assert.ErrorContains(t, err, "Incorrect API key provided")
	})

	t.Run("API error with non-JSON plain text body", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte("502 Bad Gateway: upstream proxy timeout"))
		}))
		defer server.Close()

		provider, err := NewOpenAICompatible(HTTPConfig{
			Endpoint: server.URL,
			Model:    "gpt-4o",
		})
		require.NoError(t, err)

		_, err = provider.Generate(context.Background(), []Message{UserMessage("test")})
		require.Error(t, err)
		assert.ErrorContains(t, err, "status 502")
		assert.ErrorContains(t, err, "502 Bad Gateway")
	})

	t.Run("empty choices in 200 OK response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"choices": []}`))
		}))
		defer server.Close()

		provider, err := NewOpenAICompatible(HTTPConfig{
			Endpoint: server.URL,
			Model:    "gpt-4o",
		})
		require.NoError(t, err)

		_, err = provider.Generate(context.Background(), []Message{UserMessage("test")})
		require.Error(t, err)
		assert.ErrorContains(t, err, "no choices in response")
	})

	t.Run("malformed JSON in 200 OK response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"choices": [{ broken json`))
		}))
		defer server.Close()

		provider, err := NewOpenAICompatible(HTTPConfig{
			Endpoint: server.URL,
			Model:    "gpt-4o",
		})
		require.NoError(t, err)

		_, err = provider.Generate(context.Background(), []Message{UserMessage("test")})
		require.Error(t, err)
		assert.ErrorContains(t, err, "failed to decode response JSON")
	})

	t.Run("context cancelled", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(100 * time.Millisecond)
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		provider, err := NewOpenAICompatible(HTTPConfig{
			Endpoint: server.URL,
			Model:    "gpt-4o",
		})
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()

		_, err = provider.Generate(ctx, []Message{UserMessage("test")})
		require.Error(t, err)
		assert.ErrorContains(t, err, "request failed")
	})
}
