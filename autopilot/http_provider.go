package autopilot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	defaultTimeout        = 30 * time.Second
	defaultMaxResponseSize = 10 << 20 // 10 MB

	openAIEndpoint     = "https://api.openai.com/v1/chat/completions"
	openRouterEndpoint = "https://openrouter.ai/api/v1/chat/completions"
	geminiEndpoint     = "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions"
	defaultOllamaHost  = "http://localhost:11434"
)

// HTTPConfig encapsulates configuration options for an OpenAI-compatible HTTP AI provider.
type HTTPConfig struct {
	// Endpoint is the full URL to the chat completions endpoint.
	// (e.g., "https://api.openai.com/v1/chat/completions" or "http://localhost:11434/v1/chat/completions")
	Endpoint string

	// Model specifies the model identifier (e.g., "gpt-4o", "qwen2.5-coder:7b", "deepseek-chat").
	Model string

	// APIKey is an optional authorization bearer token. Left empty for local instances like Ollama.
	APIKey string

	// Headers contains optional custom HTTP request headers.
	Headers map[string]string

	// Timeout specifies the per-request timeout. Defaults to 30 seconds if <= 0.
	Timeout time.Duration

	// Temperature controls generation randomness (e.g. 0.0 for deterministic outputs).
	Temperature *float64

	// HTTPClient allows injecting a custom *http.Client. If nil, a client with Timeout is used.
	HTTPClient *http.Client
}

// HTTPConfigOption is a functional option for configuring HTTP providers.
type HTTPConfigOption func(*HTTPConfig)

// WithTimeout sets the per-request HTTP timeout.
func WithTimeout(d time.Duration) HTTPConfigOption {
	return func(cfg *HTTPConfig) {
		cfg.Timeout = d
	}
}

// WithTemperature sets the sampling temperature.
func WithTemperature(t float64) HTTPConfigOption {
	return func(cfg *HTTPConfig) {
		cfg.Temperature = &t
	}
}

// WithHeader adds or overrides a custom HTTP header on outbound requests.
func WithHeader(key, value string) HTTPConfigOption {
	return func(cfg *HTTPConfig) {
		if cfg.Headers == nil {
			cfg.Headers = make(map[string]string)
		}
		cfg.Headers[key] = value
	}
}

// WithHTTPClient configures a custom *http.Client for the provider.
func WithHTTPClient(client *http.Client) HTTPConfigOption {
	return func(cfg *HTTPConfig) {
		cfg.HTTPClient = client
	}
}

// HTTPProvider implements AIProvider using standard OpenAI-compatible HTTP chat completions.
type HTTPProvider struct {
	cfg    HTTPConfig
	client *http.Client
}

// NewOpenAICompatible creates a universal AIProvider targeting any OpenAI-compatible endpoint
// (OpenAI, Ollama, Groq, DeepSeek, vLLM, OpenRouter, Gemini, and enterprise gateways).
func NewOpenAICompatible(cfg HTTPConfig) (*HTTPProvider, error) {
	if strings.TrimSpace(cfg.Endpoint) == "" {
		return nil, errors.New("autopilot: endpoint cannot be empty")
	}
	if strings.TrimSpace(cfg.Model) == "" {
		return nil, errors.New("autopilot: model cannot be empty")
	}

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	cfg.Timeout = timeout

	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: timeout}
	}

	return &HTTPProvider{
		cfg:    cfg,
		client: client,
	}, nil
}

// NewOpenAI creates an AIProvider targeting OpenAI's API.
func NewOpenAI(apiKey, model string, opts ...HTTPConfigOption) (*HTTPProvider, error) {
	cfg := HTTPConfig{
		Endpoint: openAIEndpoint,
		Model:    model,
		APIKey:   apiKey,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return NewOpenAICompatible(cfg)
}

// NewOpenRouter creates an AIProvider targeting OpenRouter's universal multi-model API.
func NewOpenRouter(apiKey, model string, opts ...HTTPConfigOption) (*HTTPProvider, error) {
	cfg := HTTPConfig{
		Endpoint: openRouterEndpoint,
		Model:    model,
		APIKey:   apiKey,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return NewOpenAICompatible(cfg)
}

// NewOllama creates an AIProvider targeting a local or remote Ollama instance.
// If endpoint is empty, it defaults to "http://localhost:11434".
func NewOllama(endpoint, model string, opts ...HTTPConfigOption) (*HTTPProvider, error) {
	if strings.TrimSpace(endpoint) == "" {
		endpoint = defaultOllamaHost
	}
	trimmed := strings.TrimRight(endpoint, "/")
	if !strings.HasSuffix(trimmed, "/v1/chat/completions") && !strings.HasSuffix(trimmed, "/chat/completions") {
		trimmed += "/v1/chat/completions"
	}

	cfg := HTTPConfig{
		Endpoint: trimmed,
		Model:    model,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return NewOpenAICompatible(cfg)
}

// NewGemini creates an AIProvider targeting Google's Gemini OpenAI-compatible API endpoint.
func NewGemini(apiKey, model string, opts ...HTTPConfigOption) (*HTTPProvider, error) {
	cfg := HTTPConfig{
		Endpoint: geminiEndpoint,
		Model:    model,
		APIKey:   apiKey,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return NewOpenAICompatible(cfg)
}

type chatRequest struct {
	Model       string    `json:"model"`
	Messages    []Message `json:"messages"`
	Temperature *float64  `json:"temperature,omitempty"`
}

type chatResponse struct {
	Choices []struct {
		Index   int `json:"index"`
		Message struct {
			Role    Role   `json:"role"`
			Content string `json:"content"`
		} `json:"message"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
	Error *struct {
		Message string `json:"message"`
		Type    string `json:"type"`
		Code    any    `json:"code"`
	} `json:"error,omitempty"`
}

// Generate sends the conversation messages to the LLM endpoint and returns the generated content.
func (p *HTTPProvider) Generate(ctx context.Context, messages []Message) (string, error) {
	if len(messages) == 0 {
		return "", errors.New("autopilot: at least one message is required")
	}

	reqBody := chatRequest{
		Model:       p.cfg.Model,
		Messages:    messages,
		Temperature: p.cfg.Temperature,
	}

	payloadBytes, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("autopilot: failed to marshal request payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.cfg.Endpoint, bytes.NewReader(payloadBytes))
	if err != nil {
		return "", fmt.Errorf("autopilot: failed to create HTTP request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "flux-autopilot")

	if strings.TrimSpace(p.cfg.APIKey) != "" {
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(p.cfg.APIKey))
	}

	for k, v := range p.cfg.Headers {
		req.Header.Set(k, v)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("autopilot: request failed: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(io.LimitReader(resp.Body, defaultMaxResponseSize))
	if err != nil {
		return "", fmt.Errorf("autopilot: failed to read response body: %w", err)
	}

	var parsedResp chatResponse
	jsonErr := json.Unmarshal(bodyBytes, &parsedResp)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if jsonErr == nil && parsedResp.Error != nil && parsedResp.Error.Message != "" {
			return "", fmt.Errorf("autopilot: API error (status %d): %s", resp.StatusCode, parsedResp.Error.Message)
		}
		snippet := strings.TrimSpace(string(bodyBytes))
		if len(snippet) > 200 {
			snippet = snippet[:200] + "..."
		}
		return "", fmt.Errorf("autopilot: API error (status %d): %s", resp.StatusCode, snippet)
	}

	if jsonErr != nil {
		return "", fmt.Errorf("autopilot: failed to decode response JSON: %w (body: %s)", jsonErr, string(bodyBytes))
	}

	if len(parsedResp.Choices) == 0 {
		return "", errors.New("autopilot: API returned no choices in response")
	}

	return strings.TrimSpace(parsedResp.Choices[0].Message.Content), nil
}
