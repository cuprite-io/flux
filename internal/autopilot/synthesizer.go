package autopilot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/cuprite-io/assay"
	"github.com/cuprite-io/flux/autopilot"
	"github.com/cuprite-io/flux/internal/profiler"
	"github.com/cuprite-io/flux/internal/sink"
)

// SynthesisRequest encapsulates all telemetry and user constraints needed to synthesize a Circuit.
type SynthesisRequest struct {
	// Prompt is the natural language instruction or alerting policy.
	Prompt string

	// Tags specifies the stream tags to monitor.
	Tags []string

	// Schemas contains inferred assay.SchemaNode trees by tag or tag:variant.
	Schemas map[string]*assay.SchemaNode

	// Profiles contains value distributions and exemplars by stream tag.
	Profiles map[string]*profiler.StreamProfile

	// Sinks lists the registered external sinks with their descriptions and severities.
	Sinks []sink.Descriptor

	// CircuitID is an optional preferred ID for the generated circuit.
	CircuitID string
}

// Synthesizer translates natural language intent and engine telemetry into a candidate Circuit JSON DAG.
type Synthesizer struct {
	provider autopilot.AIProvider
	prompter *PromptBuilder
}

// NewSynthesizer creates a new circuit synthesizer with the given AI provider.
func NewSynthesizer(provider autopilot.AIProvider) (*Synthesizer, error) {
	if provider == nil {
		return nil, errors.New("autopilot: provider cannot be nil")
	}
	return &Synthesizer{
		provider: provider,
		prompter: NewPromptBuilder(),
	}, nil
}

// BuildPrompts constructs the system and user prompt strings for the request.
func (s *Synthesizer) BuildPrompts(req SynthesisRequest) (systemPrompt string, userPrompt string) {
	systemPrompt = s.prompter.BuildSystemPrompt()
	userPrompt = s.prompter.BuildUserPrompt(
		req.Prompt,
		req.Tags,
		req.Schemas,
		req.Profiles,
		req.Sinks,
		req.CircuitID,
	)
	return systemPrompt, userPrompt
}

// Synthesize generates a candidate Circuit DAG JSON string from the AI provider.
func (s *Synthesizer) Synthesize(ctx context.Context, req SynthesisRequest) (string, error) {
	if strings.TrimSpace(req.Prompt) == "" {
		return "", errors.New("autopilot: prompt cannot be empty")
	}

	sysPrompt, userPrompt := s.BuildPrompts(req)

	messages := []autopilot.Message{
		autopilot.SystemMessage(sysPrompt),
		autopilot.UserMessage(userPrompt),
	}

	rawResponse, err := s.provider.Generate(ctx, messages)
	if err != nil {
		return "", fmt.Errorf("autopilot: AI generation failed: %w", err)
	}

	cleaned, err := CleanJSON(rawResponse)
	if err != nil {
		return "", fmt.Errorf("autopilot: failed to extract valid JSON from response: %w (raw: %s)", err, rawResponse)
	}

	return cleaned, nil
}

// CleanJSON extracts a clean, parseable JSON object from model output that may include
// markdown code fences or conversational preamble/postscript.
func CleanJSON(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", errors.New("empty response")
	}

	// Remove markdown fences: ```json ... ``` or ``` ... ```
	if strings.HasPrefix(trimmed, "```") {
		firstNewline := strings.IndexByte(trimmed, '\n')
		if firstNewline != -1 {
			trimmed = strings.TrimSpace(trimmed[firstNewline+1:])
		}
		if idx := strings.LastIndex(trimmed, "```"); idx != -1 {
			trimmed = strings.TrimSpace(trimmed[:idx])
		}
	}

	// Find outermost JSON object boundaries { ... }
	startIdx := strings.IndexByte(trimmed, '{')
	endIdx := strings.LastIndexByte(trimmed, '}')

	if startIdx == -1 || endIdx == -1 || startIdx >= endIdx {
		return "", errors.New("no valid JSON object found in response")
	}

	candidate := strings.TrimSpace(trimmed[startIdx : endIdx+1])

	// Validate that the extracted chunk is valid JSON
	var js json.RawMessage
	if err := json.Unmarshal([]byte(candidate), &js); err != nil {
		return "", fmt.Errorf("invalid JSON syntax: %w", err)
	}

	return candidate, nil
}
