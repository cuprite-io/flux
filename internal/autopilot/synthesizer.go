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
	"github.com/cuprite-io/flux/types"
)

const defaultMaxRetries = 3

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

// CorrectionAttempt records a single failed synthesis turn and its validation errors during reflection.
type CorrectionAttempt struct {
	Attempt   int               `json:"attempt"`
	Candidate string            `json:"candidate"`
	Errors    []ValidationError `json:"errors"`
}

// SynthesisResult represents a successfully synthesized and validated Circuit DAG.
type SynthesisResult struct {
	Circuit           *types.Circuit      `json:"-"`
	CleanJSON         string              `json:"clean_json"`
	Attempts          int                 `json:"attempts"`
	CorrectionHistory []CorrectionAttempt `json:"correction_history,omitempty"`
}

// SynthesizerOption configures Synthesizer behavior.
type SynthesizerOption func(*Synthesizer)

// WithMaxRetries configures the maximum number of self-correction reflection turns.
func WithMaxRetries(n int) SynthesizerOption {
	return func(s *Synthesizer) {
		if n > 0 {
			s.maxRetries = n
		}
	}
}

// WithValidator allows injecting a custom CircuitValidator.
func WithValidator(v *CircuitValidator) SynthesizerOption {
	return func(s *Synthesizer) {
		if v != nil {
			s.validator = v
		}
	}
}

// Synthesizer translates natural language intent and engine telemetry into an executable,
// compiler-verified Circuit DAG using an autonomous reflection loop.
type Synthesizer struct {
	provider   autopilot.AIProvider
	prompter   *PromptBuilder
	validator  *CircuitValidator
	maxRetries int
}

// NewSynthesizer creates a new circuit synthesizer with the given AI provider.
func NewSynthesizer(provider autopilot.AIProvider, opts ...SynthesizerOption) (*Synthesizer, error) {
	if provider == nil {
		return nil, errors.New("autopilot: provider cannot be nil")
	}

	val, err := NewCircuitValidator()
	if err != nil {
		return nil, fmt.Errorf("autopilot: failed to init circuit validator: %w", err)
	}

	s := &Synthesizer{
		provider:   provider,
		prompter:   NewPromptBuilder(),
		validator:  val,
		maxRetries: defaultMaxRetries,
	}

	for _, opt := range opts {
		opt(s)
	}

	return s, nil
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

// Synthesize generates a candidate Circuit DAG JSON string from the AI provider without reflection.
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

// SynthesizeWithReflection runs the closed-loop synthesis pipeline:
// 1. Prompts the AI provider with objective, schemas, profiles, and sinks.
// 2. Validates output through CircuitValidator (parse, lint, compiler, sinks).
// 3. If errors occur, feeds detailed compiler diagnostics back to the AI provider to self-repair (up to maxRetries).
func (s *Synthesizer) SynthesizeWithReflection(ctx context.Context, req SynthesisRequest) (*SynthesisResult, error) {
	if strings.TrimSpace(req.Prompt) == "" {
		return nil, errors.New("autopilot: prompt cannot be empty")
	}

	sysPrompt, userPrompt := s.BuildPrompts(req)

	messages := []autopilot.Message{
		autopilot.SystemMessage(sysPrompt),
		autopilot.UserMessage(userPrompt),
	}

	var history []CorrectionAttempt

	for attempt := 1; attempt <= s.maxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("autopilot: synthesis context canceled: %w", err)
		}

		rawResponse, err := s.provider.Generate(ctx, messages)
		if err != nil {
			return nil, fmt.Errorf("autopilot: AI generation failed at attempt %d: %w", attempt, err)
		}

		valRes := s.validator.Validate(rawResponse, req.Sinks)
		if valRes.Valid {
			return &SynthesisResult{
				Circuit:           valRes.Circuit,
				CleanJSON:         valRes.CleanJSON,
				Attempts:          attempt,
				CorrectionHistory: history,
			}, nil
		}

		// Validation failed
		history = append(history, CorrectionAttempt{
			Attempt:   attempt,
			Candidate: rawResponse,
			Errors:    valRes.Errors,
		})

		// If retries remain, format reflection feedback and query LLM again
		if attempt < s.maxRetries {
			messages = append(messages, autopilot.AssistantMessage(rawResponse))

			feedbackPrompt := fmt.Sprintf(
				"The synthesized Flux circuit failed compiler and guardrail verification with the following error(s):\n%s\n\n"+
					"Please fix all listed errors and output the corrected Flux Circuit DAG.\n"+
					"Remember:\n"+
					"- Ensure all VoltScript expressions compile with zero syntax/type errors.\n"+
					"- Only reference sinks that are explicitly listed in the available sinks.\n"+
					"- Output strictly the corrected JSON object without markdown or commentary.",
				valRes.FormatErrors(),
			)

			messages = append(messages, autopilot.UserMessage(feedbackPrompt))
		}
	}

	// All attempts exhausted
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("autopilot: failed to synthesize valid circuit after %d attempts. Latest errors:\n", s.maxRetries))
	if len(history) > 0 {
		lastErrors := history[len(history)-1].Errors
		for _, e := range lastErrors {
			fmt.Fprintf(&sb, "  - %s\n", e.String())
		}
	}

	return nil, errors.New(sb.String())
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
