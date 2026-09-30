package autopilot

import (
	"context"
	"errors"
)

// Role represents the author role of a chat message.
type Role string

const (
	// RoleSystem denotes instructions that establish behavior, context, and schemas.
	RoleSystem Role = "system"
	// RoleUser denotes messages sent by human operators or autonomous feedback loops.
	RoleUser Role = "user"
	// RoleAssistant denotes responses synthesized by the AI model.
	RoleAssistant Role = "assistant"
)

// Message represents an atomic turn in a multi-turn conversation.
type Message struct {
	Role    Role   `json:"role"`
	Content string `json:"content"`
}

// UserMessage is a convenience constructor for a user message.
func UserMessage(content string) Message {
	return Message{Role: RoleUser, Content: content}
}

// SystemMessage is a convenience constructor for a system message.
func SystemMessage(content string) Message {
	return Message{Role: RoleSystem, Content: content}
}

// AssistantMessage is a convenience constructor for an assistant message.
func AssistantMessage(content string) Message {
	return Message{Role: RoleAssistant, Content: content}
}

// AIProvider is the universal abstraction for pluggable AI generation backends.
// Implementations handle wire communication with LLMs, including multi-turn
// reflection and validation loops.
type AIProvider interface {
	Generate(ctx context.Context, messages []Message) (string, error)
}

// ProviderFunc allows using ordinary functions or closures as AIProvider instances.
type ProviderFunc func(ctx context.Context, messages []Message) (string, error)

// Generate calls f(ctx, messages).
func (f ProviderFunc) Generate(ctx context.Context, messages []Message) (string, error) {
	return f(ctx, messages)
}

// GeneratePrompt is a convenience helper for single-turn system/user prompt execution.
func GeneratePrompt(ctx context.Context, provider AIProvider, systemPrompt, userPrompt string) (string, error) {
	if provider == nil {
		return "", errors.New("autopilot: provider cannot be nil")
	}
	messages := make([]Message, 0, 2)
	if systemPrompt != "" {
		messages = append(messages, SystemMessage(systemPrompt))
	}
	if userPrompt != "" {
		messages = append(messages, UserMessage(userPrompt))
	}
	return provider.Generate(ctx, messages)
}
