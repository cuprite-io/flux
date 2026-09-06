package flux

import (
	"github.com/cuprite-io/flux/types"
)

// Volt creates a StepDefinition that evaluates a VoltScript expression.
func Volt(script string) *types.StepDefinition {
	return &types.StepDefinition{
		Type:   types.StepVolt,
		Script: script,
	}
}

// Return creates a StepDefinition that projects output data.
func Return(data map[string]any) *types.StepDefinition {
	return &types.StepDefinition{
		Type:      types.StepReturn,
		ReturnMap: data,
	}
}

// Abort creates a StepDefinition that halts execution.
func Abort() *types.StepDefinition {
	return &types.StepDefinition{
		Type: types.StepAbort,
	}
}

// SinkStepBuilder provides a fluent builder for configuring StepSink definitions.
type SinkStepBuilder struct {
	step *types.StepDefinition
}

// Sink creates a fluent builder for a named StepSink with an optional sinkType descriptor.
func Sink(name string, sinkType ...string) *SinkStepBuilder {
	st := ""
	if len(sinkType) > 0 {
		st = sinkType[0]
	}
	return &SinkStepBuilder{
		step: &types.StepDefinition{
			Type:     types.StepSink,
			SinkName: name,
			SinkType: st,
		},
	}
}

// WithCondition attaches a guard condition to the sink dispatch.
func (b *SinkStepBuilder) WithCondition(cond string) *SinkStepBuilder {
	b.step.Condition = cond
	return b
}

// WithPayload sets the projected state key to dispatch as payload.
func (b *SinkStepBuilder) WithPayload(payloadKey string) *SinkStepBuilder {
	b.step.Payload = payloadKey
	return b
}

// ToStep satisfies types.StepProvider.
func (b *SinkStepBuilder) ToStep() *types.StepDefinition {
	return b.step
}

// Build returns the underlying StepDefinition.
func (b *SinkStepBuilder) Build() *types.StepDefinition {
	return b.step
}
