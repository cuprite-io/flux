package telemetry

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Semantic attribute keys for Flux execution spans.
const (
	AttrCircuitID      = attribute.Key("flux.circuit.id")
	AttrCircuitName    = attribute.Key("flux.circuit.name")
	AttrCircuitTags    = attribute.Key("flux.circuit.tags")
	AttrNodeName       = attribute.Key("flux.node.name")
	AttrNodePruned     = attribute.Key("flux.node.pruned")
	AttrNodeCondition  = attribute.Key("flux.node.condition")
	AttrStepType       = attribute.Key("flux.step.type")
	AttrSinkName       = attribute.Key("flux.sink.name")
	AttrPassed         = attribute.Key("flux.passed")
	AttrSparkTags      = attribute.Key("flux.spark.tags")
	AttrConductEntity  = attribute.Key("flux.conduct.entity_id")
	AttrConductCategory= attribute.Key("flux.conduct.category")
	AttrCandidateCount = attribute.Key("flux.conduct.evaluated_count")
	AttrQualifiedCount = attribute.Key("flux.conduct.qualified_count")
	AttrCandidateID    = attribute.Key("flux.candidate.id")
	AttrCandidateRank  = attribute.Key("flux.candidate.rank")
	AttrCandidateScore = attribute.Key("flux.candidate.score")
	AttrFluxVersion      = attribute.Key("flux.version")
	AttrStepScript       = attribute.Key("flux.step.script")
	AttrMatchingCircuits = attribute.Key("flux.spark.matching_circuits")
)

// Tracer wraps an OpenTelemetry trace.Tracer.
// When tracer is nil, all operations are zero-allocation no-ops returning unmodified context.
type Tracer struct {
	tracer            trace.Tracer
	defaultAttributes []attribute.KeyValue
}

// NewTracer wraps an OpenTelemetry trace.Tracer and applies default attributes (e.g. flux.version).
func NewTracer(tr trace.Tracer, defaultAttrs ...attribute.KeyValue) *Tracer {
	if tr == nil {
		return nil
	}
	return &Tracer{
		tracer:            tr,
		defaultAttributes: defaultAttrs,
	}
}

// IsEnabled returns true if OpenTelemetry tracing is active.
func (t *Tracer) IsEnabled() bool {
	return t != nil && t.tracer != nil
}

// Start creates a span if tracing is enabled, otherwise returns the context and a no-op span.
func (t *Tracer) Start(ctx context.Context, spanName string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	if t == nil || t.tracer == nil {
		return ctx, trace.SpanFromContext(ctx)
	}
	if len(t.defaultAttributes) > 0 {
		opts = append([]trace.SpanStartOption{trace.WithAttributes(t.defaultAttributes...)}, opts...)
	}
	return t.tracer.Start(ctx, spanName, opts...)
}

// EndSpan ends a span, recording error status if err is non-nil.
func EndSpan(span trace.Span, err error) {
	if span == nil || !span.IsRecording() {
		return
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}
	span.End()
}
