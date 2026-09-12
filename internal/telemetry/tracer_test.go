package telemetry_test

import (
	"context"
	"testing"

	"github.com/cuprite-io/flux/internal/telemetry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestTracer_NoOpSafety(t *testing.T) {
	ctx := context.Background()

	// 1. Nil tracer wrapper
	var tr *telemetry.Tracer
	assert.False(t, tr.IsEnabled())

	spanCtx, span := tr.Start(ctx, "test-span")
	assert.Equal(t, ctx, spanCtx)
	assert.NotNil(t, span)
	assert.False(t, span.IsRecording())
	telemetry.EndSpan(span, nil)

	// 2. Initialized with noop.NewTracerProvider()
	provider := noop.NewTracerProvider()
	tr = telemetry.NewTracer(provider.Tracer("flux-test"))
	require.NotNil(t, tr)
	assert.True(t, tr.IsEnabled())

	spanCtx2, span2 := tr.Start(ctx, "test-span-noop")
	assert.NotNil(t, spanCtx2)
	assert.NotNil(t, span2)
	telemetry.EndSpan(span2, nil)
}
