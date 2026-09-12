package flux_test

import (
	"context"
	"sync"
	"testing"

	"github.com/cuprite-io/flux"
	"github.com/cuprite-io/flux/internal/sink"
	"github.com/cuprite-io/flux/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestEngine_OpenTelemetry_SparkAndNodes(t *testing.T) {
	ctx := context.Background()

	exporter := tracetest.NewInMemoryExporter()
	tp := tracesdk.NewTracerProvider(
		tracesdk.WithSyncer(exporter),
	)
	tracer := tp.Tracer("flux-integration-test")

	var emittedPayload any
	var emitMu sync.Mutex
	testSink := sink.FuncSink(func(ctx context.Context, payload any) error {
		emitMu.Lock()
		emittedPayload = payload
		emitMu.Unlock()
		return nil
	})

	eng, err := flux.New(
		flux.WithTracer(tracer),
		flux.WithSink("audit_sink", testSink),
	)
	require.NoError(t, err)
	defer eng.Close()

	child := types.NewNode("child_action").
		WithCondition("payload.amount > 100").
		Step(flux.Sink("audit_sink"))

	root := types.NewNode("root_guard").
		WithCondition("payload.active == true").
		AddChildren(child)

	circuit := types.NewCircuit("fraud_alert_pipeline").
		WithTags("stream:tx").
		WithRoot(root)

	require.NoError(t, eng.Registry().Put(ctx, circuit))

	payload := map[string]any{
		"active": true,
		"amount": 250,
	}
	res, err := eng.Spark(ctx, payload, "stream:tx")
	require.NoError(t, err)
	assert.True(t, res.Passed)

	// Close engine to flush asynchronous sink worker queue
	require.NoError(t, eng.Close())

	// Flush tracing spans
	require.NoError(t, tp.ForceFlush(ctx))

	emitMu.Lock()
	assert.NotNil(t, emittedPayload)
	emitMu.Unlock()

	spans := exporter.GetSpans()
	require.NotEmpty(t, spans)

	spanNames := make([]string, len(spans))
	for i, s := range spans {
		spanNames[i] = s.Name
	}

	assert.Contains(t, spanNames, "flux.spark")
	assert.Contains(t, spanNames, "flux.spark:prepare")
	assert.Contains(t, spanNames, "flux.circuit:fraud_alert_pipeline")
	assert.Contains(t, spanNames, "flux.node:root_guard")
	assert.Contains(t, spanNames, "flux.node:child_action")
	assert.Contains(t, spanNames, "flux.sink:audit_sink")

	var childSpan tracetest.SpanStub
	var foundChild bool
	for _, s := range spans {
		if s.Name == "flux.node:child_action" {
			childSpan = s
			foundChild = true
			break
		}
	}
	require.True(t, foundChild)

	attrMap := make(map[attribute.Key]attribute.Value)
	for _, a := range childSpan.Attributes {
		attrMap[a.Key] = a.Value
	}
	assert.Equal(t, "child_action", attrMap[attribute.Key("flux.node.name")].AsString())
	assert.False(t, attrMap[attribute.Key("flux.node.pruned")].AsBool())
	assert.Equal(t, flux.Version, attrMap[attribute.Key("flux.version")].AsString())

	// Verify all spans contain flux.version
	for _, s := range spans {
		var foundVersion bool
		for _, a := range s.Attributes {
			if a.Key == attribute.Key("flux.version") && a.Value.AsString() == flux.Version {
				foundVersion = true
				break
			}
		}
		assert.Truef(t, foundVersion, "span %s should contain flux.version attribute", s.Name)
	}
}

func TestEngine_OpenTelemetry_Conduct(t *testing.T) {
	ctx := context.Background()

	exporter := tracetest.NewInMemoryExporter()
	tp := tracesdk.NewTracerProvider(
		tracesdk.WithSyncer(exporter),
	)
	tracer := tp.Tracer("flux-conduct-test")

	eng, err := flux.New(
		flux.WithTracer(tracer),
	)
	require.NoError(t, err)
	defer eng.Close()

	item := &types.Item{
		ID:       "discount_10",
		Category: "offers",
		Data: map[string]any{
			"discount_pct": 10,
		},
		Circuit: types.NewCircuit("item_check").
			WithRoot(types.NewNode("always_pass")),
	}
	require.NoError(t, eng.Catalog().Put(ctx, item))

	req := &types.ConductRequest{
		EntityID: "user_42",
		Category: "offers",
		TopK:     5,
	}
	cRes, err := eng.Conduct(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, 1, len(cRes.Items))

	require.NoError(t, tp.ForceFlush(ctx))

	spans := exporter.GetSpans()
	require.NotEmpty(t, spans)

	var conductSpan tracetest.SpanStub
	var foundConduct bool
	for _, s := range spans {
		if s.Name == "flux.conduct" {
			conductSpan = s
			foundConduct = true
			break
		}
	}
	require.True(t, foundConduct)

	attrMap := make(map[attribute.Key]attribute.Value)
	for _, a := range conductSpan.Attributes {
		attrMap[a.Key] = a.Value
	}
	assert.Equal(t, "user_42", attrMap[attribute.Key("flux.conduct.entity_id")].AsString())
	assert.Equal(t, "offers", attrMap[attribute.Key("flux.conduct.category")].AsString())
	assert.Equal(t, int64(1), attrMap[attribute.Key("flux.conduct.evaluated_count")].AsInt64())
	assert.Equal(t, int64(1), attrMap[attribute.Key("flux.conduct.qualified_count")].AsInt64())
}
