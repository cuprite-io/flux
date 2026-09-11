package flux_test

import (
	"context"
	"testing"
	"time"

	"github.com/cuprite-io/flux"
	"github.com/cuprite-io/flux/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEngine_SchemaLearning_HeterogeneousStream(t *testing.T) {
	ctx := context.Background()

	// Initialize Engine with SchemaLearning enabled
	eng, err := flux.New(
		flux.WithSchemaLearning(true),
		flux.WithSchemaDiscriminators("level", "event_type"),
	)
	require.NoError(t, err)
	defer eng.Close()

	// Deploy dummy circuit to accept events
	root := types.NewNode("process_log").
		Step(flux.Return(map[string]any{"ok": true}))

	circuit := types.NewCircuit("log_stream_circuit").
		WithTags("stream:logs", "stream:iot").
		WithRoot(root)

	require.NoError(t, eng.Registry().Put(ctx, circuit))

	// 1. Ingest INFO logs (Shape A)
	infoLog := map[string]any{
		"level":     "INFO",
		"timestamp": time.Now().Unix(),
		"message":   "service started successfully",
		"port":      8080,
	}
	for i := 0; i < 5; i++ {
		res, sparkErr := eng.Spark(ctx, infoLog, "stream:logs")
		require.NoError(t, sparkErr)
		assert.True(t, res.Passed)
	}

	// 2. Ingest ERROR logs (Shape B with distinct error properties)
	errorLog := map[string]any{
		"level":      "ERROR",
		"timestamp":  time.Now().Unix(),
		"error_code": 503,
		"exception":  "database_timeout",
		"retryable":  true,
	}
	for i := 0; i < 5; i++ {
		res, sparkErr := eng.Spark(ctx, errorLog, "stream:logs")
		require.NoError(t, sparkErr)
		assert.True(t, res.Passed)
	}

	// 3. Ingest Unlabeled IoT Telemetry (Shape C - falls back to structural fingerprint)
	iotTelemetry := map[string]any{
		"device_id": "sensor_xyz_01",
		"temp_c":    22.4,
		"humidity":  55.1,
	}
	for i := 0; i < 5; i++ {
		res, sparkErr := eng.Spark(ctx, iotTelemetry, "stream:iot")
		require.NoError(t, sparkErr)
		assert.True(t, res.Passed)
	}

	// Flush schema tap
	tap := eng.SchemaTap()
	require.NotNil(t, tap)
	require.NoError(t, tap.Flush())

	// Verify Shape A (INFO schema)
	infoSchema, err := tap.GetSchema(ctx, "stream:logs:INFO")
	require.NoError(t, err)
	require.NotNil(t, infoSchema)
	require.Contains(t, infoSchema.Children, "port")
	require.Contains(t, infoSchema.Children, "message")
	assert.Equal(t, "number", infoSchema.Children["port"].Type)
	assert.Equal(t, "string", infoSchema.Children["message"].Type)

	// Verify Shape B (ERROR schema - completely separate, not polluted by Shape A)
	errSchema, err := tap.GetSchema(ctx, "stream:logs:ERROR")
	require.NoError(t, err)
	require.NotNil(t, errSchema)
	require.Contains(t, errSchema.Children, "error_code")
	require.Contains(t, errSchema.Children, "exception")
	require.Contains(t, errSchema.Children, "retryable")
	assert.Equal(t, "number", errSchema.Children["error_code"].Type)
	assert.Equal(t, "boolean", errSchema.Children["retryable"].Type)
	assert.NotContains(t, errSchema.Children, "port") // Clean isolation!

	// Verify Shape C (Structural Fingerprint schema)
	fingerprintID := tap.ResolveSchemaID("stream:iot", iotTelemetry)
	assert.Contains(t, fingerprintID, "stream:iot:shape_")
	iotSchema, err := tap.GetSchema(ctx, fingerprintID)
	require.NoError(t, err)
	require.NotNil(t, iotSchema)
	require.Contains(t, iotSchema.Children, "temp_c")
	require.Contains(t, iotSchema.Children, "humidity")
	assert.Equal(t, "number", iotSchema.Children["temp_c"].Type)
}

func TestEngine_SchemaLearning_StructInputs(t *testing.T) {
	ctx := context.Background()

	eng, err := flux.New(
		flux.WithSchemaLearning(true),
		flux.WithSchemaDiscriminators("event_type"),
	)
	require.NoError(t, err)
	defer eng.Close()

	type SensorPacket struct {
		EventType string  `json:"event_type"`
		SensorID  string  `json:"sensor_id"`
		Voltage   float64 `json:"voltage"`
	}

	root := types.NewNode("process").
		Step(flux.Return(map[string]any{"ok": true}))
	circuit := types.NewCircuit("sensor_circuit").
		WithTags("stream:sensors").
		WithRoot(root)
	require.NoError(t, eng.Registry().Put(ctx, circuit))

	for i := 0; i < 5; i++ {
		pkt := SensorPacket{
			EventType: "battery_telemetry",
			SensorID:  "sens_123",
			Voltage:   3.7 + float64(i)*0.1,
		}
		res, err := eng.Spark(ctx, pkt, "stream:sensors")
		require.NoError(t, err)
		assert.True(t, res.Passed)
	}

	tap := eng.SchemaTap()
	require.NoError(t, tap.Flush())

	schema, err := tap.GetSchema(ctx, "stream:sensors:battery_telemetry")
	require.NoError(t, err)
	require.NotNil(t, schema)
	require.Contains(t, schema.Children, "voltage")
	require.Contains(t, schema.Children, "sensor_id")
	assert.Equal(t, "number", schema.Children["voltage"].Type)
	assert.Equal(t, "string", schema.Children["sensor_id"].Type)
}

func BenchmarkSpark_WithoutSchemaLearning(b *testing.B) {
	ctx := context.Background()
	eng, err := flux.New()
	if err != nil {
		b.Fatal(err)
	}
	defer eng.Close()

	root := types.NewNode("process").
		Step(flux.Return(map[string]any{"ok": true}))
	circuit := types.NewCircuit("bench_circuit").
		WithTags("stream:bench").
		WithRoot(root)
	_ = eng.Registry().Put(ctx, circuit)

	payload := map[string]any{
		"level":   "INFO",
		"message": "benchmark payload",
		"code":    200,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _ = eng.Spark(ctx, payload, "stream:bench")
	}
}

func BenchmarkSpark_WithSchemaLearning(b *testing.B) {
	ctx := context.Background()
	eng, err := flux.New(
		flux.WithSchemaLearning(true),
		flux.WithSchemaDiscriminators("level"),
	)
	if err != nil {
		b.Fatal(err)
	}
	defer eng.Close()

	root := types.NewNode("process").
		Step(flux.Return(map[string]any{"ok": true}))
	circuit := types.NewCircuit("bench_circuit").
		WithTags("stream:bench").
		WithRoot(root)
	_ = eng.Registry().Put(ctx, circuit)

	payload := map[string]any{
		"level":   "INFO",
		"message": "benchmark payload",
		"code":    200,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _ = eng.Spark(ctx, payload, "stream:bench")
	}
}
