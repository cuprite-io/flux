package schematap

import (
	"context"
	"testing"
	"time"

	"github.com/cuprite-io/flux/internal/cache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchemaTap_ResolveSchemaID_ThreeTiers(t *testing.T) {
	mem := cache.NewMemoryCache()
	defer mem.Close()

	cfg := DefaultConfig()
	cfg.DiscriminatorKeys = []string{"level", "event_type"}
	tap, err := New(mem, cfg)
	require.NoError(t, err)
	defer tap.Close()

	// Tier 1: Discriminator Key Found
	logPayload := map[string]any{
		"level":   "ERROR",
		"message": "database connection refused",
		"code":    500,
	}
	id1 := tap.ResolveSchemaID("stream:logs", logPayload)
	assert.Equal(t, "stream:logs:ERROR", id1)

	// Tier 1: Second Discriminator Key Found
	iotPayload := map[string]any{
		"event_type": "temperature_reading",
		"temp_c":     24.5,
	}
	id2 := tap.ResolveSchemaID("stream:iot", iotPayload)
	assert.Equal(t, "stream:iot:temperature_reading", id2)

	// Tier 2: No Discriminator -> Falls back to Structural Key Fingerprint
	unlabeledPayload1 := map[string]any{
		"lat":   37.7749,
		"lon":   -122.4194,
		"speed": 65.2,
	}
	id3 := tap.ResolveSchemaID("stream:gps", unlabeledPayload1)
	assert.Contains(t, id3, "stream:gps:shape_")

	// Same shape produces identical fingerprint
	unlabeledPayload2 := map[string]any{
		"lon":   -74.0060,
		"speed": 0.0,
		"lat":   40.7128,
	}
	id4 := tap.ResolveSchemaID("stream:gps", unlabeledPayload2)
	assert.Equal(t, id3, id4)

	// Different shape produces different fingerprint
	unlabeledPayload3 := map[string]any{
		"battery_pct": 88,
		"status":      "CHARGING",
	}
	id5 := tap.ResolveSchemaID("stream:gps", unlabeledPayload3)
	assert.NotEqual(t, id3, id5)

	// Tier 3: If Fingerprinting disabled -> Falls back to Tag Union
	cfgNoFingerprint := cfg
	cfgNoFingerprint.FallbackToFingerprint = false
	tapUnion, err := New(mem, cfgNoFingerprint)
	require.NoError(t, err)
	defer tapUnion.Close()

	idUnion := tapUnion.ResolveSchemaID("stream:mixed", unlabeledPayload1)
	assert.Equal(t, "stream:mixed", idUnion)
}

func TestSchemaTap_SampleAndReconstruct(t *testing.T) {
	ctx := context.Background()
	mem := cache.NewMemoryCache()
	defer mem.Close()

	cfg := DefaultConfig()
	cfg.AssayConfig.FlushInterval = 50 * time.Millisecond

	tap, err := New(mem, cfg)
	require.NoError(t, err)
	defer tap.Close()

	// Ingest sample stream of error logs
	for i := 0; i < 10; i++ {
		payload := map[string]any{
			"level":      "ERROR",
			"code":       500 + i,
			"msg":        "fatal timeout",
			"is_network": i%2 == 0,
		}
		err := tap.Sample(ctx, "stream:logs", payload)
		require.NoError(t, err)
	}

	// Flush to ensure stats are in the backend
	require.NoError(t, tap.Flush())

	// Reconstruct schema
	schemaNode, err := tap.GetSchema(ctx, "stream:logs:ERROR")
	require.NoError(t, err)
	require.NotNil(t, schemaNode)

	// Verify inferred fields
	require.Contains(t, schemaNode.Children, "level")
	require.Contains(t, schemaNode.Children, "code")
	require.Contains(t, schemaNode.Children, "msg")
	require.Contains(t, schemaNode.Children, "is_network")

	assert.Equal(t, "string", schemaNode.Children["level"].Type)
	assert.Equal(t, "number", schemaNode.Children["code"].Type)
	assert.Equal(t, "boolean", schemaNode.Children["is_network"].Type)
}
