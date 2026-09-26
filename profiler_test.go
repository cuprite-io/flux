package flux_test

import (
	"context"
	"testing"

	"github.com/cuprite-io/flux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEngine_StreamingValueProfiling(t *testing.T) {
	ctx := context.Background()

	// Initialize Engine with Profiling and SchemaLearning enabled
	eng, err := flux.New(
		flux.WithProfiling(true),
		flux.WithProfilerDiscriminators("level", "service"),
		flux.WithProfilerMaxExemplars(10),
	)
	require.NoError(t, err)
	defer eng.Close()

	prof := eng.Profiler()
	require.NotNil(t, prof)

	type LogRecord struct {
		Service    string  `json:"service"`
		Level      string  `json:"level"`
		StatusCode float64 `json:"status_code"`
		LatencyMs  float64 `json:"latency_ms"`
		UserEmail  string  `json:"user_email"`
		CardNumber string  `json:"card_number"`
		Message    string  `json:"message"`
	}

	// 1. Ingest 10 healthy log records (duplicate shape and level)
	for i := 0; i < 10; i++ {
		rec := LogRecord{
			Service:    "order-service",
			Level:      "INFO",
			StatusCode: 200,
			LatencyMs:  25.0 + float64(i)*2,
			UserEmail:  "customer@store.com",
			CardNumber: "4111-2222-3333-1111",
			Message:    "order placed successfully",
		}
		res, err := eng.Spark(ctx, rec, "stream:orders")
		require.NoError(t, err)
		assert.True(t, res.Passed)
	}

	// 2. Ingest 2 distinct error records
	errorRecord := LogRecord{
		Service:    "inventory-api",
		Level:      "ERROR",
		StatusCode: 503,
		LatencyMs:  1250.0,
		UserEmail:  "admin@store.com",
		CardNumber: "4222-3333-4444-2222",
		Message:    "database connection timeout",
	}
	for i := 0; i < 2; i++ {
		res, err := eng.Spark(ctx, errorRecord, "stream:orders")
		require.NoError(t, err)
		assert.True(t, res.Passed)
	}

	// Flush profiler queue
	require.NoError(t, prof.Flush())

	// Query reconstructed profile from CacheBackend
	profile, err := prof.GetProfile(ctx, "stream:orders")
	require.NoError(t, err)
	require.NotNil(t, profile)

	// Verify total counts
	assert.Equal(t, uint64(12), profile.TotalSampled)

	// Verify deduplicated exemplars: exactly 2 distinct shapes (1 INFO, 1 ERROR)
	assert.Len(t, profile.Exemplars, 2)

	// Verify PII masking in exemplars
	for _, ex := range profile.Exemplars {
		email, _ := ex.Payload["user_email"].(string)
		card, _ := ex.Payload["card_number"].(string)
		assert.Contains(t, email, "****@store.com")
		assert.Contains(t, card, "****")
		assert.NotContains(t, email, "customer@store.com")
		assert.NotContains(t, email, "admin@store.com")
		assert.NotContains(t, card, "4111-2222-3333-1111")
		assert.NotContains(t, card, "4222-3333-4444-2222")
	}

	// Verify categorical values
	require.Contains(t, profile.Categoricals, "level")
	assert.Equal(t, uint64(10), profile.Categoricals["level"]["INFO"])
	assert.Equal(t, uint64(2), profile.Categoricals["level"]["ERROR"])

	require.Contains(t, profile.Categoricals, "service")
	assert.Equal(t, uint64(10), profile.Categoricals["service"]["order-service"])
	assert.Equal(t, uint64(2), profile.Categoricals["service"]["inventory-api"])

	// Verify numerical boundaries
	require.Contains(t, profile.Ranges, "status_code")
	assert.Equal(t, 200.0, profile.Ranges["status_code"].Min)
	assert.Equal(t, 503.0, profile.Ranges["status_code"].Max)
	assert.Equal(t, uint64(12), profile.Ranges["status_code"].Count)

	require.Contains(t, profile.Ranges, "latency_ms")
	assert.Equal(t, 25.0, profile.Ranges["latency_ms"].Min)
	assert.Equal(t, 1250.0, profile.Ranges["latency_ms"].Max)
	assert.Equal(t, uint64(12), profile.Ranges["latency_ms"].Count)
}

func TestEngine_Profiler_DisabledByDefault(t *testing.T) {
	eng, err := flux.New()
	require.NoError(t, err)
	defer eng.Close()

	assert.Nil(t, eng.Profiler())

	// Sparking should succeed without profiling overhead
	res, err := eng.Spark(context.Background(), `{"a": 1}`, "test")
	require.NoError(t, err)
	assert.True(t, res.Passed)
}
