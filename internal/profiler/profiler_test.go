package profiler_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cuprite-io/flux/internal/cache"
	"github.com/cuprite-io/flux/internal/profiler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProfiler_PIISanitization(t *testing.T) {
	ctx := context.Background()
	mc := cache.NewMemoryCache()

	cfg := profiler.DefaultConfig()
	cfg.Async = false // Synchronous for deterministic test assertion

	p, err := profiler.New(mc, cfg)
	require.NoError(t, err)
	defer p.Close()

	payload := map[string]any{
		"user_email": "john.doe@example.com",
		"credit_card": "4111-2222-3333-4444",
		"password":    "super_secret_pw",
		"api_key":     "sk_live_1234567890abcdef",
		"service":     "checkout-service",
		"level":       "ERROR",
		"status_code": 500,
	}

	err = p.Sample(ctx, "stream:auth", payload)
	require.NoError(t, err)

	profile, err := p.GetProfile(ctx, "stream:auth")
	require.NoError(t, err)
	require.NotNil(t, profile)
	require.Len(t, profile.Exemplars, 1)

	ex := profile.Exemplars[0].Payload
	// Assert sensitive keys are redacted
	assert.Equal(t, "[REDACTED]", ex["password"])
	assert.Equal(t, "[REDACTED]", ex["api_key"])

	// Assert PII values are masked
	assert.Contains(t, ex["user_email"], "****@example.com")
	assert.Contains(t, ex["credit_card"], "****4444")

	// Non-sensitive fields are preserved
	assert.Equal(t, "checkout-service", ex["service"])
	assert.Equal(t, "ERROR", ex["level"])
}

func TestProfiler_DeduplicationAndAdmission(t *testing.T) {
	ctx := context.Background()
	mc := cache.NewMemoryCache()

	cfg := profiler.DefaultConfig()
	cfg.Async = false

	p, err := profiler.New(mc, cfg)
	require.NoError(t, err)
	defer p.Close()

	// Ingest 50 identical healthy log events
	for i := 0; i < 50; i++ {
		payload := map[string]any{
			"level":       "INFO",
			"service":     "order-service",
			"status_code": 200,
			"message":     "request completed",
		}
		require.NoError(t, p.Sample(ctx, "stream:logs", payload))
	}

	profile, err := p.GetProfile(ctx, "stream:logs")
	require.NoError(t, err)
	assert.Equal(t, uint64(50), profile.TotalSampled)

	// Since all 50 payloads have identical fingerprint, only 1 exemplar is stored
	assert.Len(t, profile.Exemplars, 1)

	// Ingest a distinct ERROR payload
	errorPayload := map[string]any{
		"level":       "ERROR",
		"service":     "order-service",
		"status_code": 503,
		"message":     "database timeout",
	}
	require.NoError(t, p.Sample(ctx, "stream:logs", errorPayload))

	profile, err = p.GetProfile(ctx, "stream:logs")
	require.NoError(t, err)
	assert.Equal(t, uint64(51), profile.TotalSampled)
	// Now we have 2 distinct exemplars
	assert.Len(t, profile.Exemplars, 2)
}

func TestProfiler_BoundedCapacityEviction(t *testing.T) {
	ctx := context.Background()
	mc := cache.NewMemoryCache()

	cfg := profiler.DefaultConfig()
	cfg.Async = false
	cfg.MaxExemplars = 3 // Cap at 3 for test

	p, err := profiler.New(mc, cfg)
	require.NoError(t, err)
	defer p.Close()

	// Ingest 5 distinct payload shapes
	for i := 1; i <= 5; i++ {
		payload := map[string]any{
			"level": fmt.Sprintf("LEVEL_%d", i),
			"code":  200 + i,
		}
		require.NoError(t, p.Sample(ctx, "stream:test", payload))
		time.Sleep(5 * time.Millisecond) // Ensure unique timestamps
	}

	profile, err := p.GetProfile(ctx, "stream:test")
	require.NoError(t, err)
	assert.Equal(t, uint64(5), profile.TotalSampled)

	// Capacity was capped at 3, so only 3 exemplars are retained in CacheBackend
	assert.Len(t, profile.Exemplars, 3)

	// Oldest entries (LEVEL_1, LEVEL_2) should have been evicted via MapRemove
	levels := make([]string, 0)
	for _, ex := range profile.Exemplars {
		levels = append(levels, ex.Payload["level"].(string))
	}
	assert.Contains(t, levels, "LEVEL_5")
	assert.Contains(t, levels, "LEVEL_4")
	assert.Contains(t, levels, "LEVEL_3")
	assert.NotContains(t, levels, "LEVEL_1")
	assert.NotContains(t, levels, "LEVEL_2")
}

func TestProfiler_CategoricalsAndRanges(t *testing.T) {
	ctx := context.Background()
	mc := cache.NewMemoryCache()

	cfg := profiler.DefaultConfig()
	cfg.Async = false

	p, err := profiler.New(mc, cfg)
	require.NoError(t, err)
	defer p.Close()

	// Ingest events with different levels and status codes
	statuses := []float64{200, 200, 200, 404, 500, 503}
	levels := []string{"INFO", "INFO", "INFO", "WARN", "ERROR", "FATAL"}

	for i := range statuses {
		payload := map[string]any{
			"level":       levels[i],
			"status_code": statuses[i],
			"latency_ms":  float64((i + 1) * 50),
		}
		require.NoError(t, p.Sample(ctx, "stream:http", payload))
	}

	profile, err := p.GetProfile(ctx, "stream:http")
	require.NoError(t, err)

	// Verify Categorical values
	require.Contains(t, profile.Categoricals, "level")
	levelCats := profile.Categoricals["level"]
	assert.Equal(t, uint64(3), levelCats["INFO"])
	assert.Equal(t, uint64(1), levelCats["WARN"])
	assert.Equal(t, uint64(1), levelCats["ERROR"])
	assert.Equal(t, uint64(1), levelCats["FATAL"])

	// Verify Numerical Ranges
	require.Contains(t, profile.Ranges, "status_code")
	statusRange := profile.Ranges["status_code"]
	assert.Equal(t, 200.0, statusRange.Min)
	assert.Equal(t, 503.0, statusRange.Max)
	assert.Equal(t, uint64(6), statusRange.Count)

	require.Contains(t, profile.Ranges, "latency_ms")
	latencyRange := profile.Ranges["latency_ms"]
	assert.Equal(t, 50.0, latencyRange.Min)
	assert.Equal(t, 300.0, latencyRange.Max)
	assert.Equal(t, uint64(6), latencyRange.Count)
}

func TestProfiler_AsyncWorkerQueueAndFlush(t *testing.T) {
	ctx := context.Background()
	mc := cache.NewMemoryCache()

	cfg := profiler.DefaultConfig()
	cfg.Async = true // Asynchronous mode
	cfg.QueueSize = 100

	p, err := profiler.New(mc, cfg)
	require.NoError(t, err)
	defer p.Close()

	for i := 0; i < 20; i++ {
		payload := map[string]any{
			"device_id": fmt.Sprintf("sensor_%02d", i%5),
			"temp_c":    20.0 + float64(i)*0.5,
		}
		require.NoError(t, p.Sample(ctx, "stream:iot", payload))
	}

	// Flush async tasks
	require.NoError(t, p.Flush())

	profile, err := p.GetProfile(ctx, "stream:iot")
	require.NoError(t, err)
	assert.Equal(t, uint64(20), profile.TotalSampled)
	assert.Greater(t, len(profile.Exemplars), 0)
}
