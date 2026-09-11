package cache

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBoundedCache_RingBufferEviction(t *testing.T) {
	c := NewBoundedCache[string, int](3)
	require.Equal(t, 0, c.Len())

	c.Set("k1", 1)
	c.Set("k2", 2)
	c.Set("k3", 3)
	assert.Equal(t, 3, c.Len())

	v, ok := c.Get("k1")
	assert.True(t, ok)
	assert.Equal(t, 1, v)

	// Overwrite existing key should not change size or order
	c.Set("k2", 20)
	assert.Equal(t, 3, c.Len())
	v, ok = c.Get("k2")
	assert.True(t, ok)
	assert.Equal(t, 20, v)

	// Inserting 4th item evicts oldest ("k1")
	c.Set("k4", 4)
	assert.Equal(t, 3, c.Len())

	_, ok = c.Get("k1")
	assert.False(t, ok, "k1 should have been evicted")

	v, ok = c.Get("k4")
	assert.True(t, ok)
	assert.Equal(t, 4, v)

	// Test LoadOrStore
	loaded, exists := c.LoadOrStore("k4", 400)
	assert.True(t, exists)
	assert.Equal(t, 4, loaded)

	// LoadOrStore with new key evicts oldest ("k2")
	loaded, exists = c.LoadOrStore("k5", 5)
	assert.False(t, exists)
	assert.Equal(t, 5, loaded)
	assert.Equal(t, 3, c.Len())

	_, ok = c.Get("k2")
	assert.False(t, ok, "k2 should have been evicted")
}

func TestBoundedCache_ConcurrentAccess(t *testing.T) {
	c := NewBoundedCache[string, string](100)
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				key := fmt.Sprintf("key-%d-%d", workerID, j)
				c.Set(key, fmt.Sprintf("val-%d", j))
				_, _ = c.Get(key)
				_, _ = c.LoadOrStore(key, "default")
			}
		}(i)
	}

	wg.Wait()
	assert.LessOrEqual(t, c.Len(), 100)
}

func TestMemoryCache_SlidingWindowAndTTL(t *testing.T) {
	ctx := context.Background()
	mc := NewMemoryCache()

	// Sliding window
	count, err := mc.IncrementSlidingWindow(ctx, "rate_limit:user1", 100*time.Millisecond)
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)

	count, err = mc.IncrementSlidingWindow(ctx, "rate_limit:user1", 100*time.Millisecond)
	require.NoError(t, err)
	assert.Equal(t, int64(2), count)

	// Set and Get with TTL
	err = mc.Set(ctx, "temp_key", "hello", 50*time.Millisecond)
	require.NoError(t, err)

	val, err := mc.Get(ctx, "temp_key")
	require.NoError(t, err)
	assert.Equal(t, "hello", val)

	time.Sleep(60 * time.Millisecond)

	_, err = mc.Get(ctx, "temp_key")
	assert.Error(t, err, "key should have expired")
}

func TestMemoryCache_MapIncrementBy(t *testing.T) {
	ctx := context.Background()
	mc := NewMemoryCache()
	defer mc.Close()

	val, err := mc.MapIncrementBy(ctx, "stats:user1", "logins", 1.0)
	require.NoError(t, err)
	assert.Equal(t, 1.0, val)

	val, err = mc.MapIncrementBy(ctx, "stats:user1", "logins", 2.5)
	require.NoError(t, err)
	assert.Equal(t, 3.5, val)

	all, err := mc.MapGetAll(ctx, "stats:user1")
	require.NoError(t, err)
	assert.Equal(t, "3.5", all["logins"])
}

