package cache

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"time"
)

var (
	ErrKeyNotFound = errors.New("flux cache: key not found")
)

// CacheBackend defines the distributed cache and state interface required by Flux.
// It matches the capabilities of *capacitor.Capacitor.
type CacheBackend interface {
	Get(ctx context.Context, key string) (string, error)
	GetScan(ctx context.Context, key string, dst any) error
	Set(ctx context.Context, key string, val any, ttl time.Duration) error
	Delete(ctx context.Context, key string) error
	Exists(ctx context.Context, key string) (bool, error)
	Increment(ctx context.Context, key string) (int64, error)
	IncrementSlidingWindow(ctx context.Context, key string, window time.Duration) (int64, error)
	SetAdd(ctx context.Context, key string, member any) (bool, error)
	SetMembers(ctx context.Context, key string) ([]string, error)
	SetRemove(ctx context.Context, key string, member any) (bool, error)
	SetIsMember(ctx context.Context, key string, member any) (bool, error)
	MapSet(ctx context.Context, key, field string, value any, ttl time.Duration) (bool, error)
	MapGetScan(ctx context.Context, key, field string, dst any) (bool, error)
	MapGetAll(ctx context.Context, key string) (map[string]string, error)
	MapRemove(ctx context.Context, key, field string) (bool, error)
	Close() error
}

type cacheEntry struct {
	val       string
	expiresAt time.Time
}

type windowEntry struct {
	timestamps []time.Time
}

// MemoryCache provides a fast, thread-safe in-memory CacheBackend for local execution and testing.
type MemoryCache struct {
	mu      sync.RWMutex
	data    map[string]cacheEntry
	sets    map[string]map[string]struct{}
	maps    map[string]map[string]string
	windows map[string]*windowEntry
}

// NewMemoryCache creates a new in-memory CacheBackend.
func NewMemoryCache() *MemoryCache {
	return &MemoryCache{
		data:    make(map[string]cacheEntry),
		sets:    make(map[string]map[string]struct{}),
		maps:    make(map[string]map[string]string),
		windows: make(map[string]*windowEntry),
	}
}

func (m *MemoryCache) Get(ctx context.Context, key string) (string, error) {
	m.mu.RLock()
	entry, ok := m.data[key]
	if !ok {
		m.mu.RUnlock()
		return "", ErrKeyNotFound
	}
	if entry.expiresAt.IsZero() || time.Now().Before(entry.expiresAt) {
		val := entry.val
		m.mu.RUnlock()
		return val, nil
	}
	m.mu.RUnlock()

	// Slow path: expired entry lazy cleanup
	m.mu.Lock()
	if e, exists := m.data[key]; exists && !e.expiresAt.IsZero() && time.Now().After(e.expiresAt) {
		delete(m.data, key)
	}
	m.mu.Unlock()
	return "", ErrKeyNotFound
}

func (m *MemoryCache) GetScan(ctx context.Context, key string, dst any) error {
	val, err := m.Get(ctx, key)
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(val), dst)
}

func (m *MemoryCache) Set(ctx context.Context, key string, val any, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var strVal string
	switch v := val.(type) {
	case string:
		strVal = v
	case []byte:
		strVal = string(v)
	default:
		b, err := json.Marshal(val)
		if err != nil {
			return err
		}
		strVal = string(b)
	}

	var exp time.Time
	if ttl > 0 {
		exp = time.Now().Add(ttl)
	}

	m.data[key] = cacheEntry{
		val:       strVal,
		expiresAt: exp,
	}
	return nil
}

func (m *MemoryCache) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	delete(m.sets, key)
	delete(m.maps, key)
	delete(m.windows, key)
	return nil
}

func (m *MemoryCache) Exists(ctx context.Context, key string) (bool, error) {
	m.mu.RLock()
	entry, ok := m.data[key]
	if !ok {
		m.mu.RUnlock()
		return false, nil
	}
	if entry.expiresAt.IsZero() || time.Now().Before(entry.expiresAt) {
		m.mu.RUnlock()
		return true, nil
	}
	m.mu.RUnlock()

	// Slow path: expired entry lazy cleanup
	m.mu.Lock()
	if e, exists := m.data[key]; exists && !e.expiresAt.IsZero() && time.Now().After(e.expiresAt) {
		delete(m.data, key)
	}
	m.mu.Unlock()
	return false, nil
}

func (m *MemoryCache) Increment(ctx context.Context, key string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.data[key]
	var cur int64
	if ok {
		if !entry.expiresAt.IsZero() && time.Now().After(entry.expiresAt) {
			delete(m.data, key)
		} else if n, err := strconv.ParseInt(entry.val, 10, 64); err == nil {
			cur = n
		}
	}
	cur++
	m.data[key] = cacheEntry{val: strconv.FormatInt(cur, 10)}
	return cur, nil
}

func (m *MemoryCache) IncrementSlidingWindow(ctx context.Context, key string, window time.Duration) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-window)

	w, ok := m.windows[key]
	if !ok {
		w = &windowEntry{timestamps: make([]time.Time, 0, 16)}
		m.windows[key] = w
	}

	// Prune expired timestamps in-place without heap reallocation
	k := 0
	for _, t := range w.timestamps {
		if t.After(cutoff) {
			w.timestamps[k] = t
			k++
		}
	}
	w.timestamps = append(w.timestamps[:k], now)

	return int64(len(w.timestamps)), nil
}

func (m *MemoryCache) SetAdd(ctx context.Context, key string, member any) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	s, ok := m.sets[key]
	if !ok {
		s = make(map[string]struct{})
		m.sets[key] = s
	}

	mStr := formatMember(member)
	if _, exists := s[mStr]; exists {
		return false, nil
	}
	s[mStr] = struct{}{}
	return true, nil
}

func (m *MemoryCache) SetMembers(ctx context.Context, key string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	s, ok := m.sets[key]
	if !ok {
		return nil, nil
	}

	res := make([]string, 0, len(s))
	for k := range s {
		res = append(res, k)
	}
	return res, nil
}

func (m *MemoryCache) SetRemove(ctx context.Context, key string, member any) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	s, ok := m.sets[key]
	if !ok {
		return false, nil
	}

	mStr := formatMember(member)
	if _, exists := s[mStr]; !exists {
		return false, nil
	}
	delete(s, mStr)
	return true, nil
}

func (m *MemoryCache) SetIsMember(ctx context.Context, key string, member any) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	s, ok := m.sets[key]
	if !ok {
		return false, nil
	}

	mStr := formatMember(member)
	_, exists := s[mStr]
	return exists, nil
}

func (m *MemoryCache) MapSet(ctx context.Context, key, field string, value any, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	mp, ok := m.maps[key]
	if !ok {
		mp = make(map[string]string)
		m.maps[key] = mp
	}

	mp[field] = formatMember(value)
	return true, nil
}

func (m *MemoryCache) MapGetScan(ctx context.Context, key, field string, dst any) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	mp, ok := m.maps[key]
	if !ok {
		return false, nil
	}
	val, ok := mp[field]
	if !ok {
		return false, nil
	}

	if err := json.Unmarshal([]byte(val), dst); err != nil {
		return false, err
	}
	return true, nil
}

func (m *MemoryCache) MapGetAll(ctx context.Context, key string) (map[string]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	mp, ok := m.maps[key]
	if !ok {
		return nil, nil
	}

	res := make(map[string]string, len(mp))
	for k, v := range mp {
		res[k] = v
	}
	return res, nil
}

func (m *MemoryCache) MapRemove(ctx context.Context, key, field string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	mp, ok := m.maps[key]
	if !ok {
		return false, nil
	}
	if _, exists := mp[field]; !exists {
		return false, nil
	}
	delete(mp, field)
	return true, nil
}

func (m *MemoryCache) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = nil
	m.sets = nil
	m.maps = nil
	m.windows = nil
	return nil
}

func formatMember(m any) string {
	switch v := m.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		b, _ := json.Marshal(v)
		return string(b)
	}
}

// BoundedCache provides a thread-safe, bounded in-memory cache with O(1) FIFO eviction using a ring buffer.
type BoundedCache[K comparable, V any] struct {
	mu       sync.RWMutex
	capacity int
	items    map[K]V
	ring     []K
	head     int
	tail     int
	size     int
}

// NewBoundedCache initializes a BoundedCache with a maximum capacity.
func NewBoundedCache[K comparable, V any](capacity int) *BoundedCache[K, V] {
	if capacity <= 0 {
		capacity = 4096
	}
	return &BoundedCache[K, V]{
		capacity: capacity,
		items:    make(map[K]V, capacity),
		ring:     make([]K, capacity),
		head:     0,
		tail:     0,
		size:     0,
	}
}

// Get retrieves an item from the cache.
func (c *BoundedCache[K, V]) Get(key K) (V, bool) {
	c.mu.RLock()
	val, ok := c.items[key]
	c.mu.RUnlock()
	return val, ok
}

// Set stores an item, evicting the oldest item in O(1) time if at capacity.
func (c *BoundedCache[K, V]) Set(key K, val V) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.items[key]; exists {
		c.items[key] = val
		return
	}

	if c.size >= c.capacity {
		oldest := c.ring[c.head]
		delete(c.items, oldest)
		var zero K
		c.ring[c.head] = zero // Clear evicted reference for GC
		c.head = (c.head + 1) % c.capacity
		c.size--
	}

	c.items[key] = val
	c.ring[c.tail] = key
	c.tail = (c.tail + 1) % c.capacity
	c.size++
}

// LoadOrStore returns the existing value for the key if present.
// Otherwise, it stores and returns the given value in O(1) time.
func (c *BoundedCache[K, V]) LoadOrStore(key K, val V) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if existing, exists := c.items[key]; exists {
		return existing, true
	}

	if c.size >= c.capacity {
		oldest := c.ring[c.head]
		delete(c.items, oldest)
		var zero K
		c.ring[c.head] = zero // Clear evicted reference for GC
		c.head = (c.head + 1) % c.capacity
		c.size--
	}

	c.items[key] = val
	c.ring[c.tail] = key
	c.tail = (c.tail + 1) % c.capacity
	c.size++
	return val, false
}

// Len returns the current count of items in the cache.
func (c *BoundedCache[K, V]) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.items)
}
