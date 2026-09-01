package cache

import (
	"context"
	"encoding/json"
	"errors"
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
	defer m.mu.RUnlock()

	entry, ok := m.data[key]
	if !ok {
		return "", ErrKeyNotFound
	}
	if !entry.expiresAt.IsZero() && time.Now().After(entry.expiresAt) {
		return "", ErrKeyNotFound
	}
	return entry.val, nil
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
	delete(m.windows, key)
	return nil
}

func (m *MemoryCache) Exists(ctx context.Context, key string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entry, ok := m.data[key]
	if !ok {
		return false, nil
	}
	if !entry.expiresAt.IsZero() && time.Now().After(entry.expiresAt) {
		return false, nil
	}
	return true, nil
}

func (m *MemoryCache) Increment(ctx context.Context, key string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.data[key]
	var cur int64
	if ok {
		var n int64
		if err := json.Unmarshal([]byte(entry.val), &n); err == nil {
			cur = n
		}
	}
	cur++
	b, _ := json.Marshal(cur)
	m.data[key] = cacheEntry{val: string(b)}
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

	// Prune expired timestamps
	valid := make([]time.Time, 0, len(w.timestamps)+1)
	for _, t := range w.timestamps {
		if t.After(cutoff) {
			valid = append(valid, t)
		}
	}
	valid = append(valid, now)
	w.timestamps = valid

	return int64(len(valid)), nil
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
