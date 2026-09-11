package schematap

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cuprite-io/assay"
	"github.com/cuprite-io/flux/internal/cache"
)

// IngestionMode dictates how the SchemaTap names and isolates learned schemas.
type IngestionMode int

const (
	// ModeHierarchicalFallback tries Discriminator key first, then Structural Fingerprint, then Tag Union.
	ModeHierarchicalFallback IngestionMode = iota
	// ModeDiscriminatorOnly only uses configured discriminator keys; skips if not found.
	ModeDiscriminatorOnly
	// ModeFingerprintOnly always clusters schemas by their structural key fingerprints.
	ModeUnionOnly
)

// Config configures the SchemaTap engine.
type Config struct {
	// Enabled toggles runtime schema learning.
	Enabled bool

	// Async decouples schema learning from the Spark hot path using a background worker queue.
	// Default is true.
	Async bool

	// QueueSize is the capacity of the async sample channel buffer.
	// Default is 8192.
	QueueSize int

	// SampleRate defines the ratio of events to sample (0.0 to 1.0).
	// Default is 1.0 (100% of events sampled asynchronously).
	SampleRate float64

	// DiscriminatorKeys is a prioritized list of payload field keys to check for event variant naming
	// (e.g. ["type", "event_type", "level", "sensor_type"]).
	DiscriminatorKeys []string

	// FallbackToFingerprint allows falling back to sorted top-level key hash if discriminator key is missing.
	FallbackToFingerprint bool

	// AssayConfig provides low-level assay limits and flushing configuration.
	AssayConfig assay.Config
}

// DefaultConfig provides recommended defaults for high-performance schema sampling.
func DefaultConfig() Config {
	return Config{
		Enabled:    true,
		Async:      true,
		QueueSize:  8192,
		SampleRate: 1.0,
		DiscriminatorKeys: []string{
			"type",
			"event_type",
			"event",
			"level",
			"sensor_type",
			"msg_type",
			"action",
		},
		FallbackToFingerprint: true,
		AssayConfig: assay.Config{
			MaxDepth:         16,
			MaxPaths:         1000,
			MaxSchemas:       1000,
			MaxArrayElements: 5,
		},
	}
}

type sampleTask struct {
	defaultTag string
	payload    any
}

// SchemaTap orchestrates streaming schema inference over incoming Spark payloads using assay.
type SchemaTap struct {
	mu          sync.RWMutex
	sampler     *assay.Sampler
	cfg         Config
	discrimKeys []string

	taskQueue chan sampleTask
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	closed    bool
	closeMu   sync.RWMutex
	dropped   uint64
	sampleCtr uint64
}

// New creates and initializes a SchemaTap instance.
func New(cacheBackend cache.CacheBackend, cfg Config) (*SchemaTap, error) {
	if cacheBackend == nil {
		return nil, errors.New("schematap: cache backend cannot be nil")
	}

	sampler, err := assay.NewSampler(cacheBackend, cfg.AssayConfig)
	if err != nil {
		return nil, fmt.Errorf("schematap: failed to initialize assay sampler: %w", err)
	}

	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 8192
	}
	if cfg.SampleRate <= 0 {
		cfg.SampleRate = 1.0
	}

	ctx, cancel := context.WithCancel(context.Background())

	st := &SchemaTap{
		sampler:     sampler,
		cfg:         cfg,
		discrimKeys: cfg.DiscriminatorKeys,
		ctx:         ctx,
		cancel:      cancel,
	}

	if cfg.Async {
		st.taskQueue = make(chan sampleTask, cfg.QueueSize)
		st.wg.Add(1)
		go st.worker()
	}

	return st, nil
}

func (st *SchemaTap) worker() {
	defer st.wg.Done()

	for task := range st.taskQueue {
		schemaID := st.ResolveSchemaID(task.defaultTag, task.payload)
		_ = st.sampler.Sample(st.ctx, schemaID, task.payload)
	}
}

// Dropped returns the number of sample events dropped due to a saturated queue.
func (st *SchemaTap) Dropped() uint64 {
	return atomic.LoadUint64(&st.dropped)
}

// Sample inspects the payload, determines the schema ID using the 3-tier fallback strategy,
// and passes the payload to assay for schema learning.
// When Config.Async is enabled, this is a non-blocking O(1) channel push or drop, keeping Spark's hot path fast.
func (st *SchemaTap) Sample(ctx context.Context, defaultTag string, payload any) error {
	if st == nil || st.sampler == nil || !st.cfg.Enabled || payload == nil {
		return nil
	}

	// Sampling rate check (e.g., probabilistic / interval)
	if st.cfg.SampleRate < 1.0 {
		ctr := atomic.AddUint64(&st.sampleCtr, 1)
		// Deterministic stride sampling: 0.1 rate -> 1 in 10 events
		stride := uint64(1.0 / st.cfg.SampleRate)
		if stride > 1 && ctr%stride != 0 {
			return nil
		}
	}

	if defaultTag == "" {
		defaultTag = "stream:default"
	}

	if !st.cfg.Async {
		schemaID := st.ResolveSchemaID(defaultTag, payload)
		return st.sampler.Sample(ctx, schemaID, payload)
	}

	st.closeMu.RLock()
	if st.closed {
		st.closeMu.RUnlock()
		return nil
	}

	// For async processing, clone []byte payloads to prevent race conditions if caller reuses byte buffers
	safePayload := payload
	if b, ok := payload.([]byte); ok {
		cp := make([]byte, len(b))
		copy(cp, b)
		safePayload = cp
	}

	select {
	case st.taskQueue <- sampleTask{defaultTag: defaultTag, payload: safePayload}:
	default:
		// Saturated queue: drop non-blockingly to protect Spark's low-latency hot path
		atomic.AddUint64(&st.dropped, 1)
	}
	st.closeMu.RUnlock()

	return nil
}

// ResolveSchemaID applies the 3-tier fallback hierarchy:
// Tier 1: Discriminator Key Matching (e.g. payload.level == "ERROR" -> "stream:logs:ERROR")
// Tier 2: Structural Key Fingerprinting (e.g. sorted keys hash -> "stream:logs:shape_8f4a12b0")
// Tier 3: Unified Tag Union (fallback -> "stream:logs")
func (st *SchemaTap) ResolveSchemaID(defaultTag string, payload any) string {
	m := extractShallowMap(payload)

	// Tier 1: Discriminator Key Matching
	if len(m) > 0 && len(st.discrimKeys) > 0 {
		for _, k := range st.discrimKeys {
			if val, exists := m[k]; exists && val != nil {
				valStr := fmt.Sprintf("%v", val)
				valStr = strings.TrimSpace(valStr)
				if valStr != "" && len(valStr) < 64 {
					return fmt.Sprintf("%s:%s", defaultTag, valStr)
				}
			}
		}
	}

	// Tier 2: Structural Key Fingerprinting
	if st.cfg.FallbackToFingerprint && len(m) > 0 {
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		h := fnv.New32a()
		for _, k := range keys {
			h.Write([]byte(k))
			h.Write([]byte{';'})
		}
		return fmt.Sprintf("%s:shape_%08x", defaultTag, h.Sum32())
	}

	// Tier 3: Unified Tag Union
	return defaultTag
}

// GetSchema reconstructs the inferred schema tree from assay for the given schema ID.
func (st *SchemaTap) GetSchema(ctx context.Context, schemaID string) (*assay.SchemaNode, error) {
	if st == nil || st.sampler == nil {
		return nil, errors.New("schematap: sampler not initialized")
	}
	return st.sampler.GetSchema(ctx, schemaID)
}

// Flush flushes any pending queued tasks and accumulated schema metrics to the backend.
func (st *SchemaTap) Flush() error {
	if st == nil || st.sampler == nil {
		return nil
	}

	// If async, wait until taskQueue is drained
	if st.cfg.Async && st.taskQueue != nil {
		for len(st.taskQueue) > 0 {
			time.Sleep(1 * time.Millisecond)
		}
	}

	return st.sampler.Flush()
}

// Close gracefully drains the task queue and closes the underlying assay sampler.
func (st *SchemaTap) Close() error {
	if st == nil || st.sampler == nil {
		return nil
	}

	st.closeMu.Lock()
	if !st.closed {
		st.closed = true
		if st.cfg.Async && st.taskQueue != nil {
			close(st.taskQueue)
		}
	}
	st.closeMu.Unlock()

	if st.cfg.Async {
		st.wg.Wait()
		st.cancel()
	}

	return st.sampler.Close()
}

// extractShallowMap extracts top-level string keys without deeply traversing nested values.
func extractShallowMap(payload any) map[string]any {
	switch v := payload.(type) {
	case map[string]any:
		return v
	case []byte:
		var m map[string]any
		if err := json.Unmarshal(v, &m); err == nil {
			return m
		}
	case string:
		var m map[string]any
		if err := json.Unmarshal([]byte(v), &m); err == nil {
			return m
		}
	}

	val := reflect.ValueOf(payload)
	if val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil
		}
		val = val.Elem()
	}

	if val.Kind() == reflect.Struct {
		t := val.Type()
		m := make(map[string]any, t.NumField())
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if !f.IsExported() {
				continue
			}
			key := f.Name
			tag := f.Tag.Get("json")
			if tag != "" {
				parts := strings.Split(tag, ",")
				if parts[0] == "-" {
					continue
				}
				if parts[0] != "" {
					key = parts[0]
				}
			}
			m[key] = val.Field(i).Interface()
		}
		return m
	}

	return nil
}

