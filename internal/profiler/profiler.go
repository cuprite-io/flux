package profiler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cuprite-io/flux/internal/cache"
	"github.com/cuprite-io/flux/internal/compiler"
)

// StreamProfile aggregates recent sanitized exemplars, categorical frequencies, and numerical boundaries.
// All underlying data is queried directly from CacheBackend (Capacitor), maintaining a 100% stateless Flux core.
type StreamProfile struct {
	Tag          string                       `json:"tag"`
	TotalSampled uint64                       `json:"total_sampled"`
	UpdatedAt    time.Time                    `json:"updated_at"`
	Exemplars    []Exemplar                   `json:"exemplars"`
	Categoricals map[string]map[string]uint64 `json:"categoricals"`
	Ranges       map[string]NumberRange       `json:"ranges"`
}

// Exemplar represents a single PII-masked representative payload.
type Exemplar struct {
	Fingerprint string         `json:"fingerprint"`
	Timestamp   time.Time      `json:"timestamp"`
	Payload     map[string]any `json:"payload"`
}

// NumberRange tracks minimum and maximum boundaries for a numerical metric.
type NumberRange struct {
	Min   float64 `json:"min"`
	Max   float64 `json:"max"`
	Count uint64  `json:"count"`
}

// Config configures the streaming value profiler.
type Config struct {
	// Enabled toggles runtime profiling.
	Enabled bool

	// Async decouples profiling from the Spark hot path via a background worker queue.
	Async bool

	// QueueSize is the capacity of the async sample channel buffer.
	QueueSize int

	// SampleRate defines the ratio of events to sample (0.0 to 1.0).
	SampleRate float64

	// MaxExemplars is the maximum number of distinct exemplars retained per tag in CacheBackend.
	MaxExemplars int

	// TTL specifies the rolling expiration for profile entries in CacheBackend.
	TTL time.Duration

	// MaxCategoricalCard is the maximum cardinality per categorical field before capped.
	MaxCategoricalCard int

	// DiscriminatorKeys are prioritized keys whose values differentiate payload shapes.
	DiscriminatorKeys []string
}

// DefaultConfig provides recommended defaults for high-performance profiling.
func DefaultConfig() Config {
	return Config{
		Enabled:            true,
		Async:              true,
		QueueSize:          8192,
		SampleRate:         1.0,
		MaxExemplars:       32,
		TTL:                24 * time.Hour,
		MaxCategoricalCard: 25,
		DiscriminatorKeys: []string{
			"level", "type", "event_type", "status", "action", "msg_type", "service", "method", "severity",
		},
	}
}

type profileTask struct {
	tag     string
	payload any
}

// Profiler orchestrates streaming value profiling and exemplar retention directly in CacheBackend.
type Profiler struct {
	backend     cache.CacheBackend
	cfg         Config
	discrimKeys []string

	taskQueue chan profileTask
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	closed    bool
	closeMu   sync.RWMutex
	dropped   uint64
	sampleCtr uint64
	inFlight  int64
}

// New creates and initializes a Profiler instance backed directly by CacheBackend.
func New(cacheBackend cache.CacheBackend, cfg Config) (*Profiler, error) {
	if cacheBackend == nil {
		return nil, errors.New("profiler: cache backend cannot be nil")
	}

	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 8192
	}
	if cfg.SampleRate <= 0 {
		cfg.SampleRate = 1.0
	}
	if cfg.MaxExemplars <= 0 {
		cfg.MaxExemplars = 32
	}
	if cfg.TTL <= 0 {
		cfg.TTL = 24 * time.Hour
	}
	if cfg.MaxCategoricalCard <= 0 {
		cfg.MaxCategoricalCard = 25
	}
	if len(cfg.DiscriminatorKeys) == 0 {
		cfg.DiscriminatorKeys = DefaultConfig().DiscriminatorKeys
	}

	ctx, cancel := context.WithCancel(context.Background())

	p := &Profiler{
		backend:     cacheBackend,
		cfg:         cfg,
		discrimKeys: cfg.DiscriminatorKeys,
		ctx:         ctx,
		cancel:      cancel,
	}

	if cfg.Async {
		p.taskQueue = make(chan profileTask, cfg.QueueSize)
		p.wg.Add(1)
		go p.worker()
	}

	return p, nil
}

func (p *Profiler) worker() {
	defer p.wg.Done()

	for task := range p.taskQueue {
		_ = p.process(p.ctx, task.tag, task.payload)
		atomic.AddInt64(&p.inFlight, -1)
	}
}

// Dropped returns the number of profile events dropped due to a saturated channel queue.
func (p *Profiler) Dropped() uint64 {
	return atomic.LoadUint64(&p.dropped)
}

// Sample inspects an incoming payload and submits it to the profiler.
// When Config.Async is enabled, this is a non-blocking O(1) channel push or drop, keeping Spark's hot path fast.
func (p *Profiler) Sample(ctx context.Context, tag string, payload any) error {
	if p == nil || !p.cfg.Enabled || payload == nil {
		return nil
	}

	if p.cfg.SampleRate < 1.0 {
		ctr := atomic.AddUint64(&p.sampleCtr, 1)
		stride := uint64(1.0 / p.cfg.SampleRate)
		if stride > 1 && ctr%stride != 0 {
			return nil
		}
	}

	if tag == "" {
		tag = "stream:default"
	}

	if !p.cfg.Async {
		return p.process(ctx, tag, payload)
	}

	p.closeMu.RLock()
	if p.closed {
		p.closeMu.RUnlock()
		return nil
	}

	safePayload := payload
	if b, ok := payload.([]byte); ok {
		cp := make([]byte, len(b))
		copy(cp, b)
		safePayload = cp
	}

	select {
	case p.taskQueue <- profileTask{tag: tag, payload: safePayload}:
		atomic.AddInt64(&p.inFlight, 1)
	default:
		atomic.AddUint64(&p.dropped, 1)
	}
	p.closeMu.RUnlock()

	return nil
}

// process handles admission, PII sanitization, and CacheBackend atomic persistence.
func (p *Profiler) process(ctx context.Context, tag string, payload any) error {
	m := extractMap(payload)
	if len(m) == 0 {
		return nil
	}

	// 1. Increment total sampled counter in CacheBackend
	statsKey := "profiler:stats:" + tag
	_, _ = p.backend.MapIncrementBy(ctx, statsKey, "total_sampled", 1.0)

	// 2. Track all field names in CacheBackend Set
	fieldsKey := "profiler:fields:" + tag
	for k := range m {
		_, _ = p.backend.SetAdd(ctx, fieldsKey, k)
	}

	// 3. Compute structural & discriminator fingerprint
	fp := ComputeFingerprint(tag, m, p.discrimKeys)

	// 4. Admission check: Does this exemplar fingerprint already exist in CacheBackend?
	exemplarsKey := "profiler:exemplars:" + tag
	var existing string
	found, _ := p.backend.MapGetScan(ctx, exemplarsKey, fp, &existing)

	if !found {
		// Novel shape or error variant: Sanitize PII and store into CacheBackend
		sanitized := SanitizeMap(m)
		ex := Exemplar{
			Fingerprint: fp,
			Timestamp:   time.Now().UTC(),
			Payload:     sanitized,
		}
		data, err := json.Marshal(ex)
		if err == nil {
			_, _ = p.backend.MapSet(ctx, exemplarsKey, fp, string(data), p.cfg.TTL)

			// Enforce bounded size via MapRemove
			p.enforceExemplarCap(ctx, exemplarsKey)
		}
	}

	// 5. Update Categorical and Numerical Range Statistics in CacheBackend
	p.updateMetrics(ctx, tag, m)

	return nil
}

func (p *Profiler) enforceExemplarCap(ctx context.Context, exemplarsKey string) {
	all, err := p.backend.MapGetAll(ctx, exemplarsKey)
	if err != nil || len(all) <= p.cfg.MaxExemplars {
		return
	}

	var oldestFP string
	var oldestTime time.Time

	first := true
	for fp, raw := range all {
		var ex Exemplar
		if errJSON := json.Unmarshal([]byte(raw), &ex); errJSON == nil {
			if first || ex.Timestamp.Before(oldestTime) {
				oldestTime = ex.Timestamp
				oldestFP = fp
				first = false
			}
		}
	}

	if oldestFP != "" {
		_, _ = p.backend.MapRemove(ctx, exemplarsKey, oldestFP)
	}
}

func (p *Profiler) updateMetrics(ctx context.Context, tag string, m map[string]any) {
	rangesKey := "profiler:ranges:" + tag

	for k, v := range m {
		if v == nil {
			continue
		}

		// Categorical string tracking
		if strVal, ok := v.(string); ok {
			strVal = strings.TrimSpace(strVal)
			if strVal != "" && len(strVal) < 64 && !compiler.OpIsPII(strVal) {
				catKey := "profiler:cat:" + tag + ":" + k
				currVals, _ := p.backend.MapGetAll(ctx, catKey)
				// Only increment if already known or cardinality budget remains
				if len(currVals) < p.cfg.MaxCategoricalCard || (currVals != nil && currVals[strVal] != "") {
					_, _ = p.backend.MapIncrementBy(ctx, catKey, strVal, 1.0)
				}
			}
			continue
		}

		// Numerical range tracking
		if numVal, ok := toFloat64(v); ok {
			var nr NumberRange
			found, _ := p.backend.MapGetScan(ctx, rangesKey, k, &nr)
			if !found {
				nr = NumberRange{
					Min:   numVal,
					Max:   numVal,
					Count: 1,
				}
			} else {
				if numVal < nr.Min {
					nr.Min = numVal
				}
				if numVal > nr.Max {
					nr.Max = numVal
				}
				nr.Count++
			}
			nrBytes, _ := json.Marshal(nr)
			_, _ = p.backend.MapSet(ctx, rangesKey, k, string(nrBytes), p.cfg.TTL)
		}
	}
}

// GetProfile queries CacheBackend and reconstructs the complete StreamProfile for a given tag.
func (p *Profiler) GetProfile(ctx context.Context, tag string) (*StreamProfile, error) {
	if p == nil {
		return nil, errors.New("profiler: uninitialized")
	}

	if tag == "" {
		tag = "stream:default"
	}

	profile := &StreamProfile{
		Tag:          tag,
		TotalSampled: 0,
		UpdatedAt:    time.Now().UTC(),
		Exemplars:    make([]Exemplar, 0),
		Categoricals: make(map[string]map[string]uint64),
		Ranges:       make(map[string]NumberRange),
	}

	// 1. Get total sampled count
	statsKey := "profiler:stats:" + tag
	statsMap, _ := p.backend.MapGetAll(ctx, statsKey)
	if statsMap != nil {
		if rawTot, ok := statsMap["total_sampled"]; ok {
			if parsed, err := strconv.ParseUint(rawTot, 10, 64); err == nil {
				profile.TotalSampled = parsed
			}
		}
	}

	// 2. Get all exemplars
	exemplarsKey := "profiler:exemplars:" + tag
	exMap, _ := p.backend.MapGetAll(ctx, exemplarsKey)
	for _, raw := range exMap {
		var ex Exemplar
		if err := json.Unmarshal([]byte(raw), &ex); err == nil {
			profile.Exemplars = append(profile.Exemplars, ex)
		}
	}
	sort.Slice(profile.Exemplars, func(i, j int) bool {
		return profile.Exemplars[i].Timestamp.After(profile.Exemplars[j].Timestamp)
	})

	// 3. Get all profiled field names
	fieldsKey := "profiler:fields:" + tag
	fields, _ := p.backend.SetMembers(ctx, fieldsKey)

	// 4. Query categoricals
	for _, f := range fields {
		catKey := "profiler:cat:" + tag + ":" + f
		catMap, _ := p.backend.MapGetAll(ctx, catKey)
		if len(catMap) > 0 {
			fieldVals := make(map[string]uint64, len(catMap))
			for val, countStr := range catMap {
				count, _ := strconv.ParseUint(countStr, 10, 64)
				fieldVals[val] = count
			}
			profile.Categoricals[f] = fieldVals
		}
	}

	// 5. Query numerical ranges
	rangesKey := "profiler:ranges:" + tag
	rangesMap, _ := p.backend.MapGetAll(ctx, rangesKey)
	for f, raw := range rangesMap {
		var nr NumberRange
		if err := json.Unmarshal([]byte(raw), &nr); err == nil {
			profile.Ranges[f] = nr
		}
	}

	return profile, nil
}

// Flush waits for any queued and in-flight async tasks to finish processing.
func (p *Profiler) Flush() error {
	if p == nil {
		return nil
	}
	if p.cfg.Async && p.taskQueue != nil {
		for len(p.taskQueue) > 0 || atomic.LoadInt64(&p.inFlight) > 0 {
			time.Sleep(1 * time.Millisecond)
		}
	}
	return nil
}

// Close gracefully drains the task queue and terminates background workers.
func (p *Profiler) Close() error {
	if p == nil {
		return nil
	}

	p.closeMu.Lock()
	if !p.closed {
		p.closed = true
		if p.cfg.Async && p.taskQueue != nil {
			close(p.taskQueue)
		}
	}
	p.closeMu.Unlock()

	if p.cfg.Async {
		p.wg.Wait()
		p.cancel()
	}

	return nil
}

// ComputeFingerprint generates a deterministic 16-hex hash from the tag, discriminator values, and keys.
func ComputeFingerprint(tag string, m map[string]any, discrimKeys []string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	h := sha256.New()
	h.Write([]byte(tag))
	h.Write([]byte{';'})

	for _, dk := range discrimKeys {
		if val, ok := m[dk]; ok && val != nil {
			h.Write(fmt.Appendf(nil, "%s=%v;", dk, val))
		}
	}

	for _, k := range keys {
		h.Write([]byte(k))
		h.Write([]byte{','})
	}

	return hex.EncodeToString(h.Sum(nil))[:16]
}

// SanitizeMap creates a deeply scrubbed copy of m with all sensitive and PII fields masked.
func SanitizeMap(m map[string]any) map[string]any {
	res := make(map[string]any, len(m))
	for k, v := range m {
		lowerKey := strings.ToLower(k)
		if isSensitiveKey(lowerKey) {
			res[k] = "[REDACTED]"
			continue
		}
		res[k] = sanitizeValue(v)
	}
	return res
}

func sanitizeValue(v any) any {
	if v == nil {
		return nil
	}

	switch val := v.(type) {
	case string:
		if strings.Contains(val, "@") {
			return compiler.OpMaskEmail(val)
		}

		// Detect and mask credit card numbers (13-19 digits with optional hyphens or spaces)
		clean := strings.ReplaceAll(strings.ReplaceAll(val, "-", ""), " ", "")
		if len(clean) >= 13 && len(clean) <= 19 {
			allDigits := true
			for i := 0; i < len(clean); i++ {
				if clean[i] < '0' || clean[i] > '9' {
					allDigits = false
					break
				}
			}
			if allDigits {
				return compiler.OpMaskCard(val)
			}
		}

		if compiler.OpIsPII(val) {
			return "[REDACTED_PII]"
		}
		return val

	case map[string]any:
		return SanitizeMap(val)

	case []any:
		limit := len(val)
		if limit > 5 {
			limit = 5
		}
		sanitizedSlice := make([]any, limit)
		for i := 0; i < limit; i++ {
			sanitizedSlice[i] = sanitizeValue(val[i])
		}
		return sanitizedSlice

	default:
		return val
	}
}

func isSensitiveKey(key string) bool {
	return strings.Contains(key, "password") ||
		strings.Contains(key, "secret") ||
		strings.Contains(key, "token") ||
		strings.Contains(key, "api_key") ||
		strings.Contains(key, "apikey") ||
		strings.Contains(key, "private_key") ||
		strings.Contains(key, "credential")
}

func toFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case int32:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint64:
		return float64(n), true
	case uint32:
		return float64(n), true
	default:
		return 0, false
	}
}

func extractMap(payload any) map[string]any {
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
