package history

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cuprite-io/flux/internal/cache"
	"github.com/cuprite-io/flux/internal/profiler"
)

// Standard feedback classifications.
const (
	FeedbackValid         = "valid"
	FeedbackFalsePositive = "false_positive"
	FeedbackNoisy         = "noisy"
	FeedbackMuted         = "muted"
)

// AlertRecord represents a single historical alert or sink dispatch event.
// All records are stored directly in CacheBackend (Capacitor) to maintain a 100% stateless Flux core.
type AlertRecord struct {
	ID        string         `json:"id"`
	Timestamp time.Time      `json:"timestamp"`
	CircuitID string         `json:"circuit_id"`
	NodeName  string         `json:"node_name"`
	SinkName  string         `json:"sink_name"`
	Condition string         `json:"condition,omitempty"`
	Payload   map[string]any `json:"payload,omitempty"`
	Feedback  *Feedback      `json:"feedback,omitempty"`
}

// Feedback represents operator feedback on an alert record.
type Feedback struct {
	Classification string    `json:"classification"` // e.g. "valid", "false_positive", "noisy", "muted"
	Reason         string    `json:"reason,omitempty"`
	Author         string    `json:"author,omitempty"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// AlertStats tracks historical alert counts and rates for a circuit.
type AlertStats struct {
	CircuitID      string            `json:"circuit_id"`
	TotalFired     uint64            `json:"total_fired"`
	LastFiredAt    time.Time         `json:"last_fired_at,omitempty"`
	BySink         map[string]uint64 `json:"by_sink"`
	RecentWindow1h int64             `json:"recent_window_1h"`
}

// Config configures historical alert retention in CacheBackend.
type Config struct {
	// Enabled toggles alert history retention.
	Enabled bool

	// TTL specifies the retention period for alert records in CacheBackend (default: 14 days).
	TTL time.Duration

	// MaxRecordsPerCircuit limits the number of retained alert records per circuit (default: 100).
	MaxRecordsPerCircuit int

	// SanitizePII scrubs sensitive fields and PII before writing to CacheBackend (default: true).
	SanitizePII bool

	// Async decouples history recording from the execution thread via background queue.
	Async bool

	// QueueSize is the capacity of the async queue channel buffer (default: 4096).
	QueueSize int
}

// DefaultConfig provides recommended defaults for alert history retention.
func DefaultConfig() Config {
	return Config{
		Enabled:              true,
		TTL:                  14 * 24 * time.Hour,
		MaxRecordsPerCircuit: 100,
		SanitizePII:          true,
		Async:                true,
		QueueSize:            4096,
	}
}

// Store coordinates historical alert retention directly in CacheBackend.
type Store struct {
	backend cache.CacheBackend
	cfg     Config

	taskQueue chan AlertRecord
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	closed    bool
	closeMu   sync.RWMutex
	inFlight  int64
	dropped   uint64
}

// New creates and initializes an Alert History Store backed directly by CacheBackend.
func New(cacheBackend cache.CacheBackend, cfg Config) (*Store, error) {
	if cacheBackend == nil {
		return nil, errors.New("history: cache backend cannot be nil")
	}

	if cfg.TTL <= 0 {
		cfg.TTL = 14 * 24 * time.Hour
	}
	if cfg.MaxRecordsPerCircuit <= 0 {
		cfg.MaxRecordsPerCircuit = 100
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 4096
	}

	ctx, cancel := context.WithCancel(context.Background())

	s := &Store{
		backend: cacheBackend,
		cfg:     cfg,
		ctx:     ctx,
		cancel:  cancel,
	}

	if cfg.Async {
		s.taskQueue = make(chan AlertRecord, cfg.QueueSize)
		s.wg.Add(1)
		go s.worker()
	}

	return s, nil
}

func (s *Store) worker() {
	defer s.wg.Done()

	for record := range s.taskQueue {
		_ = s.process(s.ctx, record)
		atomic.AddInt64(&s.inFlight, -1)
	}
}

// Dropped returns the count of dropped alert records due to a saturated async channel queue.
func (s *Store) Dropped() uint64 {
	return atomic.LoadUint64(&s.dropped)
}

// Record queues or synchronously writes an alert record into CacheBackend.
func (s *Store) Record(ctx context.Context, record AlertRecord) error {
	if s == nil || !s.cfg.Enabled {
		return nil
	}

	if record.CircuitID == "" {
		return errors.New("history: circuit_id cannot be empty")
	}

	if record.ID == "" {
		record.ID = GenerateAlertID()
	}
	if record.Timestamp.IsZero() {
		record.Timestamp = time.Now().UTC()
	}

	if !s.cfg.Async {
		return s.process(ctx, record)
	}

	s.closeMu.RLock()
	if s.closed {
		s.closeMu.RUnlock()
		return nil
	}

	select {
	case s.taskQueue <- record:
		atomic.AddInt64(&s.inFlight, 1)
	default:
		atomic.AddUint64(&s.dropped, 1)
	}
	s.closeMu.RUnlock()

	return nil
}

func (s *Store) process(ctx context.Context, record AlertRecord) error {
	// 1. Sanitize payload PII if enabled
	if s.cfg.SanitizePII && record.Payload != nil {
		record.Payload = profiler.SanitizeMap(record.Payload)
	}

	// 2. Persist record into partitioned circuit map in CacheBackend
	historyKey := "alert:history:" + record.CircuitID
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("history: failed to marshal alert record: %w", err)
	}

	_, _ = s.backend.MapSet(ctx, historyKey, record.ID, string(data), s.cfg.TTL)

	// 3. Register circuit in global alert index
	_, _ = s.backend.SetAdd(ctx, "alert:circuits", record.CircuitID)

	// 4. Update frequency and sink stats
	statsKey := "alert:stats:" + record.CircuitID
	_, _ = s.backend.MapIncrementBy(ctx, statsKey, "total_fired", 1.0)
	if record.SinkName != "" {
		_, _ = s.backend.MapIncrementBy(ctx, statsKey, "sink:"+record.SinkName, 1.0)
	}
	_, _ = s.backend.MapSet(ctx, statsKey, "last_fired", record.Timestamp.Format(time.RFC3339Nano), s.cfg.TTL)

	// 5. Update 1-hour rolling rate window
	rateKey := "alert:rate:" + record.CircuitID
	_, _ = s.backend.IncrementSlidingWindow(ctx, rateKey, 1*time.Hour)

	// 6. Enforce bounded capacity via MapRemove
	s.enforceCapacity(ctx, historyKey)

	return nil
}

func (s *Store) enforceCapacity(ctx context.Context, historyKey string) {
	all, err := s.backend.MapGetAll(ctx, historyKey)
	if err != nil || len(all) <= s.cfg.MaxRecordsPerCircuit {
		return
	}

	var oldestID string
	var oldestTime time.Time
	first := true

	for id, raw := range all {
		var rec AlertRecord
		if errJSON := json.Unmarshal([]byte(raw), &rec); errJSON == nil {
			if first || rec.Timestamp.Before(oldestTime) {
				oldestTime = rec.Timestamp
				oldestID = id
				first = false
			}
		}
	}

	if oldestID != "" {
		_, _ = s.backend.MapRemove(ctx, historyKey, oldestID)
	}
}

// GetAlerts queries recent alert records for a circuit sorted with newest first.
func (s *Store) GetAlerts(ctx context.Context, circuitID string, limit int) ([]AlertRecord, error) {
	if s == nil {
		return nil, errors.New("history: uninitialized store")
	}
	if circuitID == "" {
		return nil, errors.New("history: circuit_id required")
	}

	historyKey := "alert:history:" + circuitID
	all, err := s.backend.MapGetAll(ctx, historyKey)
	if err != nil {
		return nil, err
	}

	records := make([]AlertRecord, 0, len(all))
	for _, raw := range all {
		var rec AlertRecord
		if err := json.Unmarshal([]byte(raw), &rec); err == nil {
			records = append(records, rec)
		}
	}

	sort.Slice(records, func(i, j int) bool {
		return records[i].Timestamp.After(records[j].Timestamp)
	})

	if limit > 0 && len(records) > limit {
		records = records[:limit]
	}

	return records, nil
}

// GetAlert retrieves a specific alert record by circuit ID and alert ID.
func (s *Store) GetAlert(ctx context.Context, circuitID, alertID string) (*AlertRecord, error) {
	if s == nil {
		return nil, errors.New("history: uninitialized store")
	}

	historyKey := "alert:history:" + circuitID
	var rec AlertRecord
	found, err := s.backend.MapGetScan(ctx, historyKey, alertID, &rec)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("history: alert %s not found in circuit %s", alertID, circuitID)
	}

	return &rec, nil
}

// RecordFeedback attaches operator feedback directly to a stored alert record.
func (s *Store) RecordFeedback(ctx context.Context, circuitID, alertID string, fb Feedback) error {
	if s == nil {
		return errors.New("history: uninitialized store")
	}

	rec, err := s.GetAlert(ctx, circuitID, alertID)
	if err != nil {
		return err
	}

	if fb.UpdatedAt.IsZero() {
		fb.UpdatedAt = time.Now().UTC()
	}
	rec.Feedback = &fb

	historyKey := "alert:history:" + circuitID
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}

	_, err = s.backend.MapSet(ctx, historyKey, alertID, string(data), s.cfg.TTL)
	return err
}

// GetStats queries aggregated alert counts and rolling rates for a circuit.
func (s *Store) GetStats(ctx context.Context, circuitID string) (*AlertStats, error) {
	if s == nil {
		return nil, errors.New("history: uninitialized store")
	}

	stats := &AlertStats{
		CircuitID: circuitID,
		BySink:    make(map[string]uint64),
	}

	statsKey := "alert:stats:" + circuitID
	all, err := s.backend.MapGetAll(ctx, statsKey)
	if err == nil && all != nil {
		for k, v := range all {
			if k == "total_fired" {
				if parsed, err := strconv.ParseUint(v, 10, 64); err == nil {
					stats.TotalFired = parsed
				}
			} else if k == "last_fired" {
				if parsed, err := time.Parse(time.RFC3339Nano, v); err == nil {
					stats.LastFiredAt = parsed
				}
			} else if strings.HasPrefix(k, "sink:") {
				sinkName := strings.TrimPrefix(k, "sink:")
				if parsed, err := strconv.ParseUint(v, 10, 64); err == nil {
					stats.BySink[sinkName] = parsed
				}
			}
		}
	}

	rateKey := "alert:rate:" + circuitID
	recentCount, err := s.backend.IncrementSlidingWindow(ctx, rateKey, 1*time.Hour)
	if err == nil {
		stats.RecentWindow1h = recentCount
	}

	return stats, nil
}

// ListCircuitsWithAlerts returns all circuit IDs that have recorded alerts.
func (s *Store) ListCircuitsWithAlerts(ctx context.Context) ([]string, error) {
	if s == nil {
		return nil, errors.New("history: uninitialized store")
	}
	circuits, err := s.backend.SetMembers(ctx, "alert:circuits")
	if err != nil {
		return nil, err
	}
	sort.Strings(circuits)
	return circuits, nil
}

// Flush waits for all queued and in-flight alert tasks to complete.
func (s *Store) Flush() error {
	if s == nil {
		return nil
	}
	if s.cfg.Async && s.taskQueue != nil {
		for len(s.taskQueue) > 0 || atomic.LoadInt64(&s.inFlight) > 0 {
			time.Sleep(1 * time.Millisecond)
		}
	}
	return nil
}

// Close gracefully flushes and terminates the history store.
func (s *Store) Close() error {
	if s == nil {
		return nil
	}

	s.closeMu.Lock()
	if !s.closed {
		s.closed = true
		if s.cfg.Async && s.taskQueue != nil {
			close(s.taskQueue)
		}
	}
	s.closeMu.Unlock()

	if s.cfg.Async {
		s.wg.Wait()
		s.cancel()
	}

	return nil
}

// GenerateAlertID produces a collision-resistant identifier for alert records.
func GenerateAlertID() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return fmt.Sprintf("alt_%d_%s", time.Now().UTC().UnixNano(), hex.EncodeToString(b))
}
