package sync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/cuprite-io/flux/cmd/flux/tui/client"
)

// SyncStatus represents the active lifecycle status of the local synchronization engine.
type SyncStatus int

const (
	StatusDisabled SyncStatus = iota
	StatusSyncing
	StatusSynced
	StatusError
)

func (s SyncStatus) String() string {
	switch s {
	case StatusDisabled:
		return "DISABLED"
	case StatusSyncing:
		return "SYNCING"
	case StatusSynced:
		return "SYNCED"
	case StatusError:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

// Config configures client-side snapshot synchronization.
type Config struct {
	MaxKeys      int           // Maximum cache keys to pull (default: 500)
	InspectLimit int           // Maximum key details to eagerly inspect into RAM (default: 100)
	AlertLimit   int           // Maximum alert history records per circuit (default: 20)
	Timeout      time.Duration // Network request timeout for snapshot operation (default: 10s)
	Interval     time.Duration // Background auto-refresh interval when sync is enabled (default: 15s)
}

// DefaultConfig provides sensible defaults for local snapshot sync.
func DefaultConfig() Config {
	return Config{
		MaxKeys:      500,
		InspectLimit: 100,
		AlertLimit:   20,
		Timeout:      10 * time.Second,
		Interval:     15 * time.Second,
	}
}

// SyncStartedMsg is emitted when a local snapshot sync operation begins.
type SyncStartedMsg struct{}

// SyncProgressMsg delivers real-time progress updates during synchronization.
type SyncProgressMsg struct {
	Stage   string
	Current int
	Total   int
}

// SyncCompletedMsg is delivered when snapshot sync successfully finishes or fails.
type SyncCompletedMsg struct {
	KeyCount   int
	TotalBytes int64
	Duration   time.Duration
	Err        error
}

// SyncClearedMsg is delivered when local sync is toggled off and the snapshot is cleared.
type SyncClearedMsg struct{}

// EstimateObjectSize calculates an approximate byte footprint for an arbitrary object.
func EstimateObjectSize(v any) int64 {
	if v == nil {
		return 0
	}
	switch val := v.(type) {
	case string:
		return int64(len(val))
	case []byte:
		return int64(len(val))
	default:
		if b, err := json.Marshal(v); err == nil {
			return int64(len(b))
		}
	}
	return 64
}

// FetchSnapshotCmd pulls a bounded cluster metadata snapshot into the provided SnapshotStore.
func FetchSnapshotCmd(cli client.Client, store *SnapshotStore, cfg Config) tea.Cmd {
	return func() tea.Msg {
		if cli == nil {
			return SyncCompletedMsg{Err: errors.New("no client available")}
		}

		startTime := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
		defer cancel()

		var totalBytes int64

		// 1. Fetch Cluster Status
		status, err := cli.GetStatus(ctx)
		if err != nil {
			return SyncCompletedMsg{Err: fmt.Errorf("failed fetching status: %w", err)}
		}
		totalBytes += EstimateObjectSize(status)

		// 2. Fetch Circuits
		circuits, err := cli.GetCircuits(ctx)
		if err != nil {
			return SyncCompletedMsg{Err: fmt.Errorf("failed fetching circuits: %w", err)}
		}
		totalBytes += EstimateObjectSize(circuits)

		// 3. Fetch Cache Keys
		keys, err := cli.GetCacheKeys(ctx, "", cfg.MaxKeys)
		if err != nil {
			// Cache keys might be empty or restricted, keep going with circuits
			keys = nil
		}
		totalBytes += EstimateObjectSize(keys)

		// 4. Eagerly inspect top cache key details
		details := make(map[string]*client.CacheKeyDetail)
		inspectCount := len(keys)
		if inspectCount > cfg.InspectLimit {
			inspectCount = cfg.InspectLimit
		}

		for i := 0; i < inspectCount; i++ {
			k := keys[i]
			if detail, err := cli.InspectCacheKey(ctx, k.Key); err == nil && detail != nil {
				details[k.Key] = detail
				totalBytes += EstimateObjectSize(detail)
			}
		}

		// 5. Fetch Alert History for active circuits
		alerts := make(map[string][]client.AlertRecord)
		for _, c := range circuits {
			if records, err := cli.GetAlertHistory(ctx, c.ID, cfg.AlertLimit); err == nil && len(records) > 0 {
				alerts[c.ID] = records
				totalBytes += EstimateObjectSize(records)
			}
		}

		// 6. Fetch Autopilot Sessions
		autopilot, _ := cli.GetAutopilot(ctx)
		totalBytes += EstimateObjectSize(autopilot)

		// 7. Atomically populate store
		store.SetData(status, circuits, keys, details, alerts, autopilot, totalBytes)

		totalKeys := len(circuits) + len(keys)
		return SyncCompletedMsg{
			KeyCount:   totalKeys,
			TotalBytes: totalBytes,
			Duration:   time.Since(startTime),
		}
	}
}

// Syncer orchestrates the background synchronization engine state for the TUI cockpit.
type Syncer struct {
	cfg        Config
	store      *SnapshotStore
	status     SyncStatus
	keyCount   int
	totalBytes int64
	lastSync   time.Time
	lastErr    error
}

// NewSyncer initializes a Syncer bound to the given SnapshotStore.
func NewSyncer(store *SnapshotStore, cfg Config) *Syncer {
	if cfg.MaxKeys <= 0 {
		cfg = DefaultConfig()
	}
	return &Syncer{
		cfg:    cfg,
		store:  store,
		status: StatusDisabled,
	}
}

// Store returns the underlying local SnapshotStore.
func (s *Syncer) Store() *SnapshotStore {
	return s.store
}

// Status returns the current lifecycle status.
func (s *Syncer) Status() SyncStatus {
	return s.status
}

// IsEnabled returns true if local sync is active.
func (s *Syncer) IsEnabled() bool {
	return s.status != StatusDisabled
}

// SetEnabled enables or disables local sync.
func (s *Syncer) SetEnabled(enabled bool, cli client.Client) tea.Cmd {
	if enabled {
		s.status = StatusSyncing
		s.lastErr = nil
		return FetchSnapshotCmd(cli, s.store, s.cfg)
	}

	s.status = StatusDisabled
	s.store.Clear()
	s.keyCount = 0
	s.totalBytes = 0
	s.lastErr = nil
	return func() tea.Msg {
		return SyncClearedMsg{}
	}
}

// HandleCompleted processes a completed sync message.
func (s *Syncer) HandleCompleted(msg SyncCompletedMsg) {
	if msg.Err != nil {
		s.status = StatusError
		s.lastErr = msg.Err
		return
	}

	s.status = StatusSynced
	s.keyCount = msg.KeyCount
	s.totalBytes = msg.TotalBytes
	s.lastSync = time.Now()
	s.lastErr = nil
}

// HeaderBadge returns the formatted header badge string (e.g. "[SYNCED: 1.4MB, 120 keys]").
func (s *Syncer) HeaderBadge() string {
	switch s.status {
	case StatusDisabled:
		return "Local Sync: OFF"
	case StatusSyncing:
		return "Local Sync: [SYNCING...]"
	case StatusSynced:
		return fmt.Sprintf("Local Sync: [SYNCED: %s, %d keys]", FormatBytes(s.totalBytes), s.keyCount)
	case StatusError:
		if s.lastErr != nil {
			return fmt.Sprintf("Local Sync: [SYNC ERR: %s]", s.lastErr.Error())
		}
		return "Local Sync: [SYNC ERR]"
	default:
		return "Local Sync: OFF"
	}
}

// Client returns a client.Client that reads from the local snapshot store when synced.
func (s *Syncer) Client(fallback client.Client) client.Client {
	return s.store.Client(fallback)
}

