package sync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/cuprite-io/flux/cmd/flux/tui/client"
	"github.com/cuprite-io/flux/types"
)

// SearchResult represents a single match found during local snapshot search.
type SearchResult struct {
	Type    string `json:"type"` // "circuit", "key", "alert", "schema"
	ID      string `json:"id"`
	Title   string `json:"title"`
	Snippet string `json:"snippet"`
	Detail  any    `json:"detail,omitempty"`
}

// FormatBytes formats a byte count into human-readable representation (e.g. "1.4MB", "350KB").
func FormatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB", float64(b)/float64(div), "KMGTPE"[exp])
}

// SnapshotStore provides a thread-safe, in-memory cache of remote cluster state.
// When Local Sync is enabled, operators can perform sub-millisecond searches,
// regex filtering, and offline inspection directly from local laptop RAM.
type SnapshotStore struct {
	mu          sync.RWMutex
	status      *client.StatusResponse
	circuits    []client.CircuitSummary
	circuitsMap map[string]client.CircuitSummary
	keys        []client.CacheKeyInfo
	details     map[string]*client.CacheKeyDetail
	alerts      map[string][]client.AlertRecord
	autopilot   []client.AutopilotSession
	topology    *client.ClusterTopology
	lastSync    time.Time
	totalBytes  int64
	synced      bool
}

// NewStore creates an empty, initialized SnapshotStore.
func NewStore() *SnapshotStore {
	return &SnapshotStore{
		circuitsMap: make(map[string]client.CircuitSummary),
		details:     make(map[string]*client.CacheKeyDetail),
		alerts:      make(map[string][]client.AlertRecord),
	}
}

// SetData atomically updates the local snapshot with newly synchronized data.
func (s *SnapshotStore) SetData(
	status *client.StatusResponse,
	circuits []client.CircuitSummary,
	keys []client.CacheKeyInfo,
	details map[string]*client.CacheKeyDetail,
	alerts map[string][]client.AlertRecord,
	autopilot []client.AutopilotSession,
	totalBytes int64,
) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.status = status
	s.circuits = circuits
	s.circuitsMap = make(map[string]client.CircuitSummary, len(circuits))
	for _, c := range circuits {
		s.circuitsMap[c.ID] = c
	}

	s.keys = keys
	s.details = details
	if s.details == nil {
		s.details = make(map[string]*client.CacheKeyDetail)
	}

	s.alerts = alerts
	if s.alerts == nil {
		s.alerts = make(map[string][]client.AlertRecord)
	}

	s.autopilot = autopilot
	s.totalBytes = totalBytes
	s.lastSync = time.Now()
	s.synced = true
}

// Clear resets and wipes the local snapshot from RAM.
func (s *SnapshotStore) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.status = nil
	s.circuits = nil
	s.circuitsMap = make(map[string]client.CircuitSummary)
	s.keys = nil
	s.details = make(map[string]*client.CacheKeyDetail)
	s.alerts = make(map[string][]client.AlertRecord)
	s.autopilot = nil
	s.topology = nil
	s.totalBytes = 0
	s.lastSync = time.Time{}
	s.synced = false
}

// IsSynced returns true if a snapshot is currently cached and active.
func (s *SnapshotStore) IsSynced() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.synced
}

// Stats returns the active key count, total byte size, and last sync timestamp.
func (s *SnapshotStore) Stats() (keyCount int, totalBytes int64, lastSync time.Time) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.keys) + len(s.circuits), s.totalBytes, s.lastSync
}

// Status returns the cached cluster status response.
func (s *SnapshotStore) Status() *client.StatusResponse {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.status
}

// Circuits returns all cached circuits.
func (s *SnapshotStore) Circuits() []client.CircuitSummary {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]client.CircuitSummary, len(s.circuits))
	copy(result, s.circuits)
	return result
}

// Circuit retrieves a cached circuit definition by ID.
func (s *SnapshotStore) Circuit(id string) (*client.CircuitSummary, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.circuitsMap[id]
	if !ok {
		return nil, false
	}
	return &c, true
}

// Keys returns cached cache keys, optionally filtered by prefix.
func (s *SnapshotStore) Keys(prefix string) []client.CacheKeyInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if prefix == "" {
		result := make([]client.CacheKeyInfo, len(s.keys))
		copy(result, s.keys)
		return result
	}

	var matched []client.CacheKeyInfo
	for _, k := range s.keys {
		if strings.HasPrefix(k.Key, prefix) || strings.Contains(k.Key, prefix) {
			matched = append(matched, k)
		}
	}
	return matched
}

// Detail retrieves cached inspection data for a specific key.
func (s *SnapshotStore) Detail(key string) (*client.CacheKeyDetail, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	d, ok := s.details[key]
	if !ok {
		return nil, false
	}
	return d, true
}

// Alerts returns cached alert records for a specific circuit ID.
func (s *SnapshotStore) Alerts(circuitID string) []client.AlertRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()
	records, ok := s.alerts[circuitID]
	if !ok {
		return nil
	}
	result := make([]client.AlertRecord, len(records))
	copy(result, records)
	return result
}

// Autopilot returns cached autopilot sessions.
func (s *SnapshotStore) Autopilot() []client.AutopilotSession {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]client.AutopilotSession, len(s.autopilot))
	copy(result, s.autopilot)
	return result
}

// SetTopology caches the cluster topology in the snapshot store.
func (s *SnapshotStore) SetTopology(topo *client.ClusterTopology) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.topology = topo
}

// Topology returns cached cluster topology.
func (s *SnapshotStore) Topology() *client.ClusterTopology {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.topology == nil {
		return nil
	}
	tCopy := *s.topology
	nodes := make([]client.ClusterNode, len(s.topology.Nodes))
	copy(nodes, s.topology.Nodes)
	tCopy.Nodes = nodes
	return &tCopy
}

// Search performs a local regex or substring search across circuits, keys, and alerts.
func (s *SnapshotStore) Search(query string) []SearchResult {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if strings.TrimSpace(query) == "" {
		return nil
	}

	var results []SearchResult
	lowerQuery := strings.ToLower(query)

	// Try compiling as regex; fall back to case-insensitive substring
	rx, err := regexp.Compile("(?i)" + query)
	useRegex := err == nil

	match := func(target string) bool {
		if target == "" {
			return false
		}
		if useRegex {
			return rx.MatchString(target)
		}
		return strings.Contains(strings.ToLower(target), lowerQuery)
	}

	// 1. Search Circuits
	for _, c := range s.circuits {
		matched := false
		snippet := ""

		if match(c.ID) {
			matched = true
			snippet = fmt.Sprintf("Circuit ID: %s (status: %s, nodes: %d)", c.ID, c.Status, c.NodeCount)
		} else if match(c.RootCondition) {
			matched = true
			snippet = fmt.Sprintf("Condition: %s", c.RootCondition)
		} else {
			for _, tag := range c.Tags {
				if match(tag) {
					matched = true
					snippet = fmt.Sprintf("Tag: %s", tag)
					break
				}
			}
		}

		if matched {
			results = append(results, SearchResult{
				Type:    "circuit",
				ID:      c.ID,
				Title:   fmt.Sprintf("Circuit: %s", c.ID),
				Snippet: snippet,
				Detail:  c,
			})
		}
	}

	// 2. Search Cache Keys & Details
	for _, k := range s.keys {
		matched := false
		snippet := ""

		if match(k.Key) {
			matched = true
			snippet = fmt.Sprintf("Key: %s [%s, size: %d]", k.Key, strings.ToUpper(k.Type), k.Size)
		} else if detail, ok := s.details[k.Key]; ok && detail != nil {
			switch v := detail.Value.(type) {
			case string:
				if match(v) {
					matched = true
					snippet = truncateSnippet(v, 60)
				}
			case map[string]string:
				for f, val := range v {
					if match(f) || match(val) {
						matched = true
						snippet = fmt.Sprintf("Field %s: %s", f, truncateSnippet(val, 40))
						break
					}
				}
			}
		}

		if matched {
			results = append(results, SearchResult{
				Type:    "key",
				ID:      k.Key,
				Title:   fmt.Sprintf("Key: %s", k.Key),
				Snippet: snippet,
				Detail:  s.details[k.Key],
			})
		}
	}

	// 3. Search Alert Records
	for circuitID, records := range s.alerts {
		for _, a := range records {
			matched := false
			snippet := ""

			if match(a.Condition) {
				matched = true
				snippet = fmt.Sprintf("Alert Condition: %s", a.Condition)
			} else if match(a.SinkName) {
				matched = true
				snippet = fmt.Sprintf("Sink: %s", a.SinkName)
			} else if a.Payload != nil {
				if b, err := json.Marshal(a.Payload); err == nil && match(string(b)) {
					matched = true
					snippet = fmt.Sprintf("Payload: %s", truncateSnippet(string(b), 60))
				}
			}

			if matched {
				results = append(results, SearchResult{
					Type:    "alert",
					ID:      a.ID,
					Title:   fmt.Sprintf("Alert: %s (%s)", a.ID, circuitID),
					Snippet: snippet,
					Detail:  a,
				})
			}
		}
	}

	return results
}

func truncateSnippet(s string, maxLen int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.TrimSpace(s)
	if len(s) > maxLen {
		return s[:maxLen] + "..."
	}
	return s
}

// SnapshotClient wraps a SnapshotStore and provides a client.Client interface.
// Read operations are resolved locally from RAM with zero network overhead when the store is synced.
type SnapshotClient struct {
	store    *SnapshotStore
	fallback client.Client
}

// Client returns a client.Client backed by this SnapshotStore, with the provided fallback.
func (s *SnapshotStore) Client(fallback client.Client) client.Client {
	return &SnapshotClient{
		store:    s,
		fallback: fallback,
	}
}

func (c *SnapshotClient) GetStatus(ctx context.Context) (*client.StatusResponse, error) {
	if c.store.IsSynced() {
		if st := c.store.Status(); st != nil {
			return st, nil
		}
	}
	if c.fallback != nil {
		return c.fallback.GetStatus(ctx)
	}
	return nil, errors.New("no status available")
}

func (c *SnapshotClient) GetCircuits(ctx context.Context) ([]client.CircuitSummary, error) {
	if c.store.IsSynced() {
		return c.store.Circuits(), nil
	}
	if c.fallback != nil {
		return c.fallback.GetCircuits(ctx)
	}
	return nil, nil
}

func (c *SnapshotClient) GetCircuit(ctx context.Context, id string) (*client.CircuitSummary, error) {
	if c.store.IsSynced() {
		if circ, ok := c.store.Circuit(id); ok {
			return circ, nil
		}
	}
	if c.fallback != nil {
		return c.fallback.GetCircuit(ctx, id)
	}
	return nil, nil
}

func (c *SnapshotClient) PutCircuit(ctx context.Context, circuit *types.Circuit) (*client.CircuitSummary, error) {
	if c.fallback != nil {
		return c.fallback.PutCircuit(ctx, circuit)
	}
	return nil, errors.New("offline/read-only mode")
}

func (c *SnapshotClient) GetAutopilot(ctx context.Context) ([]client.AutopilotSession, error) {
	if c.store.IsSynced() {
		return c.store.Autopilot(), nil
	}
	if c.fallback != nil {
		return c.fallback.GetAutopilot(ctx)
	}
	return nil, nil
}

func (c *SnapshotClient) GetAlertHistory(ctx context.Context, circuitID string, limit int) ([]client.AlertRecord, error) {
	if c.store.IsSynced() {
		alerts := c.store.Alerts(circuitID)
		if limit > 0 && len(alerts) > limit {
			return alerts[:limit], nil
		}
		return alerts, nil
	}
	if c.fallback != nil {
		return c.fallback.GetAlertHistory(ctx, circuitID, limit)
	}
	return nil, nil
}

func (c *SnapshotClient) StreamAlerts(ctx context.Context) (<-chan client.AlertEvent, <-chan error, error) {
	if c.fallback != nil {
		return c.fallback.StreamAlerts(ctx)
	}
	return nil, nil, errors.New("alert streaming not supported in offline mode")
}

func (c *SnapshotClient) SubmitFeedback(ctx context.Context, req client.FeedbackRequest) (*client.FeedbackResponse, error) {
	if c.fallback != nil {
		return c.fallback.SubmitFeedback(ctx, req)
	}
	return nil, errors.New("offline/read-only mode")
}

func (c *SnapshotClient) DryRunEval(ctx context.Context, req client.EvalRequest) (*client.EvalResponse, error) {
	if c.fallback != nil {
		return c.fallback.DryRunEval(ctx, req)
	}
	return nil, errors.New("offline/read-only mode")
}

func (c *SnapshotClient) GetCacheKeys(ctx context.Context, prefix string, limit int) ([]client.CacheKeyInfo, error) {
	if c.store.IsSynced() {
		keys := c.store.Keys(prefix)
		if limit > 0 && len(keys) > limit {
			return keys[:limit], nil
		}
		return keys, nil
	}
	if c.fallback != nil {
		return c.fallback.GetCacheKeys(ctx, prefix, limit)
	}
	return nil, nil
}

func (c *SnapshotClient) InspectCacheKey(ctx context.Context, key string) (*client.CacheKeyDetail, error) {
	if c.store.IsSynced() {
		if d, ok := c.store.Detail(key); ok {
			return d, nil
		}
	}
	if c.fallback != nil {
		return c.fallback.InspectCacheKey(ctx, key)
	}
	return nil, errors.New("key not found in local snapshot")
}

func (c *SnapshotClient) GetClusterTopology(ctx context.Context) (*client.ClusterTopology, error) {
	if c.store.IsSynced() {
		if topo := c.store.Topology(); topo != nil {
			return topo, nil
		}
	}
	if c.fallback != nil {
		return c.fallback.GetClusterTopology(ctx)
	}
	return nil, errors.New("cluster topology not available")
}

func (c *SnapshotClient) Close() error {
	if c.fallback != nil {
		return c.fallback.Close()
	}
	return nil
}
