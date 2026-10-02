package cluster

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cuprite-io/flux/internal/cache"
)

var (
	// ErrElectorStopped is returned when an operation is attempted on a stopped elector.
	ErrElectorStopped = errors.New("flux cluster: leader elector is stopped")
)

// NodeHeartbeat encapsulates heartbeat metadata published periodically by an active cluster node.
type NodeHeartbeat struct {
	NodeID    string            `json:"node_id"`
	UpdatedAt time.Time         `json:"updated_at"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// ElectorConfig specifies configuration parameters for the cluster leader elector.
type ElectorConfig struct {
	NodeID            string            // Unique identifier for this node (default: hostname-randomHex)
	MembershipKey     string            // Cache map key for node membership table (default: "flux:cluster:nodes")
	HeartbeatInterval time.Duration     // Cadence for publishing local heartbeats (default: 3s)
	HeartbeatTTL      time.Duration     // Maximum duration before an inactive node is declared dead (default: 10s)
	Metadata          map[string]string // Optional node metadata attributes
	OnElected         func()            // Callback invoked upon acquiring leadership
	OnRevoked         func()            // Callback invoked upon losing leadership
}

// DefaultElectorConfig provides production default settings.
func DefaultElectorConfig() ElectorConfig {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "node"
	}
	randomSuffix := make([]byte, 4)
	_, _ = rand.Read(randomSuffix)

	return ElectorConfig{
		NodeID:            fmt.Sprintf("%s-%s", hostname, hex.EncodeToString(randomSuffix)),
		MembershipKey:     "flux:cluster:nodes",
		HeartbeatInterval: 3 * time.Second,
		HeartbeatTTL:      10 * time.Second,
	}
}

// Option configures an ElectorConfig instance.
type Option func(*ElectorConfig)

// WithNodeID sets a custom node identifier for this replica.
func WithNodeID(id string) Option {
	return func(c *ElectorConfig) {
		if id != "" {
			c.NodeID = id
		}
	}
}

// WithMembershipKey sets the cache map key for the node membership table.
func WithMembershipKey(key string) Option {
	return func(c *ElectorConfig) {
		if key != "" {
			c.MembershipKey = key
		}
	}
}

// WithHeartbeatInterval sets the periodic heartbeat broadcast cadence.
func WithHeartbeatInterval(d time.Duration) Option {
	return func(c *ElectorConfig) {
		if d > 0 {
			c.HeartbeatInterval = d
		}
	}
}

// WithHeartbeatTTL sets the staleness threshold after which inactive nodes are pruned.
func WithHeartbeatTTL(d time.Duration) Option {
	return func(c *ElectorConfig) {
		if d > 0 {
			c.HeartbeatTTL = d
		}
	}
}

// WithMetadata sets optional metadata tags published with this node's heartbeat.
func WithMetadata(m map[string]string) Option {
	return func(c *ElectorConfig) {
		c.Metadata = m
	}
}

// WithOnElected sets a hook invoked when this node becomes the cluster leader.
func WithOnElected(fn func()) Option {
	return func(c *ElectorConfig) {
		c.OnElected = fn
	}
}

// WithOnRevoked sets a hook invoked when this node yields cluster leadership.
func WithOnRevoked(fn func()) Option {
	return func(c *ElectorConfig) {
		c.OnRevoked = fn
	}
}

// LeaderElector defines the cluster leader election and membership interface.
type LeaderElector interface {
	// NodeID returns the identifier of the local node.
	NodeID() string

	// IsLeader returns true if the local node is currently the elected cluster leader.
	IsLeader() bool

	// LeaderID returns the identifier of the current cluster leader, or empty if unknown.
	LeaderID() string

	// IsMultiNode returns true if multiple active nodes are currently detected in the cluster.
	IsMultiNode() bool

	// ActiveNodes returns the list of all currently active node IDs, sorted lexicographically.
	ActiveNodes() []string

	// Evaluate performs a single heartbeat and leader election check against the cache backend.
	// Returns true if the local node is the leader after evaluation.
	Evaluate(ctx context.Context) (bool, error)

	// StepDown voluntarily yields leadership by removing the local node from the membership table.
	StepDown(ctx context.Context) error

	// Start launches the background heartbeat and election evaluation loop.
	Start(ctx context.Context) error

	// Stop gracefully terminates the elector loop and yields leadership.
	Stop(ctx context.Context) error
}

// Elector coordinates cluster leader election and membership using node heartbeats backed by CacheBackend.
// It automatically detects single-node vs multi-node topologies. In single-node environments, the node is
// immediately and unconditionally the leader. In multi-node deployments, the active node with the
// lexicographically lowest identifier is deterministically elected leader.
type Elector struct {
	backend cache.CacheBackend
	cfg     ElectorConfig

	mu            sync.RWMutex
	isLeader      bool
	leaderID      string
	isMultiNode   bool
	activeNodes   []string
	lastEvaluated time.Time

	cancelLoop context.CancelFunc
	stopped    atomic.Bool
	wg         sync.WaitGroup
}

// NewLeaderElector initializes a unified leader elector backed by CacheBackend.
// If backend is nil, an in-memory CacheBackend is automatically utilized.
func NewLeaderElector(backend cache.CacheBackend, opts ...Option) *Elector {
	if backend == nil {
		backend = cache.NewMemoryCache()
	}
	cfg := DefaultElectorConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	return &Elector{
		backend: backend,
		cfg:     cfg,
	}
}

// NodeID returns the unique identifier of this node.
func (e *Elector) NodeID() string {
	return e.cfg.NodeID
}

// IsLeader returns true if this node is currently the cluster leader.
func (e *Elector) IsLeader() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.isLeader
}

// LeaderID returns the identifier of the currently recognized cluster leader.
func (e *Elector) LeaderID() string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.leaderID
}

// IsMultiNode returns true if multiple active nodes are currently detected.
func (e *Elector) IsMultiNode() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.isMultiNode
}

// ActiveNodes returns a snapshot of all active node IDs in lexicographical order.
func (e *Elector) ActiveNodes() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return slices.Clone(e.activeNodes)
}

// Evaluate publishes the local node heartbeat and evaluates cluster leadership status.
// It detects single-node vs multi-node topologies and deterministically selects the leader.
func (e *Elector) Evaluate(ctx context.Context) (bool, error) {
	if e.stopped.Load() {
		return false, ErrElectorStopped
	}

	now := time.Now().UTC()
	hb := NodeHeartbeat{
		NodeID:    e.cfg.NodeID,
		UpdatedAt: now,
		Metadata:  e.cfg.Metadata,
	}

	data, err := json.Marshal(hb)
	if err != nil {
		return false, fmt.Errorf("flux cluster: failed to marshal heartbeat: %w", err)
	}

	// 1. Publish own heartbeat to the membership map
	if _, err := e.backend.MapSet(ctx, e.cfg.MembershipKey, e.cfg.NodeID, string(data), e.cfg.HeartbeatTTL); err != nil {
		return false, fmt.Errorf("flux cluster: failed to publish heartbeat: %w", err)
	}

	// 2. Fetch all registered cluster nodes
	entries, err := e.backend.MapGetAll(ctx, e.cfg.MembershipKey)
	if err != nil {
		return false, fmt.Errorf("flux cluster: failed to get cluster membership: %w", err)
	}

	// 3. Filter active nodes within TTL cutoff and prune stale nodes
	cutoff := now.Add(-e.cfg.HeartbeatTTL)
	active := make([]string, 0, len(entries))
	hasSelf := false

	for id, raw := range entries {
		var nodeHB NodeHeartbeat
		if err := json.Unmarshal([]byte(raw), &nodeHB); err != nil {
			continue
		}
		if nodeHB.UpdatedAt.Before(cutoff) {
			// Heartbeat expired; lazily remove stale node from membership
			_, _ = e.backend.MapRemove(ctx, e.cfg.MembershipKey, id)
			continue
		}
		active = append(active, id)
		if id == e.cfg.NodeID {
			hasSelf = true
		}
	}

	// Ensure local node is always recognized as active
	if !hasSelf {
		active = append(active, e.cfg.NodeID)
	}

	// 4. Sort deterministically
	slices.Sort(active)

	leaderID := active[0]
	amLeader := (leaderID == e.cfg.NodeID)
	multiNode := len(active) > 1

	// 5. Update state and trigger transitions outside lock
	e.mu.Lock()
	wasLeader := e.isLeader
	e.isLeader = amLeader
	e.leaderID = leaderID
	e.isMultiNode = multiNode
	e.activeNodes = active
	e.lastEvaluated = now
	e.mu.Unlock()

	if !wasLeader && amLeader {
		if e.cfg.OnElected != nil {
			e.cfg.OnElected()
		}
	} else if wasLeader && !amLeader {
		if e.cfg.OnRevoked != nil {
			e.cfg.OnRevoked()
		}
	}

	return amLeader, nil
}

// TryAcquire executes Evaluate, provided for compatibility with lease-based call sites.
func (e *Elector) TryAcquire(ctx context.Context) (bool, error) {
	return e.Evaluate(ctx)
}

// StepDown voluntarily yields leadership by removing this node from the membership table.
func (e *Elector) StepDown(ctx context.Context) error {
	e.mu.Lock()
	wasLeader := e.isLeader
	e.isLeader = false
	e.leaderID = ""
	e.mu.Unlock()

	if wasLeader && e.cfg.OnRevoked != nil {
		e.cfg.OnRevoked()
	}

	_, err := e.backend.MapRemove(ctx, e.cfg.MembershipKey, e.cfg.NodeID)
	return err
}

// Start launches the background heartbeat and leadership evaluation loop.
func (e *Elector) Start(ctx context.Context) error {
	if e.stopped.Load() {
		return ErrElectorStopped
	}

	e.mu.Lock()
	if e.cancelLoop != nil {
		e.mu.Unlock()
		return nil // already running
	}

	loopCtx, cancel := context.WithCancel(ctx)
	e.cancelLoop = cancel
	e.mu.Unlock()

	// Initial evaluation
	_, _ = e.Evaluate(loopCtx)

	e.wg.Add(1)
	go e.runLoop(loopCtx)

	return nil
}

// runLoop executes the periodic heartbeat broadcast and membership evaluation loop.
func (e *Elector) runLoop(ctx context.Context) {
	defer e.wg.Done()

	ticker := time.NewTicker(e.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			_ = e.StepDown(context.Background())
			return
		case <-ticker.C:
			_, _ = e.Evaluate(ctx)
		}
	}
}

// Stop terminates the background election loop and yields leadership.
func (e *Elector) Stop(ctx context.Context) error {
	if !e.stopped.CompareAndSwap(false, true) {
		return nil
	}

	e.mu.Lock()
	cancel := e.cancelLoop
	e.cancelLoop = nil
	e.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	e.wg.Wait()
	return e.StepDown(ctx)
}
