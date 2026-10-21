package cluster

import (
	"fmt"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/cuprite-io/flux/cmd/flux/tui/client"
)

// Node status indicators.
const (
	StatusHealthy = "HEALTHY"
	StatusWarning = "WARNING"
	StatusExpired = "EXPIRED"
)

// Event severity classifications.
const (
	SeverityInfo     = "INFO"
	SeverityWarning  = "WARNING"
	SeverityCritical = "CRITICAL"
)

// ClusterNode represents a discovered node replica in the distributed cluster.
type ClusterNode struct {
	NodeID          string            `json:"node_id"`
	Role            string            `json:"role"` // "LEADER" or "FOLLOWER"
	IsLeader        bool              `json:"is_leader"`
	IsSelf          bool              `json:"is_self"`
	Status          string            `json:"status"` // "HEALTHY", "WARNING", "EXPIRED"
	Addr            string            `json:"addr,omitempty"`
	StreamPort      int               `json:"stream_port,omitempty"`
	HeartbeatAge    time.Duration     `json:"heartbeat_age"`
	LeaseRemaining  time.Duration     `json:"lease_remaining"`
	UpdatedAt       time.Time         `json:"updated_at"`
	Uptime          string            `json:"uptime,omitempty"`
	Metadata        map[string]string `json:"metadata,omitempty"`
}

// FailoverEvent captures a consensus transition, leader election, or membership change.
type FailoverEvent struct {
	Timestamp  time.Time `json:"timestamp"`
	EventType  string    `json:"event_type"` // "ELECTION", "FAILOVER", "JOIN", "LEAVE", "DEGRADED", "RECOVERED"
	PrevLeader string    `json:"prev_leader,omitempty"`
	NewLeader  string    `json:"new_leader,omitempty"`
	Message    string    `json:"message"`
	Severity   string    `json:"severity"`
}

// TopologyTracker maintains state history and detects cluster transitions across polling intervals.
type TopologyTracker struct {
	mu          sync.RWMutex
	prevLeader  string
	prevNodes   map[string]string // nodeID -> status
	events      []FailoverEvent
	maxEvents   int
	initialized bool
}

// NewTopologyTracker initializes a TopologyTracker with bounded event storage.
func NewTopologyTracker(maxEvents int) *TopologyTracker {
	if maxEvents <= 0 {
		maxEvents = 50
	}
	return &TopologyTracker{
		prevNodes: make(map[string]string),
		events:    make([]FailoverEvent, 0, maxEvents),
		maxEvents: maxEvents,
	}
}

// ProcessTopology consumes a fresh cluster topology snapshot and detects state transitions.
func (t *TopologyTracker) ProcessTopology(topo *client.ClusterTopology, selfNodeID string) []ClusterNode {
	t.mu.Lock()
	defer t.mu.Unlock()

	if topo == nil {
		return nil
	}

	now := time.Now().UTC()
	nodes := make([]ClusterNode, 0, len(topo.Nodes))
	currentMap := make(map[string]string, len(topo.Nodes))

	for _, n := range topo.Nodes {
		role := "FOLLOWER"
		if n.IsLeader {
			role = "LEADER"
		}

		status := n.Status
		if status == "" {
			status = StatusHealthy
		}

		uptime := n.Uptime
		if uptime == "" {
			uptime = "active"
		}

		node := ClusterNode{
			NodeID:         n.NodeID,
			Role:           role,
			IsLeader:       n.IsLeader,
			IsSelf:         n.NodeID == selfNodeID,
			Status:         status,
			Addr:           n.Addr,
			StreamPort:     n.StreamPort,
			HeartbeatAge:   n.HeartbeatAge,
			LeaseRemaining: n.LeaseRemain,
			UpdatedAt:      n.UpdatedAt,
			Uptime:         uptime,
			Metadata:       n.Metadata,
		}
		nodes = append(nodes, node)
		currentMap[n.NodeID] = status
	}

	// Detect Consensus Transitions
	if !t.initialized {
		t.initialized = true
		t.prevLeader = topo.LeaderID
		t.prevNodes = currentMap
		if topo.LeaderID != "" {
			t.recordEventLocked(FailoverEvent{
				Timestamp: now,
				EventType: "ELECTION",
				NewLeader: topo.LeaderID,
				Message:   fmt.Sprintf("Initial consensus: %s recognized as elected cluster leader.", topo.LeaderID),
				Severity:  SeverityInfo,
			})
		}
	} else {
		// 1. Leader Shift / Failover Detection
		if topo.LeaderID != t.prevLeader && topo.LeaderID != "" {
			severity := SeverityWarning
			var reason string
			if t.prevLeader == "" {
				reason = fmt.Sprintf("Cluster consensus formed: %s elected leader.", topo.LeaderID)
				severity = SeverityInfo
			} else {
				reason = fmt.Sprintf("Failover detected: leader shifted from %s to %s (lexicographical convergence).", t.prevLeader, topo.LeaderID)
				severity = SeverityCritical
			}

			t.recordEventLocked(FailoverEvent{
				Timestamp:  now,
				EventType:  "FAILOVER",
				PrevLeader: t.prevLeader,
				NewLeader:  topo.LeaderID,
				Message:    reason,
				Severity:   severity,
			})
			t.prevLeader = topo.LeaderID
		}

		// 2. Node Join & Status Transition Detection (evaluated deterministically)
		sortedCurrentIDs := make([]string, 0, len(currentMap))
		for id := range currentMap {
			sortedCurrentIDs = append(sortedCurrentIDs, id)
		}
		sort.Strings(sortedCurrentIDs)

		for _, id := range sortedCurrentIDs {
			st := currentMap[id]
			prevSt, exists := t.prevNodes[id]
			if !exists {
				t.recordEventLocked(FailoverEvent{
					Timestamp: now,
					EventType: "JOIN",
					Message:   fmt.Sprintf("Node %s joined cluster topology (status: %s).", id, st),
					Severity:  SeverityInfo,
				})
			} else if prevSt != st {
				severity := SeverityWarning
				if st == StatusExpired {
					severity = SeverityCritical
				} else if st == StatusHealthy {
					severity = SeverityInfo
				}
				t.recordEventLocked(FailoverEvent{
					Timestamp: now,
					EventType: "HEALTH_CHANGE",
					Message:   fmt.Sprintf("Node %s health transition: %s -> %s.", id, prevSt, st),
					Severity:  severity,
				})
			}
		}

		// 3. Node Leave / Prune Detection
		sortedPrevIDs := make([]string, 0, len(t.prevNodes))
		for id := range t.prevNodes {
			sortedPrevIDs = append(sortedPrevIDs, id)
		}
		sort.Strings(sortedPrevIDs)

		for _, id := range sortedPrevIDs {
			prevSt := t.prevNodes[id]
			if _, exists := currentMap[id]; !exists {
				t.recordEventLocked(FailoverEvent{
					Timestamp: now,
					EventType: "LEAVE",
					Message:   fmt.Sprintf("Node %s removed from cluster membership table (was %s).", id, prevSt),
					Severity:  SeverityWarning,
				})
			}
		}

		t.prevNodes = currentMap
	}

	// Sort nodes: leader first, then lexicographically by NodeID
	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].IsLeader != nodes[j].IsLeader {
			return nodes[i].IsLeader
		}
		return nodes[i].NodeID < nodes[j].NodeID
	})

	return nodes
}

// recordEventLocked appends an event to the bounded event log.
func (t *TopologyTracker) recordEventLocked(evt FailoverEvent) {
	if len(t.events) >= t.maxEvents {
		t.events = t.events[1:]
	}
	t.events = append(t.events, evt)
}

// AddEvent manually inserts an event into the tracker history (useful for tests or external telemetry).
func (t *TopologyTracker) AddEvent(evt FailoverEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.recordEventLocked(evt)
}

// Events returns a chronological copy of all recorded failover and membership events.
func (t *TopologyTracker) Events() []FailoverEvent {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return slices.Clone(t.events)
}

// Leader returns the currently recognized leader node ID.
func (t *TopologyTracker) Leader() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.prevLeader
}

// Clear resets the tracking history.
func (t *TopologyTracker) Clear() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.prevLeader = ""
	t.prevNodes = make(map[string]string)
	t.events = make([]FailoverEvent, 0, t.maxEvents)
	t.initialized = false
}

// FormatDuration converts a duration into a concise human-readable string.
func FormatDuration(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	return fmt.Sprintf("%dm%ds", int(d.Minutes()), int(d.Seconds())%60)
}

// FormatLease formats a lease countdown duration with status awareness.
func FormatLease(d time.Duration, status string) string {
	if status == StatusExpired || d <= 0 {
		return "EXPIRED"
	}
	return fmt.Sprintf("%.1fs", d.Seconds())
}
