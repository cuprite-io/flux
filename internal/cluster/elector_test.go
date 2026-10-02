package cluster

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cuprite-io/flux/internal/cache"
)

func TestLeaderElector_SingleNode(t *testing.T) {
	backend := cache.NewMemoryCache()
	defer backend.Close()

	var electedCount int64
	var revokedCount int64

	elector := NewLeaderElector(backend,
		WithNodeID("single-node-1"),
		WithHeartbeatInterval(20*time.Millisecond),
		WithHeartbeatTTL(100*time.Millisecond),
		WithOnElected(func() {
			atomic.AddInt64(&electedCount, 1)
		}),
		WithOnRevoked(func() {
			atomic.AddInt64(&revokedCount, 1)
		}),
	)

	ctx := context.Background()

	// Initial evaluation
	isLeader, err := elector.Evaluate(ctx)
	if err != nil {
		t.Fatalf("unexpected evaluate error: %v", err)
	}
	if !isLeader {
		t.Fatalf("expected single node to be leader")
	}

	if !elector.IsLeader() {
		t.Errorf("IsLeader should return true")
	}
	if elector.LeaderID() != "single-node-1" {
		t.Errorf("expected leader ID 'single-node-1', got %q", elector.LeaderID())
	}
	if elector.IsMultiNode() {
		t.Errorf("expected single node, but IsMultiNode returned true")
	}
	if len(elector.ActiveNodes()) != 1 || elector.ActiveNodes()[0] != "single-node-1" {
		t.Errorf("expected active nodes [single-node-1], got %v", elector.ActiveNodes())
	}
	if atomic.LoadInt64(&electedCount) != 1 {
		t.Errorf("expected OnElected to be called once, got %d", electedCount)
	}

	// Repeated evaluation (renewal) should not refire OnElected
	isLeader, err = elector.Evaluate(ctx)
	if err != nil {
		t.Fatalf("unexpected renewal error: %v", err)
	}
	if !isLeader {
		t.Fatalf("expected still leader")
	}
	if atomic.LoadInt64(&electedCount) != 1 {
		t.Errorf("OnElected should not be called again on renewal, got %d", electedCount)
	}

	// StepDown
	if err := elector.StepDown(ctx); err != nil {
		t.Fatalf("unexpected stepdown error: %v", err)
	}
	if elector.IsLeader() {
		t.Errorf("expected not leader after stepdown")
	}
	if atomic.LoadInt64(&revokedCount) != 1 {
		t.Errorf("expected OnRevoked to be called once, got %d", revokedCount)
	}
}

func TestLeaderElector_MultiNode_ElectionAndFailover(t *testing.T) {
	backend := cache.NewMemoryCache()
	defer backend.Close()

	var elected1, revoked1 int64
	var elected2, revoked2 int64

	node1 := NewLeaderElector(backend,
		WithNodeID("node-1"),
		WithHeartbeatInterval(20*time.Millisecond),
		WithHeartbeatTTL(80*time.Millisecond),
		WithOnElected(func() { atomic.AddInt64(&elected1, 1) }),
		WithOnRevoked(func() { atomic.AddInt64(&revoked1, 1) }),
	)

	node2 := NewLeaderElector(backend,
		WithNodeID("node-2"),
		WithHeartbeatInterval(20*time.Millisecond),
		WithHeartbeatTTL(80*time.Millisecond),
		WithOnElected(func() { atomic.AddInt64(&elected2, 1) }),
		WithOnRevoked(func() { atomic.AddInt64(&revoked2, 1) }),
	)

	ctx := context.Background()

	// Evaluate both nodes
	lead1, err := node1.Evaluate(ctx)
	if err != nil || !lead1 {
		t.Fatalf("expected node-1 to be leader, got lead=%v err=%v", lead1, err)
	}

	lead2, err := node2.Evaluate(ctx)
	if err != nil || lead2 {
		t.Fatalf("expected node-2 to be follower, got lead=%v err=%v", lead2, err)
	}

	// Re-evaluate node-1 so it detects node-2
	_, _ = node1.Evaluate(ctx)

	// Both should detect multi-node
	if !node1.IsMultiNode() {
		t.Errorf("node-1 should report multi-node")
	}
	if !node2.IsMultiNode() {
		t.Errorf("node-2 should report multi-node")
	}

	if node1.LeaderID() != "node-1" || node2.LeaderID() != "node-1" {
		t.Errorf("expected both nodes to identify node-1 as leader, got node1=%q, node2=%q",
			node1.LeaderID(), node2.LeaderID())
	}

	if atomic.LoadInt64(&elected1) != 1 {
		t.Errorf("expected node-1 elected once, got %d", elected1)
	}
	if atomic.LoadInt64(&elected2) != 0 {
		t.Errorf("node-2 should not have been elected, got %d", elected2)
	}

	// Node 1 steps down
	if err := node1.StepDown(ctx); err != nil {
		t.Fatalf("node-1 stepdown error: %v", err)
	}
	if atomic.LoadInt64(&revoked1) != 1 {
		t.Errorf("expected node-1 revoked once, got %d", revoked1)
	}

	// Node 2 evaluates -> should immediately become leader!
	lead2, err = node2.Evaluate(ctx)
	if err != nil || !lead2 {
		t.Fatalf("expected node-2 to assume leadership, got lead=%v err=%v", lead2, err)
	}
	if !node2.IsLeader() {
		t.Errorf("node-2 IsLeader should be true")
	}
	if node2.LeaderID() != "node-2" {
		t.Errorf("expected leader to be node-2, got %q", node2.LeaderID())
	}
	if node2.IsMultiNode() {
		t.Errorf("node-2 is now single node since node-1 stepped down")
	}
	if atomic.LoadInt64(&elected2) != 1 {
		t.Errorf("expected node-2 elected once, got %d", elected2)
	}
}

func TestLeaderElector_HeartbeatExpiration_Failover(t *testing.T) {
	backend := cache.NewMemoryCache()
	defer backend.Close()

	ttl := 60 * time.Millisecond
	node1 := NewLeaderElector(backend,
		WithNodeID("node-1"),
		WithHeartbeatTTL(ttl),
	)
	node2 := NewLeaderElector(backend,
		WithNodeID("node-2"),
		WithHeartbeatTTL(ttl),
	)

	ctx := context.Background()

	// Initial evaluation: node-1 is leader
	_, _ = node1.Evaluate(ctx)
	_, _ = node2.Evaluate(ctx)
	if !node1.IsLeader() {
		t.Fatalf("node-1 should be leader")
	}
	if node2.IsLeader() {
		t.Fatalf("node-2 should not be leader")
	}

	// Node 1 crashes abruptly without StepDown (stops updating heartbeats).
	// Sleep longer than TTL so node-1's heartbeat expires
	time.Sleep(ttl + 20*time.Millisecond)

	// Node 2 evaluates -> discovers node-1 expired, prunes it, and takes leadership
	lead2, err := node2.Evaluate(ctx)
	if err != nil {
		t.Fatalf("node-2 evaluate error: %v", err)
	}
	if !lead2 || !node2.IsLeader() {
		t.Fatalf("node-2 should take over leadership after node-1 expiration")
	}
	if node2.LeaderID() != "node-2" {
		t.Errorf("expected leader ID node-2, got %q", node2.LeaderID())
	}
}

func TestLeaderElector_DynamicPreemptionByLowerID(t *testing.T) {
	backend := cache.NewMemoryCache()
	defer backend.Close()

	var revokedB int64
	nodeB := NewLeaderElector(backend,
		WithNodeID("node-b"),
		WithHeartbeatTTL(100*time.Millisecond),
		WithOnRevoked(func() { atomic.AddInt64(&revokedB, 1) }),
	)

	ctx := context.Background()

	// node-b starts alone and becomes leader
	leadB, _ := nodeB.Evaluate(ctx)
	if !leadB {
		t.Fatalf("node-b should be leader")
	}

	// Now node-a (lexicographically lower) joins
	nodeA := NewLeaderElector(backend,
		WithNodeID("node-a"),
		WithHeartbeatTTL(100*time.Millisecond),
	)

	leadA, err := nodeA.Evaluate(ctx)
	if err != nil || !leadA {
		t.Fatalf("node-a should be elected leader, got lead=%v err=%v", leadA, err)
	}

	// node-b evaluates again, discovers node-a is lower, and yields leadership
	leadB, err = nodeB.Evaluate(ctx)
	if err != nil || leadB {
		t.Fatalf("node-b should yield leadership to node-a, got lead=%v err=%v", leadB, err)
	}
	if nodeB.IsLeader() {
		t.Errorf("node-b IsLeader should be false")
	}
	if nodeB.LeaderID() != "node-a" {
		t.Errorf("expected node-b to recognize node-a as leader, got %q", nodeB.LeaderID())
	}
	if atomic.LoadInt64(&revokedB) != 1 {
		t.Errorf("expected node-b OnRevoked called once, got %d", revokedB)
	}
}

func TestLeaderElector_StartStopLifecycle(t *testing.T) {
	backend := cache.NewMemoryCache()
	defer backend.Close()

	var elected int64
	elector := NewLeaderElector(backend,
		WithNodeID("node-lifecycle"),
		WithHeartbeatInterval(25*time.Millisecond),
		WithHeartbeatTTL(100*time.Millisecond),
		WithOnElected(func() { atomic.AddInt64(&elected, 1) }),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := elector.Start(ctx); err != nil {
		t.Fatalf("start error: %v", err)
	}

	// Immediate acquisition on start
	if !elector.IsLeader() {
		t.Fatalf("expected leader immediately on start")
	}

	// Let the ticker run a couple of cycles
	time.Sleep(75 * time.Millisecond)

	if atomic.LoadInt64(&elected) != 1 {
		t.Errorf("expected elected exactly once, got %d", elected)
	}

	// Stop
	if err := elector.Stop(context.Background()); err != nil {
		t.Fatalf("stop error: %v", err)
	}

	if elector.IsLeader() {
		t.Errorf("expected not leader after stop")
	}

	// Evaluate after stop should fail
	_, err := elector.Evaluate(context.Background())
	if err != ErrElectorStopped {
		t.Errorf("expected ErrElectorStopped, got %v", err)
	}
}

func TestLeaderElector_ConcurrentAccess(t *testing.T) {
	backend := cache.NewMemoryCache()
	defer backend.Close()

	elector := NewLeaderElector(backend,
		WithNodeID("node-concurrent"),
		WithHeartbeatInterval(10*time.Millisecond),
		WithHeartbeatTTL(50*time.Millisecond),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_ = elector.Start(ctx)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = elector.IsLeader()
				_ = elector.LeaderID()
				_ = elector.IsMultiNode()
				_ = elector.ActiveNodes()
				_, _ = elector.Evaluate(ctx)
				time.Sleep(1 * time.Millisecond)
			}
		}()
	}

	wg.Wait()
	_ = elector.Stop(context.Background())
}
