package state_test

import (
	"context"
	"sync"
	"testing"

	"github.com/cuprite-io/flux/internal/state"
)

func TestStateContext_ForkAndMerge(t *testing.T) {
	parent := state.NewContext(context.Background(), map[string]any{"user_id": "u101", "level": 5})
	parent.Set("xp", 1200)

	// Fork child A & B
	childA := parent.Fork()
	childB := parent.Fork()

	// Child A mutates state
	childA.Set("tier", "GOLD")
	childA.Set("xp", 2000)

	// Child B mutates different keys
	childB.Set("badge", "SLAYER")

	// Verify isolation before merge
	if v, _ := parent.Get("tier"); v != nil {
		t.Errorf("parent should not have childA tier yet")
	}

	// Merge child A then child B
	parent.MergeChild(childA)
	parent.MergeChild(childB)

	if v, _ := parent.Get("tier"); v != "GOLD" {
		t.Errorf("expected tier GOLD, got %v", v)
	}
	if v, _ := parent.Get("badge"); v != "SLAYER" {
		t.Errorf("expected badge SLAYER, got %v", v)
	}
	if v, _ := parent.Get("xp"); v != 2000 {
		t.Errorf("expected xp 2000, got %v", v)
	}
}

func TestStateContext_DeadMaskPruning(t *testing.T) {
	ctx := state.NewContext(context.Background(), nil)

	const nodeBit1 uint64 = 1 << 0
	const nodeBit2 uint64 = 1 << 1

	if ctx.IsPruned(nodeBit1) {
		t.Errorf("nodeBit1 should not be pruned initially")
	}

	ctx.MarkPruned(nodeBit1)

	if !ctx.IsPruned(nodeBit1) {
		t.Errorf("nodeBit1 should be marked pruned")
	}
	if ctx.IsPruned(nodeBit2) {
		t.Errorf("nodeBit2 should not be pruned")
	}
}

func TestStateContext_ConcurrentAccess(t *testing.T) {
	ctx := state.NewContext(context.Background(), map[string]any{"base": 1})

	var wg sync.WaitGroup
	workers := 100

	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(id int) {
			defer wg.Done()
			child := ctx.Fork()
			child.Set("key", id)
			_ = child.Snapshot()
			if id%2 == 0 {
				child.MarkPruned(uint64(1 << (id % 64)))
			}
			ctx.MergeChild(child)
		}(i)
	}

	wg.Wait()

	if len(ctx.Snapshot()) == 0 {
		t.Errorf("snapshot should not be empty")
	}
}

func TestStateContext_ShadowMode(t *testing.T) {
	c := state.AcquireContext(context.Background(), map[string]any{"data": 123})
	defer state.ReleaseContext(c)

	if c.IsShadow() {
		t.Errorf("expected clean context to have IsShadow() == false")
	}
	if c.ShadowFirings() != 0 {
		t.Errorf("expected 0 shadow firings, got %d", c.ShadowFirings())
	}

	c.SetShadowMode(true)
	if !c.IsShadow() {
		t.Errorf("expected IsShadow() == true")
	}

	c.IncrementShadowFirings()
	c.IncrementShadowFirings()
	if c.ShadowFirings() != 2 {
		t.Errorf("expected 2 shadow firings, got %d", c.ShadowFirings())
	}

	// Fork inherits shadow mode
	child := c.Fork()
	if !child.IsShadow() {
		t.Errorf("expected child to inherit shadow mode")
	}
	if child.ShadowFirings() != 0 {
		t.Errorf("expected new child branch to have 0 firings, got %d", child.ShadowFirings())
	}

	child.IncrementShadowFirings()
	child.IncrementShadowFirings()
	child.IncrementShadowFirings()

	c.MergeChild(child)
	if c.ShadowFirings() != 5 {
		t.Errorf("expected 2 + 3 = 5 shadow firings after merge, got %d", c.ShadowFirings())
	}

	exported := c.ExportType()
	if !exported.ShadowMode {
		t.Errorf("expected exported.ShadowMode == true")
	}
	if exported.ShadowFirings != 5 {
		t.Errorf("expected exported.ShadowFirings == 5, got %d", exported.ShadowFirings)
	}

	// Release and reacquire - verify pool reset
	state.ReleaseContext(c)
	reacquired := state.AcquireContext(context.Background(), nil)
	defer state.ReleaseContext(reacquired)

	if reacquired.IsShadow() {
		t.Errorf("expected reacquired context to have IsShadow() == false")
	}
	if reacquired.ShadowFirings() != 0 {
		t.Errorf("expected reacquired context to have 0 firings, got %d", reacquired.ShadowFirings())
	}
}
