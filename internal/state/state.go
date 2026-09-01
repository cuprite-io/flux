package state

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/cuprite-io/flux/internal/vm"
	"github.com/cuprite-io/flux/types"
)

type stateContextKey struct{}

// WithContext stores a *state.Context inside a standard context.Context.
func WithContext(parent context.Context, sctx *Context) context.Context {
	return context.WithValue(parent, stateContextKey{}, sctx)
}

// FromContext retrieves the *state.Context from a context.Context if present.
func FromContext(ctx context.Context) *Context {
	if ctx == nil {
		return nil
	}
	if v := ctx.Value(stateContextKey{}); v != nil {
		if sctx, ok := v.(*Context); ok {
			return sctx
		}
	}
	return nil
}

// Context manages the execution scratchpad, immutable input, and branch liveness for a Circuit run.
type Context struct {
	Ctx           context.Context
	OriginalInput any

	// mu protects mutable maps during branch copy/write operations
	mu         sync.RWMutex
	scratchpad map[string]any
	deltas     map[string]any // tracks keys mutated specifically in this context
	returnData map[string]any

	// Atomic flags for fast path checks
	deadMask uint64
	aborted  uint32 // 0 = active, 1 = aborted

	errorsMu sync.Mutex
	errors   []error
}

// NewContext creates a fresh execution Context for a Circuit run.
func NewContext(ctx context.Context, input any) *Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return &Context{
		Ctx:           ctx,
		OriginalInput: input,
		scratchpad:    make(map[string]any, 16),
		deltas:        make(map[string]any, 8),
		returnData:    nil,
		deadMask:      0,
		aborted:       0,
		errors:        nil,
	}
}

// Fork creates an isolated child Context that inherits a snapshot of current parent variables.
// Mutations in the child branch do not overwrite parent or sibling variables.
func (c *Context) Fork() *Context {
	c.mu.RLock()
	defer c.mu.RUnlock()

	childScratch := make(map[string]any, len(c.scratchpad)+8)
	for k, v := range c.scratchpad {
		childScratch[k] = v
	}

	return &Context{
		Ctx:           c.Ctx,
		OriginalInput: c.OriginalInput,
		scratchpad:    childScratch,
		deltas:        make(map[string]any, 8),
		returnData:    c.returnData,
		deadMask:      atomic.LoadUint64(&c.deadMask),
		aborted:       atomic.LoadUint32(&c.aborted),
		errors:        nil,
	}
}

// MergeChild merges child scratchpad mutations back into the parent.
func (c *Context) MergeChild(child *Context) {
	if child == nil {
		return
	}
	child.mu.RLock()
	defer child.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()

	// Only apply keys that were mutated in the child
	for k, v := range child.deltas {
		c.scratchpad[k] = v
		c.deltas[k] = v
	}
	if child.returnData != nil {
		if c.returnData == nil {
			c.returnData = make(map[string]any, len(child.returnData))
		}
		for k, v := range child.returnData {
			c.returnData[k] = v
		}
	}
	if atomic.LoadUint32(&child.aborted) == 1 {
		atomic.StoreUint32(&c.aborted, 1)
	}

	child.errorsMu.Lock()
	if len(child.errors) > 0 {
		c.errorsMu.Lock()
		c.errors = append(c.errors, child.errors...)
		c.errorsMu.Unlock()
	}
	child.errorsMu.Unlock()
}

// Get retrieves a variable from Scratchpad or OriginalInput map.
func (c *Context) Get(key string) (any, bool) {
	c.mu.RLock()
	val, ok := c.scratchpad[key]
	c.mu.RUnlock()
	if ok {
		return val, true
	}

	// Check if OriginalInput is a map
	if m, isMap := c.OriginalInput.(map[string]any); isMap {
		val, ok = m[key]
		return val, ok
	}
	return nil, false
}

// Set sets a variable in the Scratchpad and tracks it in deltas.
func (c *Context) Set(key string, val any) {
	c.mu.Lock()
	c.scratchpad[key] = val
	c.deltas[key] = val
	c.mu.Unlock()
}

// Snapshot returns a copy of the entire current scratchpad.
func (c *Context) Snapshot() map[string]any {
	c.mu.RLock()
	defer c.mu.RUnlock()

	res := make(map[string]any, len(c.scratchpad)+8)
	// Base input if map
	if m, isMap := c.OriginalInput.(map[string]any); isMap {
		res["payload"] = m
		for k, v := range m {
			res[k] = v
		}
	}
	for k, v := range c.scratchpad {
		res[k] = v
	}
	return res
}

// SetReturn records output key-values from a StepReturn step.
func (c *Context) SetReturn(data map[string]any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.returnData == nil {
		c.returnData = make(map[string]any, len(data))
	}
	for k, v := range data {
		c.returnData[k] = v
	}
}

// ReturnData returns the recorded return data map.
func (c *Context) ReturnData() map[string]any {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.returnData
}

// Abort marks the execution as explicitly aborted.
func (c *Context) Abort() {
	atomic.StoreUint32(&c.aborted, 1)
}

// IsAborted returns true if execution has been aborted.
func (c *Context) IsAborted() bool {
	return atomic.LoadUint32(&c.aborted) == 1
}

// MarkPruned sets the branch dead bitmask flag in O(1) time.
func (c *Context) MarkPruned(nodeBit uint64) {
	for {
		cur := atomic.LoadUint64(&c.deadMask)
		next := cur | nodeBit
		if atomic.CompareAndSwapUint64(&c.deadMask, cur, next) {
			return
		}
	}
}

// IsPruned checks whether the given nodeBit has been pruned in O(1) time.
func (c *Context) IsPruned(nodeBit uint64) bool {
	return (atomic.LoadUint64(&c.deadMask) & nodeBit) != 0
}

// AddError records an execution error safely.
func (c *Context) AddError(err error) {
	if err == nil {
		return
	}
	c.errorsMu.Lock()
	c.errors = append(c.errors, err)
	c.errorsMu.Unlock()
}

// Errors returns a copy of all accumulated errors.
func (c *Context) Errors() []error {
	c.errorsMu.Lock()
	defer c.errorsMu.Unlock()
	if len(c.errors) == 0 {
		return nil
	}
	res := make([]error, len(c.errors))
	copy(res, c.errors)
	return res
}

// ExportType converts internal Context into public types.StateContext.
func (c *Context) ExportType() *types.StateContext {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return &types.StateContext{
		Ctx:           c.Ctx,
		OriginalInput: c.OriginalInput,
		Scratchpad:    c.Snapshot(),
		ReturnData:    c.returnData,
		DeadMask:      atomic.LoadUint64(&c.deadMask),
		Aborted:       atomic.LoadUint32(&c.aborted) == 1,
		Errors:        c.Errors(),
	}
}

// VMContextAdapter adapts state.Context to vm.VMContext.
type VMContextAdapter struct {
	Ctx *Context
}

func (a *VMContextAdapter) GetState(key string) (any, bool) {
	return a.Ctx.Get(key)
}

func (a *VMContextAdapter) SetState(key string, val any) {
	a.Ctx.Set(key, val)
}

func (a *VMContextAdapter) CallNative(fnIdx uint32, args []vm.Value, frame *vm.FrameArena) (vm.Value, error) {
	return vm.NullValue, nil
}

func (a *VMContextAdapter) IsPruned(bit uint64) bool {
	return a.Ctx.IsPruned(bit)
}

func (a *VMContextAdapter) SetPruned(bit uint64) {
	a.Ctx.MarkPruned(bit)
}
