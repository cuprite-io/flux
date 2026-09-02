package flux

import (
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cuprite-io/flux/internal/cache"
	"github.com/cuprite-io/flux/internal/catalog"
	"github.com/cuprite-io/flux/internal/compiler"
	"github.com/cuprite-io/flux/internal/engine"
	"github.com/cuprite-io/flux/internal/pool"
	"github.com/cuprite-io/flux/internal/registry"
	"github.com/cuprite-io/flux/internal/sink"
	"github.com/cuprite-io/flux/internal/state"
	"github.com/cuprite-io/flux/types"
)

// Engine is the unified, high-performance Circuit execution data plane.
type Engine struct {
	cache    cache.CacheBackend
	registry *registry.Registry
	catalog  *catalog.Catalog
	sinks    *sink.Registry
	pool     *pool.WorkerPool
	compiler *compiler.Compiler
	executor *engine.Executor
	workers  int
	mu       sync.RWMutex
}

// New creates and initializes a new Flux Engine.
func New(opts ...Option) (*Engine, error) {
	e := &Engine{
		sinks:   sink.New(4, 1024),
		workers: 0,
	}

	for _, opt := range opts {
		opt(e)
	}

	if e.cache == nil {
		e.cache = cache.NewMemoryCache()
	}

	e.registry = registry.New(e.cache)
	e.catalog = catalog.New(e.cache)
	e.pool = pool.NewWorkerPool(e.workers, 4096)

	comp, err := compiler.NewCompiler(e.cache)
	if err != nil {
		return nil, fmt.Errorf("flux: failed to initialize compiler: %w", err)
	}
	e.compiler = comp

	e.executor = engine.NewExecutor(e.compiler, e.pool, func(sinkName string, payload any) error {
		return e.sinks.Dispatch(context.Background(), sinkName, payload)
	})

	return e, nil
}

// Registry returns the Circuit Registry subsystem.
func (e *Engine) Registry() *registry.Registry {
	return e.registry
}

// Catalog returns the Candidate Item Catalog subsystem.
func (e *Engine) Catalog() *catalog.Catalog {
	return e.catalog
}

// RegisterSink registers a named external sink.
func (e *Engine) RegisterSink(name string, s sink.Sink) {
	e.sinks.Register(name, s)
}

// Close gracefully stops the Engine and drains all background queues.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.sinks != nil {
		_ = e.sinks.Close()
	}
	if e.pool != nil {
		e.pool.Close()
	}
	if e.cache != nil {
		_ = e.cache.Close()
	}
	return nil
}

// Spark evaluates an incoming payload sequentially across all matching Circuits.
// Input must strictly be JSON ([]byte/string), Go slice, or Go struct.
func (e *Engine) Spark(ctx context.Context, payload any, tags ...string) (*types.SparkResult, error) {
	if err := ValidateInput(payload); err != nil {
		return nil, err
	}

	circuits := e.registry.GetMatching(ctx, tags...)
	if len(circuits) == 0 {
		return &types.SparkResult{
			OriginalInput:    payload,
			Passed:           true,
			ExecutedCircuits: nil,
		}, nil
	}

	normalizedInput := normalizeInput(payload)

	// Single-Circuit Fast Path (>95% production workload)
	if len(circuits) == 1 {
		sctx := state.AcquireContext(ctx, normalizedInput)
		defer state.ReleaseContext(sctx)
		res, err := e.executor.ExecuteCircuit(ctx, circuits[0], sctx)
		if res != nil {
			res.OriginalInput = payload
			if res.ReturnedData == nil {
				res.ReturnedData = make(map[string]any)
			}
			return res, err
		}
		return &types.SparkResult{
			OriginalInput:    payload,
			ReturnedData:     make(map[string]any),
			Passed:           err == nil,
			ExecutedCircuits: []string{circuits[0].ID},
			Errors:           []error{err},
		}, err
	}

	combinedResult := &types.SparkResult{
		OriginalInput:    payload,
		ReturnedData:     make(map[string]any),
		Passed:           true,
		ExecutedCircuits: make([]string, 0, len(circuits)),
		Errors:           nil,
	}

	// Execute matching Circuits sequentially
	// Isolation rule: Every Circuit runs with the original immutable payload base
	for _, circuit := range circuits {
		sctx := state.AcquireContext(ctx, normalizedInput)
		res, err := e.executor.ExecuteCircuit(ctx, circuit, sctx)
		state.ReleaseContext(sctx)

		combinedResult.ExecutedCircuits = append(combinedResult.ExecutedCircuits, circuit.ID)

		if err != nil {
			combinedResult.Passed = false
			combinedResult.Errors = append(combinedResult.Errors, err)
			if res != nil {
				for k, v := range res.ReturnedData {
					combinedResult.ReturnedData[k] = v
				}
			}
			continue
		}

		if res != nil {
			if !res.Passed {
				combinedResult.Passed = false
			}
			for k, v := range res.ReturnedData {
				combinedResult.ReturnedData[k] = v
			}
			if len(res.Errors) > 0 {
				combinedResult.Errors = append(combinedResult.Errors, res.Errors...)
			}
		}
	}

	return combinedResult, nil
}

// Conduct evaluates candidate items from the Catalog against entity state.
// Ineligible items are silently omitted, and qualified items have their dynamic output populated.
func (e *Engine) Conduct(ctx context.Context, req *types.ConductRequest) (*types.ConductResult, error) {
	if req == nil {
		return nil, errors.New("flux: conduct request required")
	}

	startTime := time.Now()

	// 1. Fetch Candidate Items matching target category partition in 1 call
	targetCategory := req.TargetCategory()
	items := e.catalog.GetByCategory(ctx, targetCategory)
	evaluatedCount := len(items)

	if evaluatedCount == 0 {
		return &types.ConductResult{
			EntityID:       req.EntityID,
			Items:          nil,
			EvaluatedCount: 0,
			Duration:       time.Since(startTime),
		}, nil
	}

	// 2. Hydrate Entity State from CacheBackend if EntityID is specified
	entityState := make(map[string]any)
	if req.EntityID != "" && e.cache != nil {
		if raw, err := e.cache.Get(ctx, "entity:"+req.EntityID); err == nil && raw != "" {
			if errJSON := json.Unmarshal([]byte(raw), &entityState); errJSON != nil {
				_ = e.cache.GetScan(ctx, "entity:"+req.EntityID, &entityState)
			}
		} else {
			_ = e.cache.GetScan(ctx, "entity:"+req.EntityID, &entityState)
		}
	}
	// Overlay request context
	for k, v := range req.Context {
		entityState[k] = v
	}

	// 3. Parallel Candidate Item Qualification (Lock-Free Thread Slots)
	evalResults := make([]*types.EvaluatedItem, len(items))
	latch := pool.NewLatch(len(items))

	for i, it := range items {
		item := it
		rankIdx := i

		e.pool.Submit(func() {
			defer latch.CountDown()

			// Check item embedded qualification circuit
			if item.Circuit == nil {
				// No qualification circuit -> automatically qualified with base data
				evalResults[rankIdx] = &types.EvaluatedItem{
					ID:             item.ID,
					Data:           item.Data,
					ComputedOutput: item.Data,
					Score:          1.0,
					Rank:           rankIdx + 1,
				}
				return
			}

			// Evaluate embedded circuit against hydrated entity state
			itemContext := make(map[string]any, len(entityState)+len(item.Data)+2)
			for k, v := range entityState {
				itemContext[k] = v
			}
			for k, v := range item.Data {
				itemContext[k] = v
			}
			itemContext["user"] = entityState
			itemContext["item"] = item.Data

			sctx := state.AcquireContext(ctx, itemContext)
			res, err := e.executor.ExecuteCircuit(ctx, item.Circuit, sctx)

			// Ineligible / aborted items are SILENTLY OMITTED
			// If root node was pruned (condition failed) or execution aborted -> omit
			if err != nil || !res.Passed || sctx.IsAborted() || sctx.IsPruned(1<<0) {
				state.ReleaseContext(sctx)
				return
			}

			computedOut := item.Data
			if len(res.ReturnedData) > 0 {
				computedOut = res.ReturnedData
			}
			state.ReleaseContext(sctx)

			evalResults[rankIdx] = &types.EvaluatedItem{
				ID:             item.ID,
				Data:           item.Data,
				ComputedOutput: computedOut,
				Score:          1.0,
				Rank:           0,
			}
		})
	}

	latch.Wait()

	// 4. Compact qualified results lock-free
	qualified := make([]types.EvaluatedItem, 0, len(items))
	for _, res := range evalResults {
		if res != nil {
			qualified = append(qualified, *res)
		}
	}

	// 5. Apply Top-K ranking (Bounded Min-Heap for K < N, Full Sort for K >= N or TopK == 0)
	var finalItems []types.EvaluatedItem
	if req.TopK > 0 && len(qualified) > req.TopK {
		h := &itemMinHeap{}
		heap.Init(h)
		for _, it := range qualified {
			if h.Len() < req.TopK {
				heap.Push(h, it)
			} else if it.Score > (*h)[0].Score {
				(*h)[0] = it
				heap.Fix(h, 0)
			}
		}
		finalItems = make([]types.EvaluatedItem, h.Len())
		for i := len(finalItems) - 1; i >= 0; i-- {
			finalItems[i] = heap.Pop(h).(types.EvaluatedItem)
		}
		for i := range finalItems {
			finalItems[i].Rank = i + 1
		}
	} else {
		sort.Slice(qualified, func(i, j int) bool {
			return qualified[i].Score > qualified[j].Score
		})
		for i := range qualified {
			qualified[i].Rank = i + 1
		}
		finalItems = qualified
	}

	return &types.ConductResult{
		EntityID:       req.EntityID,
		Items:          finalItems,
		EvaluatedCount: evaluatedCount,
		Duration:       time.Since(startTime),
	}, nil
}

type itemMinHeap []types.EvaluatedItem

func (h itemMinHeap) Len() int           { return len(h) }
func (h itemMinHeap) Less(i, j int) bool { return h[i].Score < h[j].Score }
func (h itemMinHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *itemMinHeap) Push(x any)        { *h = append(*h, x.(types.EvaluatedItem)) }
func (h *itemMinHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// normalizeInput converts Go structs, slices, or JSON to a map representation for Volt execution.
type structFieldInfo struct {
	key   string
	index int
}

var structFieldCache sync.Map // reflect.Type -> []structFieldInfo

func getStructFields(t reflect.Type) []structFieldInfo {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if v, ok := structFieldCache.Load(t); ok {
		return v.([]structFieldInfo)
	}

	var fields []structFieldInfo
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
		fields = append(fields, structFieldInfo{
			key:   key,
			index: i,
		})
	}

	structFieldCache.Store(t, fields)
	return fields
}

func fastStructToMap(val reflect.Value) map[string]any {
	if val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil
		}
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct {
		return nil
	}

	fields := getStructFields(val.Type())
	m := make(map[string]any, len(fields))
	for _, f := range fields {
		m[f.key] = val.Field(f.index).Interface()
	}
	return m
}

func normalizeInput(payload any) any {
	if payload == nil {
		return nil
	}

	switch v := payload.(type) {
	case string:
		var m map[string]any
		if err := json.Unmarshal([]byte(v), &m); err == nil {
			return m
		}
		return v
	case []byte:
		var m map[string]any
		if err := json.Unmarshal(v, &m); err == nil {
			return m
		}
		return v
	case map[string]any:
		return v
	}

	val := reflect.ValueOf(payload)
	if val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil
		}
		val = val.Elem()
	}

	if val.Kind() == reflect.Struct {
		if m := fastStructToMap(val); m != nil {
			return m
		}
	}

	return payload
}
