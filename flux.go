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
	"time"

	"github.com/cuprite-io/flux/internal/cache"
	"github.com/cuprite-io/flux/internal/catalog"
	"github.com/cuprite-io/flux/internal/compiler"
	"github.com/cuprite-io/flux/internal/engine"
	"github.com/cuprite-io/flux/internal/history"
	"github.com/cuprite-io/flux/internal/pool"
	"github.com/cuprite-io/flux/internal/profiler"
	"github.com/cuprite-io/flux/internal/registry"
	"github.com/cuprite-io/flux/internal/schematap"
	"github.com/cuprite-io/flux/internal/sink"
	"github.com/cuprite-io/flux/internal/state"
	"github.com/cuprite-io/flux/internal/telemetry"
	"github.com/cuprite-io/flux/types"
	"go.opentelemetry.io/otel/trace"
)

// Engine is the unified, high-performance Circuit execution data plane.
type Engine struct {
	cache          cache.CacheBackend
	ownedCache     bool
	registry       *registry.Registry
	catalog        *catalog.Catalog
	sinks          *sink.Registry
	pool           *pool.WorkerPool
	compiler       *compiler.Compiler
	executor       *engine.Executor
	workers        int
	schemaLearning bool
	schemaConfig   schematap.Config
	schemaTap      *schematap.SchemaTap
	profiling      bool
	profilerConfig profiler.Config
	profiler       *profiler.Profiler
	historyEnabled bool
	historyConfig  history.Config
	history        *history.Store
	tracer         *telemetry.Tracer
}

// New creates and initializes a new Flux Engine.
func New(opts ...Option) (*Engine, error) {
	e := &Engine{
		sinks:          sink.New(4, 1024),
		workers:        0,
		schemaConfig:   schematap.DefaultConfig(),
		profilerConfig: profiler.DefaultConfig(),
		historyConfig:  history.DefaultConfig(),
	}

	for _, opt := range opts {
		opt(e)
	}

	if e.cache == nil {
		e.cache = cache.NewMemoryCache()
		e.ownedCache = true
	}

	e.registry = registry.New(e.cache)
	e.catalog = catalog.New(e.cache)
	e.pool = pool.NewWorkerPool(e.workers, 4096)

	comp, err := compiler.NewCompiler(e.cache)
	if err != nil {
		return nil, fmt.Errorf("flux: failed to initialize compiler: %w", err)
	}
	e.compiler = comp

	if e.historyEnabled {
		hist, err := history.New(e.cache, e.historyConfig)
		if err != nil {
			return nil, fmt.Errorf("flux: failed to initialize history store: %w", err)
		}
		e.history = hist
	}

	var execOpts []engine.ExecutorOption
	if e.tracer != nil {
		execOpts = append(execOpts, engine.WithExecutorTracer(e.tracer))
	}
	execOpts = append(execOpts, engine.WithAlertHook(func(ctx context.Context, circuitID, nodeName, sinkName, condition string, payload any) {
		if e.history != nil {
			var m map[string]any
			if asMap, ok := payload.(map[string]any); ok {
				m = asMap
			} else if b, ok := payload.([]byte); ok {
				_ = json.Unmarshal(b, &m)
			} else if s, ok := payload.(string); ok {
				_ = json.Unmarshal([]byte(s), &m)
			}
			_ = e.history.Record(ctx, history.AlertRecord{
				CircuitID: circuitID,
				NodeName:  nodeName,
				SinkName:  sinkName,
				Condition: condition,
				Payload:   m,
			})
		}
	}))

	e.executor = engine.NewExecutor(e.compiler, e.pool, func(sinkName string, payload any) error {
		return e.sinks.Dispatch(context.Background(), sinkName, payload)
	}, execOpts...)

	if e.schemaLearning {
		tap, err := schematap.New(e.cache, e.schemaConfig)
		if err != nil {
			return nil, fmt.Errorf("flux: failed to initialize schema tap: %w", err)
		}
		e.schemaTap = tap
	}

	if e.profiling {
		prof, err := profiler.New(e.cache, e.profilerConfig)
		if err != nil {
			return nil, fmt.Errorf("flux: failed to initialize profiler: %w", err)
		}
		e.profiler = prof
	}

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

// SchemaTap returns the Schema Inference subsystem if enabled.
func (e *Engine) SchemaTap() *schematap.SchemaTap {
	return e.schemaTap
}

// Profiler returns the Streaming Value Profiler subsystem if enabled.
func (e *Engine) Profiler() *profiler.Profiler {
	return e.profiler
}

// History returns the Alert History Store subsystem if enabled.
func (e *Engine) History() *history.Store {
	return e.history
}

// GetAlertHistory queries historical alerts for a circuit from CacheBackend.
func (e *Engine) GetAlertHistory(ctx context.Context, circuitID string, limit int) ([]history.AlertRecord, error) {
	if e.history == nil {
		return nil, errors.New("flux: alert history store not enabled")
	}
	return e.history.GetAlerts(ctx, circuitID, limit)
}

// RecordAlertFeedback attaches operator feedback to a historical alert record in CacheBackend.
func (e *Engine) RecordAlertFeedback(ctx context.Context, circuitID, alertID string, fb history.Feedback) error {
	if e.history == nil {
		return errors.New("flux: alert history store not enabled")
	}
	return e.history.RecordFeedback(ctx, circuitID, alertID, fb)
}

// GetAlertStats queries aggregated alert counts and rates for a circuit from CacheBackend.
func (e *Engine) GetAlertStats(ctx context.Context, circuitID string) (*history.AlertStats, error) {
	if e.history == nil {
		return nil, errors.New("flux: alert history store not enabled")
	}
	return e.history.GetStats(ctx, circuitID)
}

// AlertRecord re-exports history.AlertRecord.
type AlertRecord = history.AlertRecord

// AlertFeedback re-exports history.Feedback.
type AlertFeedback = history.Feedback

// AlertStats re-exports history.AlertStats.
type AlertStats = history.AlertStats

const (
	FeedbackValid         = history.FeedbackValid
	FeedbackFalsePositive = history.FeedbackFalsePositive
	FeedbackNoisy         = history.FeedbackNoisy
	FeedbackMuted         = history.FeedbackMuted
)

// SinkDescriptor re-exports sink.Descriptor.
type SinkDescriptor = sink.Descriptor

// SinkOption re-exports sink.Option.
type SinkOption = sink.Option

const (
	// SinkSeverityInfo is used for informational, audit, or routine notifications.
	SinkSeverityInfo = sink.SeverityInfo
	// SinkSeverityWarning is used for degraded performance, anomalies, or threshold warnings.
	SinkSeverityWarning = sink.SeverityWarning
	// SinkSeverityCritical is used for outages, fatal failures, security incidents, or paging on-call.
	SinkSeverityCritical = sink.SeverityCritical
)

// RegisterSink registers a named external sink with optional descriptive metadata.
func (e *Engine) RegisterSink(name string, s sink.Sink, opts ...sink.Option) {
	e.sinks.Register(name, s, opts...)
}

// ListSinks returns descriptors for all registered external sinks.
func (e *Engine) ListSinks() []sink.Descriptor {
	return e.sinks.ListDescriptors()
}

// SinkDescriptor returns the descriptor for a named sink if registered.
func (e *Engine) SinkDescriptor(name string) (sink.Descriptor, bool) {
	return e.sinks.GetDescriptor(name)
}

// Close gracefully stops the Engine and drains all background queues.
// If the cache backend was externally supplied via WithCache, it is left open.
func (e *Engine) Close() error {
	if e.history != nil {
		_ = e.history.Close()
	}
	if e.profiler != nil {
		_ = e.profiler.Close()
	}
	if e.schemaTap != nil {
		_ = e.schemaTap.Close()
	}
	if e.sinks != nil {
		_ = e.sinks.Close()
	}
	if e.pool != nil {
		e.pool.Close()
	}
	if e.ownedCache && e.cache != nil {
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

	var span trace.Span
	if e.tracer.IsEnabled() {
		ctx, span = e.tracer.Start(ctx, "flux.spark",
			trace.WithAttributes(
				telemetry.AttrSparkTags.StringSlice(tags),
			),
		)
	}

	var prepSpan trace.Span
	var prepCtx context.Context = ctx
	if e.tracer.IsEnabled() {
		prepCtx, prepSpan = e.tracer.Start(ctx, "flux.spark:prepare")
	}

	if e.schemaTap != nil {
		primaryTag := "stream:default"
		if len(tags) > 0 {
			primaryTag = tags[0]
		}
		_ = e.schemaTap.Sample(prepCtx, primaryTag, payload)
	}

	if e.profiler != nil {
		primaryTag := "stream:default"
		if len(tags) > 0 {
			primaryTag = tags[0]
		}
		_ = e.profiler.Sample(prepCtx, primaryTag, payload)
	}

	circuits := e.registry.GetMatching(prepCtx, tags...)
	if len(circuits) == 0 {
		if prepSpan != nil {
			prepSpan.SetAttributes(telemetry.AttrMatchingCircuits.Int(0))
			telemetry.EndSpan(prepSpan, nil)
		}
		res := &types.SparkResult{
			OriginalInput:    payload,
			Passed:           true,
			ExecutedCircuits: nil,
		}
		if span != nil {
			span.SetAttributes(telemetry.AttrPassed.Bool(true))
			telemetry.EndSpan(span, nil)
		}
		return res, nil
	}

	normalizedInput := normalizeInput(payload)
	if prepSpan != nil {
		prepSpan.SetAttributes(telemetry.AttrMatchingCircuits.Int(len(circuits)))
		telemetry.EndSpan(prepSpan, nil)
	}

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
			if span != nil {
				span.SetAttributes(telemetry.AttrPassed.Bool(res.Passed))
				telemetry.EndSpan(span, err)
			}
			return res, err
		}
		sparkRes := &types.SparkResult{
			OriginalInput:    payload,
			ReturnedData:     make(map[string]any),
			Passed:           err == nil,
			ExecutedCircuits: []string{circuits[0].ID},
			Errors:           []error{err},
		}
		if span != nil {
			span.SetAttributes(telemetry.AttrPassed.Bool(sparkRes.Passed))
			telemetry.EndSpan(span, err)
		}
		return sparkRes, err
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

	if span != nil {
		span.SetAttributes(telemetry.AttrPassed.Bool(combinedResult.Passed))
		telemetry.EndSpan(span, nil)
	}

	return combinedResult, nil
}

// Conduct evaluates candidate items from the Catalog against entity state.
// Ineligible items are silently omitted, and qualified items have their dynamic output populated.
func (e *Engine) Conduct(ctx context.Context, req *types.ConductRequest) (*types.ConductResult, error) {
	if req == nil {
		return nil, errors.New("flux: conduct request required")
	}

	if req.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, req.Timeout)
		defer cancel()
	}

	var span trace.Span
	if e.tracer.IsEnabled() {
		ctx, span = e.tracer.Start(ctx, "flux.conduct",
			trace.WithAttributes(
				telemetry.AttrConductEntity.String(req.EntityID),
				telemetry.AttrConductCategory.String(req.TargetCategory()),
			),
		)
	}

	startTime := time.Now()

	// 1. Fetch Candidate Items matching target category partition in 1 call
	targetCategory := req.TargetCategory()
	items := e.catalog.GetByCategory(ctx, targetCategory)
	evaluatedCount := len(items)

	if evaluatedCount == 0 {
		res := &types.ConductResult{
			EntityID:       req.EntityID,
			Items:          nil,
			EvaluatedCount: 0,
			Duration:       time.Since(startTime),
		}
		if span != nil {
			span.SetAttributes(
				telemetry.AttrCandidateCount.Int(0),
				telemetry.AttrQualifiedCount.Int(0),
			)
			telemetry.EndSpan(span, nil)
		}
		return res, nil
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

			if ctx.Err() != nil {
				return
			}

			// Check item embedded qualification circuit
			if item.Circuit == nil {
				// No qualification circuit -> automatically qualified with base data
				evalResults[rankIdx] = &types.EvaluatedItem{
					ID:             item.ID,
					Data:           cloneMap(item.Data),
					ComputedOutput: cloneMap(item.Data),
					Score:          1.0,
					Rank:           rankIdx + 1,
				}
				return
			}

			// Evaluate embedded circuit against hydrated entity state via zero-allocation layered context
			sctx := state.AcquireLayeredContext(ctx, entityState, item.Data)
			res, err := e.executor.ExecuteCircuit(ctx, item.Circuit, sctx)

			// Ineligible / aborted items are SILENTLY OMITTED
			// If root node was pruned (condition failed) or execution aborted -> omit
			if err != nil || res == nil || !res.Passed || sctx.IsAborted() || sctx.IsPruned(1<<0) {
				state.ReleaseContext(sctx)
				return
			}

			computedOut := cloneMap(item.Data)
			if len(res.ReturnedData) > 0 {
				computedOut = cloneMap(res.ReturnedData)
			}
			state.ReleaseContext(sctx)

			evalResults[rankIdx] = &types.EvaluatedItem{
				ID:             item.ID,
				Data:           cloneMap(item.Data),
				ComputedOutput: computedOut,
				Score:          1.0,
				Rank:           0,
			}
		})
	}

	latch.Wait()

	if err := ctx.Err(); err != nil {
		if span != nil {
			telemetry.EndSpan(span, err)
		}
		return nil, fmt.Errorf("flux conduct: %w", err)
	}

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

	if span != nil {
		span.SetAttributes(
			telemetry.AttrCandidateCount.Int(evaluatedCount),
			telemetry.AttrQualifiedCount.Int(len(finalItems)),
		)
		telemetry.EndSpan(span, nil)
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

var structFieldCache = cache.NewBoundedCache[reflect.Type, []structFieldInfo](2048)

func getStructFields(t reflect.Type) []structFieldInfo {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if v, ok := structFieldCache.Get(t); ok {
		return v
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

	structFieldCache.Set(t, fields)
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

func cloneMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	res := make(map[string]any, len(m))
	for k, v := range m {
		res[k] = v
	}
	return res
}
