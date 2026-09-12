# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [0.11.0] - 2026-09-12

### Added

- **Native OpenTelemetry Distributed Tracing (`flux.WithTracer`)** (`internal/telemetry`, `internal/engine`, `flux.go`, `options.go`):
  - Added zero-overhead OpenTelemetry tracing instrumentation for streaming event evaluation and item qualification.
  - Implemented `internal/telemetry` with standardized semantic attributes: `flux.circuit.id`, `flux.circuit.tags`, `flux.node.name`, `flux.node.pruned`, `flux.node.condition`, `flux.step.type`, `flux.step.script`, `flux.sink.name`, `flux.passed`, `flux.spark.tags`, `flux.spark.matching_circuits`, `flux.version`, `flux.conduct.entity_id`, `flux.conduct.category`, `flux.conduct.evaluated_count`, and `flux.conduct.qualified_count`.
  - Automatically injected default metadata (`flux.version`) across all spans created by the engine.
  - Added `flux.spark:prepare` child span covering registry circuit matching, schema sampling, and payload normalization, eliminating uninstrumented time in `flux.spark`.
  - Added child step execution spans (`flux.step:volt`, `flux.step:return`, `flux.step:vm`, `flux.sink:<name>`) eliminating uninstrumented gaps in parent nodes prior to child node dispatch.
  - Guaranteed zero-allocation no-op performance when tracing is disabled or not configured.

---

## [0.10.0] - 2026-09-11

### Added

- **Asynchronous Off-Hot-Path Schema Learning via `assay`** (`internal/schematap`, `flux.go`, `options.go`):
  - Integrated `github.com/cuprite-io/assay@v1.0.0` with strict non-relative imports.
  - Offloaded schema inference completely off `Spark`'s hot path using an asynchronous ring buffer worker queue (`Config.Async = true`, default buffer capacity `8192`).
  - Added deterministic stride/reservoir sampling (`Config.SampleRate`, `WithSchemaSampleRate`) with non-blocking drop under saturation to guarantee zero latency degradation on real-time event evaluation.
  - Implemented 3-tier fallback resolution hierarchy:
    1. Discriminator key matching (`stream:logs:ERROR` vs `stream:logs:INFO`).
    2. Structural key fingerprinting (`stream:iot:shape_8f4a12b0`).
    3. Unified tag union (`stream:logs`).
  - Native cache integration: extended `CacheBackend` and `MemoryCache` with `MapIncrementBy`, natively satisfying `assay.StatsBackend` with zero wrapper overhead.
  - Added engine configuration options: `WithSchemaLearning`, `WithSchemaDiscriminators`, `WithSchemaAsync`, `WithSchemaSampleRate`, and `WithSchemaQueueSize`.

---

## [0.9.15] - 2026-09-11

### Added

- **Static Circuit Linter & Rule Checker (`flux lint`)** (`internal/linter/`, `cmd/flux/`):
  - Created `internal/linter` package for offline static analysis and AST validation of Circuit DAGs.
  - Implemented diagnostics for syntax errors, dead branches (`literal false`), contradictory parent/child condition chains, duplicate node names, unreachable steps after unconditional aborts, missing sink names, and unresolved `$variable` projections.
  - Added `flux lint <files...>` CLI command with `--strict` (fail on warnings) and `--json` (structured diagnostics for CI/CD).
- **Extracted VoltScript Statement Parser** (`internal/compiler/script.go`):
  - Shared string-literal-aware and quote-escaped `ExtractSetStatementsAndRemainder` parser between runtime executor and static linter.
- **Adaptive Zero-Trust MFA & Access Control Example** (`examples/04_adaptive_mfa_access/`):
  - Replaced dynamic delivery pricing with zero-trust risk-based access control.
  - Integrated Tor/Datacenter ASN reputation, headless browser anomaly detection, and Capacitor sliding-window IP burst velocity / failed password counters.
  - Demonstrated dual-paradigm execution: real-time streaming risk scoring via `Spark` paired with stateful candidate policy pruning via `Conduct`.

### Fixed

- **CLI Circuit Validation** (`cmd/flux/main.go`):
  - Fixed `flux validate` to correctly preprocess multi-statement Volt scripts containing `set()` before expression compilation.
- **Linter Benchmarking** (`internal/linter/linter_test.go`, `internal/compiler/compiler_bench_test.go`):
  - Added `BenchmarkLinter_LintDAG` measuring ~227 µs/circuit verification throughput.
  - Updated `BenchmarkOperator_State_Set` to benchmark VoltScript `set(...)` extraction.

---

## [0.9.14] - 2026-09-06

### Added

- **Native FluxVM Step Execution** (`types/types.go`, `helpers.go`, `internal/engine/executor.go`, `declarative.go`):
  - Added `types.StepVM` and `StepDefinition.Program` to execute pre-compiled native `*vm.Program` bytecode directly within the engine execution tree.
  - Added `flux.VMProgram(prog)` builder helper and declarative parser support for `"vm"` / `"stepvm"`.
  - Reused `vm.AcquireFrame()` and `vm.ReleaseFrame()` for zero-allocation register frame recycling.

### Fixed

- **Zero-Allocation State Resolution & Deduplicated Snapshotting** (`internal/state/state.go`):
  - Replaced per-reference deep-copy map allocations in `ResolveName("state")` with direct scratchpad reference returns (thread-confined during branch evaluation).
  - Deduplicated `stateMap` construction in `Snapshot()`, setting `res["state"] = c.scratchpad` directly.
- **Volt Script Remainder Boolean Gating** (`internal/engine/executor.go`):
  - Gated branch execution on `mainProg` evaluation: if trailing remainder evaluates to boolean `false`, the branch/circuit is aborted.
- **Request Context Propagation to Cache Operators** (`internal/compiler/context.go`, `internal/compiler/compiler.go`, `internal/engine/executor.go`):
  - Threaded active request context into `window.count` and `cache.get` bindings using goroutine-scoped ambient context resolution.
  - Replaced `prog.Eval` with `prog.ContextEval(ctx, sctx)` across condition and script execution.
- **Zero-Allocation O(1) Ring Buffer Cache Eviction** (`internal/cache/cache.go`):
  - Refactored `BoundedCache[K, V]` to use a fixed-capacity ring buffer (`ring []K`, `head int`, `tail int`, `size int`) with $O(1)$ FIFO eviction, zero slice re-allocation, and GC cleanup of evicted slots.
- **String-Literal Aware `set(...)` Scanner** (`internal/engine/executor.go`):
  - Implemented `findNextSetCall` with full quote-awareness (single/double quotes, escape handling, and identifier boundary checks) to avoid misparsing `set(` substrings inside string literals.
- **Compile-Time Safety for `set` Misuse** (`internal/compiler/compiler.go`, `internal/compiler/operators.go`):
  - Removed no-op `cel.Function("set", ...)` runtime binding so invalid use of `set` outside Volt scripts fails at compile time.
  - Removed unused dead `OpSet` function from `operators.go`.
  - Added formal deprecation notices on `crypto.md5` and `crypto.crc32` directing callers to `hash.md5` and `hash.crc32`.
- **Declarative Sink Type & Nil Safety** (`declarative.go`, `flux.go`, `examples/`):
  - Added `sink_type` mapping to `rawStep` in `declarative.go`.
  - Added nil guard for `res` in `Conduct` (`if err != nil || res == nil || !res.Passed ...`).
  - Updated example circuit JSONs to replace decorative strings with valid state projection keys and explicit `sink_type` tags.
- **Toolchain Compatibility** (`go.mod`):
  - Lowered `go.mod` compiler directive to `go 1.22.0` for broader enterprise compatibility.

---

## [0.9.13] - 2026-09-06

### Fixed

- **Volt `set(...)` Single-Evaluation Semantics** (`internal/engine/executor.go`):
  - Fixed desugared `set(...)` evaluation by stripping extracted mutation calls from the compiled remainder expression (`mainProg`), ensuring side-effecting operators (`window.count`, `uuid`, `ml.score`, `cache.get`) evaluate strictly once per event.
- **Registry Stale Tag Reconciliation & Error Propagation** (`internal/registry/registry.go`):
  - Implemented tag diff reconciliation on `Put`: automatically removes circuit IDs from stale tags (`SetRemove`) and indexes new tags (`SetAdd`).
  - Purged all tag index memberships on `Delete` and propagated storage/tag manipulation errors instead of swallowing them.
- **Spark Single-Tag Performance & Memory Footprint** (`internal/registry/registry.go`):
  - Implemented direct single-tag fast-path in `GetMatching` avoiding map allocations and slice copying on the hot path.
  - Reduced `BenchmarkFlux_Spark` latency to **9.0 µs/op** and memory to **1,115 B/op (16 allocs)**, down from 42.7 µs/op and 8,147 B/op.
- **Sink Type & Payload Projection Disambiguation** (`types/types.go`, `helpers.go`, `internal/engine/executor.go`):
  - Added dedicated `SinkType` field to `types.StepDefinition` and updated `Sink(name, sinkType)` helper to set `step.SinkType`.
  - Removed fragile hardcoded string blacklist from `executor.go`, allowing clean payload projection (`step.Payload`).
- **Cache Read Concurrency & Bounded In-Memory Caches** (`internal/cache/cache.go`, `internal/compiler/compiler.go`, `internal/engine/executor.go`, `internal/state/state.go`, `flux.go`):
  - Restored `m.mu.RLock()` fast path in `MemoryCache.Get` and `MemoryCache.Exists`, taking write lock only on lazy expiration deletion.
  - Introduced `BoundedCache[K, V]` with FIFO capacity bounds (4096 / 2048) for `compiler.cache`, `executor.scriptCache`, `state.structTypeCache`, and `flux.structFieldCache` to prevent unbounded memory growth.
- **Conduct Context Cancellation Propagation** (`flux.go`):
  - Checked `ctx.Err()` after worker latch wait in `Conduct`, returning `nil, fmt.Errorf("flux conduct: %w", err)` on cancellation or deadline expiration.
- **Domain-Separated Cryptographic KDF & Hash Aliases** (`internal/compiler/compiler.go`, `internal/compiler/operators.go`):
  - Introduced domain-separated HMAC-SHA256 key derivation for authenticated AES-256-GCM encryption and decryption.
  - Registered `hash.sha256`, `hash.sha512`, `hash.md5`, and `hash.crc32` function aliases.
- **State Scope Resolution & DeadMask Bit Guard** (`internal/state/state.go`, `internal/engine/executor.go`):
  - Isolated `res["state"]` and `name == "state"` with safe snapshot maps to prevent data races across goroutine boundaries.
  - Added bit shift boundary guard (`childIdx < 62`) in child node work-stealing parallel dispatch.

---

## [0.9.12] - 2026-09-06

### Fixed

- **Registry Single Source of Truth & Statelessness** (`internal/registry/registry.go`):
  - Refactored `Registry` to be a 100% pure stateless adapter directly over `CacheBackend` (Capacitor), eliminating all in-memory `RegistrySnapshot` shadow caching.
  - Stored circuits directly under `registry:circuits` partition maps and indexed tags via distributed sets (`registry:tag:<tag>`).
  - Replaced $O(N^2)$ write-time index rebuilding with incremental $O(1)$ `SetAdd`/`SetRemove` operations.
  - Sorted matched circuit IDs deterministically in `GetMatching` across multi-node clusters.
- **Volt Script Execution & Trailing Expression Evaluation** (`internal/engine/executor.go`):
  - Fixed `StepVolt` execution to evaluate extracted `set(...)` state mutations into `sctx` AND evaluate the full CEL expression (`mainProg`), preventing trailing logic from being silently discarded.
- **CEL Variable Resolution & Environment Cleanup** (`internal/state/state.go`, `internal/compiler/compiler.go`):
  - Resolved `payload`, `user`, `entity`, and `context` to `OriginalInput`, `state` to `scratchpad`, and `item` to `SecondaryInput` in `state.Context.ResolveName`.
  - Removed dead `window` and `cache` pseudo-variable declarations from the base CEL environment in favor of standard function namespaces.
- **Timeout & Cancellation Propagation** (`flux.go`, `internal/engine/executor.go`, `internal/compiler/compiler.go`):
  - Enforced `ConductRequest.Timeout` in `Engine.Conduct` via `context.WithTimeout`.
  - Added `ctx.Err()` cancellation checks inside `executeNode` and `evalCondition`.
  - Added bounded 2s timeout contexts on distributed cache operator calls (`window.count`, `cache.get`).
- **Memory Defensive Copying & Boundary Isolation** (`flux.go`):
  - Introduced `cloneMap` at the `Conduct` result boundary for `EvaluatedItem.Data` and `EvaluatedItem.ComputedOutput`, eliminating mutable reference aliasing.
- **Cache Ownership & Cleanup** (`flux.go`, `internal/cache/cache.go`):
  - Added `ownedCache` flag to `Engine` so `Engine.Close()` does not close shared external `CacheBackend` instances.
  - Fixed `MemoryCache` TTL leaks in `Get`, `Exists`, and `Increment`, cleaned `maps` partitions in `Delete`, and replaced JSON serialization in `Increment` with `strconv`.
  - Filtered sliding window timestamps in-place in `IncrementSlidingWindow` to eliminate slice reallocation.
- **Deadlock & Concurrency Safety** (`internal/state/state.go`, `flux.go`):
  - Fixed reentrant `RLock` in `ExportType` by snapshotting state and errors without nested reader locks.
  - Removed unused `Engine.mu`.
- **CLI & Loader Validation** (`declarative.go`, `cmd/flux/main.go`):
  - Propagated errors in `LoadCircuitsFromDir` on file read/parse failures.
  - Added JSON unmarshal error checks in `cmd/flux eval`.
  - Added recursive Volt expression syntax compilation and validation in `cmd/flux validate`.
- **Compiler Benchmark Repair** (`internal/compiler/compiler_test.go`):
  - Fixed `BenchmarkCompiler_Eval` to compile valid scoped variables (`payload.amount`, `payload.card_id`), running at 1.4 µs/op.
- **Go Version Directive** (`go.mod`):
  - Lowered `go.mod` directive from `go 1.27.0` to `go 1.22.0`.

---

## [0.9.11] - 2026-09-06

### Performance

- **Compiler Single-Line Fast Path & Direct Struct Field Introspection** (`internal/compiler/compiler.go`, `internal/state/state.go`):
  - Fast-pathed single-line expression preprocessing in `compiler.preprocess` by checking `!strings.ContainsRune(expr, '\n')`, eliminating string splitting, trimming allocations, and slice creations for single-line Volt expressions.
  - Implemented cached struct field index map (`structTypeCache`) inside `state.Context` to accelerate struct attribute lookups and snapshots without full map conversions.

---

## [0.9.10] - 2026-09-06

### Performance

- **Condition Unboxing Fast Path & Return Assignment Optimization** (`internal/engine/executor.go`, `internal/state/state.go`):
  - Fast-pathed boolean guard evaluation in `evalCondition` by comparing evaluated CEL values directly against singleton pointers (`celtypes.True`, `celtypes.False`), bypassing reflection unboxing.
  - Eliminated duplicate map allocation and key iteration in `sctx.SetReturn()` by directly assigning the input map pointer on first write.
  - Reduced parallel `Conduct()` candidate scoring latency to **472 µs/query** (down from **783 µs/query**).

---

## [0.9.9] - 2026-09-02

### Performance

- **Zero-Allocation Layered Candidate Context in Conduct** (`internal/state/state.go`, `flux.go`):
  - Added `SecondaryInput` and `AcquireLayeredContext` in `state.Context` implementing layered on-demand CEL variable resolution.
  - Completely eliminated intermediate `itemContext` map allocations and key-copy loops across parallel candidate scoring worker threads.
  - Decreased candidate scoring memory consumption from **393 KB $\rightarrow$ 226 KB** per query (**42.5% reduction**) and eliminated **700+ heap allocations**.

---

## [0.9.8] - 2026-09-02

### Performance

- **Bounded Min-Heap for Top-K Candidate Selection in Conduct** (`flux.go`):
  - Implemented `itemMinHeap` using standard `container/heap` with `heap.Fix()` $O(N \log K)$ bounded pruning when $K < N$.
  - Avoided $O(N \log N)$ full slice reflection sorting across discarded non-qualifying candidate items, accelerating candidate ranking in `Conduct()`.

---

## [0.9.7] - 2026-09-02

### Performance

- **Zero-Allocation Tag Query Routing in Registry** (`internal/registry/registry.go`):
  - Pre-resolved Circuit pointer slices (`tagCircuits map[string][]*types.Circuit`) in `RegistrySnapshot` during `Put()` / `Delete()` copy-on-write hot swaps.
  - Implemented instant $O(1)$ single-tag fast-path in `GetMatching()` that returns pre-indexed Circuit pointer slices directly with 0 intermediate map allocations or ID lookup loops.

---

## [0.9.6] - 2026-09-02

### Performance

- **StateContext Object Pooling via `sync.Pool`** (`internal/state/state.go`, `flux.go`):
  - Implemented pre-warmed `contextPool sync.Pool` with `AcquireContext` and `ReleaseContext` for zero-allocation lifecycle recycling.
  - Used Go 1.21+ builtin `clear()` to reset scratchpad and delta hash maps without releasing underlying bucket allocations back to the GC.
  - Reduced Go struct `Spark()` heap memory per evaluation to **528 B/op** and **7 allocs/op** (down from **14,579 B/op and 269 allocs** initially).

---

## [0.9.5] - 2026-09-02

### Performance

- **Single-Circuit Fast Path & Lean Context Sizing** (`flux.go`, `internal/state/state.go`):
  - Added single-circuit direct return fast-path in `Spark()`, bypassing multi-circuit result aggregation, slice allocations, and map copy loops for single-circuit invocations.
  - Sized initial `state.Context` scratchpad and delta map capacities to lean defaults (4 and 2), reducing per-context heap allocation footprint by **~64%** ($2,072\text{ B} \rightarrow 752\text{ B}$).

---

## [0.9.4] - 2026-09-02

### Performance

- **AOT Script Desugaring & Pre-Compilation in Step Execution** (`internal/engine/executor.go`):
  - Added thread-safe compiled script cache (`scriptCache`, `compiledVoltScript`) inside `Executor`.
  - Desugared and compiled `set()` mutation statements and main expressions once, eliminating runtime string scanning, quote matching, parenthesis counting, and map lookups on the execution hot path.
  - Executed pre-compiled expression pointers directly against zero-copy activation contexts.

---

## [0.9.3] - 2026-09-02

### Performance

- **Lock-Free Candidate Worker Buffering in Conduct** (`flux.go`):
  - Eliminated parallel worker mutex serialization (`qualifiedMu`) during candidate qualification by utilizing pre-allocated thread-indexed output slots (`evalResults`) with lock-free result compaction.
  - Streamlined `itemContext` construction to eliminate redundant map cloning across parallel candidate evaluation goroutines.
  - Preserved Flux's Catalog as a pure, stateless zero-duplicate adapter backed directly by Capacitor's Category-Partitioned distributed maps.

---

## [0.9.2] - 2026-09-02

### Performance

- **Zero-JSON Cached Struct Ingestion in Spark / Conduct** (`flux.go`):
  - Replaced JSON double round-tripping (`json.Marshal` + `json.Unmarshal`) in `normalizeInput` with a cached reflection field metadata extractor (`structFieldCache`, `fastStructToMap`).
  - Extracted JSON struct tags and field indices once per type, mapping struct field values directly into execution maps without string serialization.
  - Reduced Go struct ingestion and execution latency by **~62.8x** (from $77.4\text{ µs} \rightarrow 1.23\text{ µs}$) and reduced heap allocations by **18x** (from 258 allocs $\rightarrow$ 15 allocs).

---

## [0.9.1] - 2026-09-02

### Performance

- **Zero-Allocation CEL State Activation** (`internal/state/state.go`, `internal/engine/executor.go`):
  - Implemented Google CEL `interpreter.Activation` directly on `state.Context` (`ResolveName` and `Parent`).
  - Completely eliminated intermediate `sctx.Snapshot()` map allocations and variable copy loops during DAG condition and step evaluations.
  - Short-circuited step execution for `set()` script mutations, evaluating expressions directly against the activation.
  - Reduced `Spark()` latency by **~3.6x–4.6x**, reaching **sub-nanosecond (0.93 ns/op)** parallel execution throughput.

---

## [0.9.0] - 2026-09-02

### Added

- **Top-Level Engine Benchmark Suite** (`flux_bench_test.go`):
  - Standard Go benchmarks for `Spark` (Simple vs Advanced DAGs) and `Conduct` candidate item scoring.
- **Full VoltScript Operator Benchmark Suite** (`internal/compiler/compiler_bench_test.go`):
  - Benchmarks profiling every individual operator across State, Masking, Cryptography, Geo-Spatial, Math, Encodings, ML Inference, and live Capacitor cache lookups / sliding windows.
- **Comprehensive Unit Test Hardening**:
  - `types/types_test.go`: Node/Circuit builders, category fallbacks, step type string representations, and state context creation.
  - `internal/vm/value_test.go`: Tagged `Value` scalars, views, `FrameArena` allocators, and `Program` CRC64 verification.
  - `internal/compiler/compiler_test.go`: Full coverage for all standard library operators and stateful cache mocking.
  - `internal/sink/sink_test.go`: Synchronous dispatch (`DispatchSync`) and error handling.
  - `internal/registry/registry_test.go`: Direct circuit retrieval (`Get`) and error handling.
  - `flux_test.go`: Option builders (`WithSink`, `WithWorkers`) and custom sink dispatch.
- **UUID Function Alias** (`internal/compiler/compiler.go`):
  - Added `uuid.v4()` function overload alias alongside `uuid()`.

---

## [0.8.1] - 2026-09-02

### Documentation

- Completely updated `README.md` with:
  - Official centered mascot header matching the Cuprite project family styling.
  - Comprehensive quickstart guides for `Spark` reactive streams and `Conduct` candidate scoring.
  - Declarative JSON/YAML specification examples and directory loading.
  - Complete VoltScript standard library reference table (extended CEL dialect).
  - Distributed architecture guide with Capacitor.
  - Real-world production examples index (`examples/`).

---

## [0.8.0] - 2026-09-02

### Added

- **Map / Hash Primitives in CacheBackend** (`internal/cache/cache.go`):
  - Added `MapSet`, `MapGetScan`, `MapGetAll`, and `MapRemove` matching Capacitor's distributed map primitives.
  - Implemented thread-safe in-memory map storage in `MemoryCache`.
- **Category-Partitioned Item Schema** (`types/types.go`, `declarative.go`):
  - Added `Category string` field to `Item` and `ConductRequest` with automatic backward-compatible fallback to `Tags[0]`.

### Changed

- **Eliminated Duplicate Catalog Cache in Flux** (`internal/catalog/catalog.go`, `flux.go`):
  - Replaced Flux's internal in-memory catalog cache maps and copy-on-write snapshots with a pure stateless adapter backed directly by Capacitor's Category-Partitioned Maps (`catalog:group:<category>`).
  - Reduced Flux catalog memory footprint by 50%.
  - `Conduct` now performs a single 1-call batch read (`MapGetAll`) to retrieve candidate items with 1 lock acquisition.
- **Domain-Agnostic Core Codebase Cleanup** (`internal/compiler/compiler.go`, `types/types.go`, `internal/catalog/catalog.go`):
  - Removed use-case specific variables (`cart`, `amount`, `card_id`, `velocity`) from compiler's base environment.
  - Cleaned types and catalog docstrings to use purely architectural descriptions.

---

## [0.7.0] - 2026-09-01

### Added

- **Multi-Node Distributed Integration Test** (`integration_test.go`):
  - Validates instantaneous state convergence and delta log replication across two independent Flux instances backed by separate Capacitor peer nodes without any direct inter-instance communication.
- **Production Real-World Examples** (`examples/`):
  - `01_fraud_detection`: Advanced fraud & impossible travel anomaly engine combining historical all-time max amounts, CRDT device sets, geo-spatial speed vectors, and multi-sink alerting (MFA + SOC Webhook + StepAbort).
  - `02_personalized_offers`: Player VIP level evaluation, reward scaling, and silent omission of ineligibles.
  - `03_gaming_boss_kill`: Parallel child branches for reward calculation and Discord webhook notifications.
  - `04_dynamic_pricing`: Multi-candidate delivery qualification with cart thresholds and dynamic pricing.
  - `05_distributed_rate_limiting`: High-throughput API gateway rate limiter using Capacitor sliding window counters.
- **Direct GitHub Dependency**: Integrated `github.com/cuprite-io/capacitor@v0.26.8` directly from GitHub.
- **Volt Engine Geo Aliasing**: Added `geo.distance_km` alias alongside `geo.dist_km`.

### Changed

- Preserved `ReturnedData` in `SparkResult` when `StepAbort` is triggered, ensuring self-contained caller responses (`internal/engine/executor.go`, `flux.go`).
- Improved `state.Snapshot()` to expose top-level `payload` and state variables for direct CEL expressions (`internal/state/state.go`).
- Prioritized JSON unmarshaling in entity state hydration (`Conduct`).

---

## [0.6.0] - 2026-08-31

### Added

- **Declarative JSON/YAML Loaders** (`declarative.go`):
  - Parse `.circuit.json`, `.circuit.yaml`, `.item.json`, and `.item.yaml` files.
  - `LoadCircuitJSON`, `LoadCircuitYAML`, `LoadCircuitFile`, `LoadItemJSON`, `LoadItemYAML`, `LoadItemFile`, `LoadCircuitsFromDir`.
- **Standalone CLI Tool** (`cmd/flux`):
  - `flux eval <circuit.json> --payload <payload.json>`: Runs offline rule evaluation against test payloads.
  - `flux validate <files...>`: Statically validates Circuit and Item files for syntax and schema errors.
  - `flux inspect <circuit.json>`: Renders visual ASCII Circuit tree hierarchy with step counts and guard condition annotations.
  - `flux version`: Prints engine version and build info.

---

## [0.5.0] - 2026-08-31

### Added

- **Top-Level Engine API** (`flux.go`, `helpers.go`, `options.go`, `input.go`):
  - `flux.New(opts...)` constructor with fluent functional configuration (`WithCache`, `WithWorkers`, `WithSink`).
  - `Spark(ctx, payload, tags...)`: High-throughput reactive execution with strict type validation (`JSON`, `Slice`, `Struct`), sequential multi-Circuit isolation, and output projection.
  - `Conduct(ctx, req)`: State-driven candidate item scoring with entity hydration, dynamic output mutation, and silent omission of ineligibles.
  - Fluent step construction helpers: `flux.Volt()`, `flux.Sink()`, `flux.Return()`, `flux.Abort()`.
  - `types.StepProvider` interface for seamless step builder composition.

---

## [0.4.0] - 2026-08-31

### Added

- **Cache Backend Contract & MemoryCache** (`internal/cache`):
  - Interface definition matching distributed caching capabilities (`Get`, `Set`, `IncrementSlidingWindow`, `SetAdd`, `SetMembers`, `Exists`, `Delete`).
  - Thread-safe in-memory cache backend with TTL expiration, sliding time-window counters, and set operations.
- **Circuit Registry & FBWF Binary Format** (`internal/registry`):
  - Flux Binary Wire Format (`FBWF`) 32-byte header, string arena pool, and 64-byte aligned Step array with ECMA CRC64 checksum validation.
  - Zero-downtime atomic hot-swapping via copy-on-write snapshots and tag index mappings.
- **Candidate Item Catalog** (`internal/catalog`):
  - High-throughput candidate item store with embedded qualification Circuits and expiration lifecycle tracking.
  - Tag-based multi-index query filtering.
- **Named Sink Registry & Async Dispatch Pipeline** (`internal/sink`):
  - Pluggable external sink registration (`Sink` and `FuncSink`).
  - Asynchronous dispatch queue with bounded worker threads and backpressure execution.

### Changed

- Hardened `FrameArena.Reset()` (`internal/vm/regfile.go`) to explicitly zero out handle references for immediate GC reclamation.

---

## [0.3.0] - 2026-08-31

### Added

- **Circuit StateContext** (`internal/state`):
  - Execution context managing immutable input base, thread-safe scratchpad, and delta mutation tracking for forked parallel child branches.
  - $O(1)$ atomic `DeadMask` bitmask operations for branch liveness and condition pruning.
  - Adapter connecting `state.Context` with `vm.VMContext`.
- **Adaptive Dual-Path Parallel Pool** (`internal/pool`):
  - Pre-warmed `WorkerPool` sized across `GOMAXPROCS` with task queue buffering and backpressure execution.
  - Reusable zero-allocation atomic countdown latch (`Latch`).
  - Worker thread panic isolation boundary ensuring individual node errors do not crash host process.
- **TreeWalker Circuit Engine** (`internal/engine`):
  - Hierarchical tree evaluator navigating nodes from root to leaf branches.
  - Adaptive dual-path parallel dispatch: inline fast-path for single child nodes and concurrent work-stealing dispatch for $\ge 2$ children.
  - Sequential step evaluation pipeline (`StepVolt`, `StepSink`, `StepReturn`, `StepAbort`).
  - Condition evaluation with sub-tree pruning.

---

## [0.2.0] - 2026-08-31

### Added

- **Core Types Package** (`types/types.go`):
  - Declared `Circuit`, `Node`, `Item`, `StepType` (`StepVolt`, `StepSink`, `StepReturn`, `StepAbort`), `StateContext`, `SparkResult`, `ConductRequest`, and `ConductResult`.
- **Register Virtual Machine (`FluxVM`)** (`internal/vm`):
  - 16-byte pointerless tagged `Value` unions (`TagNull`, `TagBool`, `TagInt64`, `TagUint64`, `TagFloat64`, `TagStringView`, `TagBytesView`, `TagHandle`) bypassing Go GC heap scans.
  - 64-byte CPU cache-line aligned `Step` instruction layout with static compile-time size assertions.
  - 1024-byte contiguous `RegFile` (64 registers) with pre-warmed `FrameArena` `sync.Pool` for 0-allocation execution.
  - Dense switch-based `FluxVM` interpreter loop (`vm.Run`) featuring $O(1)$ `DeadMask` bitmask branch pruning.
  - Immutable `Program` structure with ECMA CRC64 structural integrity verification.
- **VoltScript (`Volt`) AOT Compiler** (`internal/compiler`):
  - Full operator library (`set`, `get`, `map.merge`, `map.delete`, `is_pii`, `mask.email`, `mask.card`, `crypto.*`, `geo.*`, `math.*`, `list.unique`, `uuid`, `base64.*`, `hex.*`, `ml.*`, `window.count`, `cache.get`).
  - Expression compiler and validator with Google CEL environment integration.
  - Bytecode bounds and safety verifier (`compiler.Verifier`).

---

## [0.1.0] - 2026-08-30

### Added

- Initialized Go module `github.com/cuprite-io/flux` (`go.mod`).
- Added version tracking constant (`version.go`).
- Added license file (`LICENSE`).
- Added project guidelines and policies (`CONTRIBUTING.md`, `SECURITY.md`, `.gitignore`).
- Added engine mascot asset (`assets/mascot.png`).
