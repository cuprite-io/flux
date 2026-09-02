# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
