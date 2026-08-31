# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
