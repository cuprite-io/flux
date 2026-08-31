# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
