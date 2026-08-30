# Contributing to Flux

First off, thank you for considering contributing to Flux! It's people like you that make it an exceptional data plane tool.

## Technical Philosophy

- **Zero-Allocation Hot Path**: Event processing in `Spark` and scoring in `Conduct` must produce strictly **0 heap allocations** per event after warmup.
- **Sub-Microsecond Latency**: Keep $p50$ execution latency under 350ns. All branch pruning must execute in $O(1)$ time via bitmask operations.
- **Embedded & Decentralized**: Flux contains zero dedicated internal in-memory caches. All caching and persistence must delegate cleanly to `CacheBackend` (`Capacitor`).
- **Memory Safety & Sandbox**: All rule scripts evaluated via **Volt** must remain sandboxed, deterministic, and non-Turing-complete.
- **Concurrency Safety**: Multi-threaded execution across parallel Circuit tree branches must pass `go test -race` under high contention.

## Development Workflow

1. **Fork & Branch**: Create a feature branch matching your planned change.
2. **Local Development**:
   - Ensure your code follows `go fmt`.
   - Run tests: `go test -v ./...`
   - Run benchmarks: `go test -bench=. -benchmem ./...`
3. **Changelog & Versioning**:
   - **Crucial**: Update `CHANGELOG.md` under the `[Unreleased]` section for every pull request or change.
   - Bump the version in `version.go` if your changes warrant a version bump (following Semantic Versioning).
4. **Pull Request**:
   - Provide a clear explanation of what was added, changed, or fixed.
   - Ensure CI benchmarks and tests pass without regressions.

## Code of Conduct

Be respectful, constructive, and professional. We aim to build a welcoming community for everyone.

## Reporting Bugs

Use GitHub Issues to report bugs. Provide:
- A clear description of the issue.
- Minimal code snippet / Circuit definition to reproduce.
- Environment details (`go version`, OS, architecture).
