# Repository Guidelines

## Project Structure & Module Organization

This Go module provides one MCP endpoint backed by a gopls process per Git worktree. Root-level Go files implement the CLI, HTTP/stdio frontends, routing, request queues, and shared-process lifecycle. `internal/config/` owns limits and environment validation; `internal/transport/` owns JSON-RPC framing, stdio/SSE connections, and buffer accounting. Internal packages must not import the root command.

`internal/protocol/` owns pure MCP message adaptations. Tests and benchmarks live beside implementations in `*_test.go`; saved fuzz inputs live in package-local `testdata/fuzz/`. Read `SPEC.md` for behavioral contracts and `README.md` for usage. `LEASES.md` describes proposed ownership and idle-eviction work; only the open-file budget (`budget.go`, SPEC L6) evicts today.

## Build, Test, and Development Commands

Use Go 1.27 as specified in `go.mod`. Git and a gopls binary supporting `gopls mcp` must be on `PATH` for local operation.

- `go build .` builds the local command.
- `go run . http 127.0.0.1:6099 /absolute/path/to/repo` starts the loopback HTTP MCP endpoint at `/mcp`.
- `go run . bridge /absolute/path/to/repo` starts the stdio bridge.
- `go test -race .` checks root integration and lifecycle behavior; test changed internal packages explicitly. See `README.md` for coverage, real-gopls integration, and pprof commands.
- `golangci-lint fmt` formats Go code; `golangci-lint run ./...` must pass before committing. CI uses v2.13.2 and builds Linux/amd64 and Darwin/arm64.

## Coding Style & Naming Conventions

Use standard Go formatting with tabs, short lowercase package names, and descriptive mixed-case identifiers. Keep transport mechanics separate from routing and process ownership. Preserve bounded queues, cancellation semantics, process identity checks, and protocol-only stdout; send diagnostics to stderr.

## Testing Guidelines

Use the standard `testing` package with testify `require` for fatal checks and `assert` for non-fatal ones, plus neighboring helpers; only `assert` off the test goroutine (stubs, handlers, background goroutines), since `require` calls `FailNow`. Name tests `TestXxx`, benchmarks `BenchmarkXxx`, and fuzz targets `FuzzXxx`. Use `t.Parallel()` where safe; tests changing process-wide environment must stay serial. Use `testing/synctest` for deterministic bridge timing; keep real listeners and child processes outside its bubbles. Add regression coverage for changed behavior. CI requires 85% statement coverage per package and overall using `scripts/check_coverage.py`. Run tests only for affected packages locally; CI runs the full race suite.

## Commit & Pull Request Guidelines

Recent commits use imperative subjects with prefixes such as `fix:`, `refactor:`, `test:`, `docs:`, `chore:`, and `ci:`. Keep changes focused. PR descriptions should explain the problem, resulting behavior, and validation commands; link relevant issues and update behavioral documentation when contracts change. Ensure build, test, and lint CI jobs pass.
