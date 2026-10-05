# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

@AGENTS.md

## Commands

```sh
go test -race -run '^TestToolCallSpanningTwoWorktreesIsRefused$' .          # one root test
go test -race ./internal/transport                                          # one internal package
go test -race -tags=integration -run '^TestEndToEndRealGopls$' -timeout=3m . # real gopls (needs gopls on PATH)
go test -race -coverprofile=/tmp/gopls-manager.cover . ./internal/...
python3 scripts/check_coverage.py /tmp/gopls-manager.cover --minimum 85     # CI coverage gate
python3 -m unittest discover -s scripts                                     # tests for the coverage script
golangci-lint run ./...                                                     # no repo config: default linters
```

Benchmarks and pprof recipes are in README.md "Development".

## Architecture

One `router` (`bridge.go`) per frontend session. It owns routing state, the in-flight
request map and per-worktree `lane`s. Both frontends build on it:

- **stdio bridge** (`session.go`): `readFromClient` → `readIngress` admits calls into
  the 64-message routing queue and handles `notifications/cancelled` immediately →
  `routing.go` resolves the target worktree → the call goes to that worktree's `lane`
  (`lane.go`) → the upstream SSE connection → `readFromUpstream` (`upstream.go`)
  answers `roots/list` itself and forwards responses → `writeToClient`. Pathless calls
  follow `sticky`.
- **HTTP** (`http.go`): `httpBackend` wraps a router with `stateless = true`. Pathless
  calls always go to home, there is no sticky state, and client IDs are rewritten to
  internal sequence IDs so different clients cannot collide.

Worktree resolution (`paths.go`, `resolver.go`) shells out to `git rev-parse`. One
resolver worker does the filesystem work behind a path memo and a directory memo;
memo hits and pathless calls skip the worker.

Request accounting (`requests.go`, `operations.go`) tracks client-visible calls
(`awaitingUpstream`) apart from upstream operation credits. A credit is released
only when gopls actually answers, not on local cancel or timeout.

Shared-process lifecycle (`manager.go`, `registry.go`, `process.go`, `probe.go`,
`maintenance.go`) lives across processes. `~/.local/share/gopls-ports.map` is
flock-guarded and shared by every manager on the machine, and gopls children outlive
the manager. Dropping a record without killing its process strands a 1–2 GB index,
so liveness verdicts are conservative: only "connection refused" is conclusive, and
a `ps` identity check runs before any signal. SPEC.md §8 has the rules. The one
deliberate eviction is the open-file budget (`budget.go`, SPEC L6): on macOS each
gopls holds a kqueue fd per worktree entry, so maintenance terminates the servers
with the lowest aged use counts while the total lsof row count is over the limit,
sparing any worktree with an upstream call under 30 s old (`pending.go`, which also
publishes them per manager pid for cross-process visibility).

## Testing conventions

- `TestDocsNameTestsThatExist` fails if SPEC.md or README.md cites a test name or
  table-case name (`(case "…")`) that no longer exists. When you rename a test or a
  table row, update the docs in the same change. Each SPEC.md invariant names the
  test that pins it, so add the citation when you add an invariant.
- `newTestManager` (`manager_test.go`) gives a manager its own map file. Its `alive`,
  `ready` and `start` fields are function seams to stub. `router.dial` is the
  equivalent seam for upstream connections.
- `stubGopls` and tests that set `HOME`/`PATH` with `t.Setenv` must stay serial.
- `newLinkedWorktree` (`bridge_test.go`) creates a real linked git worktree for
  routing tests.
