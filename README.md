# gopls-mcp-manager

One stdio or stateless Streamable HTTP MCP endpoint routes tool calls to a gopls
process per Git worktree. The manager starts processes on demand.

## Install

```sh
go install github.com/ajvengo/gopls-mcp-manager@latest
```

Building requires Go 1.27 or newer. Runtime requires Git, Go, and a Go-built
`gopls` v0.23.0 or newer on `PATH`; CI exercises v0.23.0 and v0.24.0 on Linux and
macOS. The manager checks gopls' package identity and declared module version,
without a dependency allowlist. Missing, invalid and `(devel)` versions fail.
`list` and `status` do not need gopls to inspect retained state.

## Use

Register the manager as your stdio MCP server.

```json
{
  "mcpServers": {
    "gopls": { "command": "gopls-mcp-manager" }
  }
}
```

| Command | Effect |
| --- | --- |
| `gopls-mcp-manager http [address [path]]` | Serve stateless MCP at `/mcp`. Defaults to `127.0.0.1:6099` and home `.` |
| `gopls-mcp-manager [bridge [path]]` | Run stdio, the default command. Home defaults to `.` |
| `gopls-mcp-manager ensure [path]` | Find or start gopls for the worktree and print its port |
| `gopls-mcp-manager supervisor` | Run the persistent owner in the foreground for startup diagnostics |
| `gopls-mcp-manager list` | Sweep records and list retained servers, including terminating ones |
| `gopls-mcp-manager status` | Read-only JSON with recorded server count, identity-checked RSS, termination state and log sizes |
| `gopls-mcp-manager trim-logs` | Offline: truncate managed logs above the configured threshold in place |
| `gopls-mcp-manager restart` | Drain calls and restart all managed gopls generations, preserving client connections |
| `gopls-mcp-manager restart --hot [--binary path]` | Fork/exec the selected manager binary and restart all managed gopls generations |
| `gopls-mcp-manager reload [--binary path]` | Alias for `restart --hot`; defaults to the invoking binary |

`path` is resolved to the root of the worktree containing it, with symlinks
removed. The worktree the bridge starts in is its **home**.

### Shared owner and reclamation

Bridge, HTTP and `ensure` share an owner that survives client disconnects.
It checks retirement every 30 s. `GOPLS_MANAGER_IDLE_TIMEOUT` (default `1h`)
sets both the idle interval and abandoned-work grace. Quiet connections allow
retirement and reconnect on the next call. Ordinary retirement sends SIGTERM
and waits 10 s. Unconfirmed exit or failed cleanup retains capacity.
[Owner contract](SPEC.md#persistent-owner-contract)

Frontends must match the owner's private protocol and resource configuration.
The selected Go toolchain must report the exact private telemetry directory and
mode `off` before spawn and each ID-bearing upstream write.
[gopls and toolchain requirements](SPEC.md#9-spawning)

### Restart and binary reload

Run restart commands as the owner's OS user with the same configuration.
Plain restart replaces gopls generations and rebuilds indexes.

```sh
gopls-mcp-manager restart --timeout 5m
```

Build and activate an updated owner from the checkout.

```sh
mkdir -p "$HOME/.local/bin"
go build -o "$HOME/.local/bin/gopls-mcp-manager-next" .
gopls-mcp-manager reload --binary "$HOME/.local/bin/gopls-mcp-manager-next" --timeout 5m
gopls-mcp-manager status
```

`reload` and `restart --hot` select the invoking executable by default. Keep
the selected binary at its path for later child launches and update service
launch commands for future starts. Reload waits for the successor to finish
restarting and prints a JSON receipt. Both restart modes require a running owner
and accept a positive `--timeout` up to `1h` (default `5m`).

Admitted calls drain before frontends pause at message boundaries. Work racing
the pause can abort the restart. New calls retain their original deadlines,
and sent requests are never replayed. A hot successor resumes traffic after
confirmed old-owner exit. The [restart contract](SPEC.md#persistent-owner-contract)
lists capacity checks, refusal conditions and resume order.

Reload preserves public connections and existing frontend binaries. Restart
the service normally to update frontends, change configuration, or upgrade an
owner without handoff support. See [legacy migration](#legacy-migration) before
reusing the registry.

### Stateless HTTP

Start the loopback HTTP frontend.

```sh
gopls-mcp-manager http 127.0.0.1:6099 /absolute/path/to/repo
```

Configure your MCP client's Streamable HTTP server URL as
`http://127.0.0.1:6099/mcp`. The endpoint accepts MCP POST requests and returns
JSON responses. It issues no session ID and ignores supplied session IDs.
GET and DELETE return 405. The listener is restricted to loopback. Public HTTP
requests require a numeric loopback or `localhost` Host. Cross-origin browser
POSTs are rejected before backend admission. Local clients can omit browser
headers. The SDK override `MCPGODEBUG=allowsessionsinstateless=1` restores
legacy session-header and DELETE behavior.

HTTP routes each request independently. Absolute `file`, `dir` and `files`
arguments select a worktree. Pathless calls use home. To change their target,
start another HTTP frontend with a different home and port.

The owner reuses bounded SSE connections to gopls. It forwards tool lists,
schemas, pagination, results and errors using unique internal request IDs.
Unsolicited notifications, progress streams and client roots are omitted.

Cancel the HTTP request to cancel its internal call. Separate cancellation
POSTs have no cross-request effect. The owner completes cancellation locally
and may notify the owning SSE connection.

Cache misses use a separate 64-message queue. Warm and pathless calls can
route during a cold lookup. Delivery may still wait for toolchain validation
on the same filesystem worker.

Each HTTP exchange reserves a maximum-size body allowance through response
completion, including slow or chunked requests. The default cap is 16
exchanges. Excess requests receive 503. Defaults are 4 MiB per body, 5 s for
headers, 30 s for body reads and 30 s from first response output for writes.
Tool execution uses the optional execution timeout. Failed backends retire
after active handlers and queue cleanup. A later request can open a replacement.
[HTTP contract](SPEC.md#stateless-http-frontend)

## Routing

All resolvable absolute paths in a `tools/call` must name one worktree. Split
calls that span worktrees. Stdio remembers the last resolved worktree in wire
order. Calls without usable paths follow it, or use home if it is unset.
Ordinary non-tool traffic goes to home.

The router caches successful `git rev-parse --show-toplevel` lookups by path
and physical directory. On a miss, all paths in the call share a 10 s budget,
including the wait for the shared filesystem worker. Queueing, routing and
delivery share a 40 s ingress budget, with a separate 30 s delivery ceiling.
The stdio reader handles cancellation even if the 64-message routing queue
is full.

## Bridge behavior

- The client initializes with home. Later upstream handshakes run privately.
  The bridge adds a missing roots capability and answers `roots/list` with
  filtered roots. Other server requests receive `method not found`. A failed
  reply closes its upstream connection and fails the pending calls.
- Each worktree has a delivery lane with 64 delivery and 64 cancellation
  slots. Full queues reject requests and drop notifications. Expired queued
  calls cannot start gopls.
- Cancellation releases the local slot, cancels pending delivery and suppresses
  late results. Delivered calls can receive an advisory on their owning
  connection. Remote credits survive local cancellation, timeout and disconnect
  until a definitive response or confirmed generation exit.
- Upstream failure leaves the session open. A later call can reconnect.
  Uncertain writes return a “may have executed” error and are never replayed.
- EOF, Ctrl+C and SIGTERM detach stdio without waiting for responses. EOF
  exits successfully. Signal cancellation exits with an error.
  [Failure and shutdown contract](SPEC.md#5-failure-semantics)

## Limits and measurements

| Environment variable | Default | Meaning |
| --- | --- | --- |
| GOPLS_MANAGER_MAX_OUTSTANDING | 1024 | Outstanding calls per session |
| GOPLS_MANAGER_MAX_OUTSTANDING_PER_LANE | 128 | Outstanding calls per worktree |
| GOPLS_MANAGER_MAX_LANES | 64 | Retained worktree lanes per session; existing lanes remain usable at capacity |
| GOPLS_MANAGER_MAX_SERVERS | 64 | Recorded shared servers allowed before a new spawn; existing servers remain reusable |
| GOPLS_MANAGER_MAX_HTTP_CONNECTIONS | 128 | Accepted HTTP sockets, capped before parsing headers |
| GOPLS_MANAGER_MAX_OWNER_SESSIONS | 16 | Combined frontend attachments and cached HTTP backends |
| GOPLS_MANAGER_MAX_OWNER_OPERATIONS | 1024 | Global queued/unconfirmed operations including handshakes and controls |
| GOPLS_MANAGER_IDLE_TIMEOUT | 1h | Positive idle and abandoned-work grace duration |
| GOPLS_MANAGER_MAX_LOG_BYTES | 67108864 | Aggregate managed log retention, including retired worktrees |
| GOPLS_MANAGER_RETAINED_MESSAGE_BUDGET | 67108864 | Owner-wide retained-message reservations; at least one maximum-size message |
| GOPLS_MANAGER_MAX_CACHE_ENTRIES | 4096 | Entries in each path/directory memo; a full memo clears before inserting |
| GOPLS_MANAGER_CACHE_TTL | 5m | Positive duration; both memo levels expire together, lazily on access or metrics snapshots |
| GOPLS_MANAGER_MAX_MESSAGE_BYTES | 4194304 | Maximum stdio JSON value (including preceding whitespace), HTTP body, upstream POST and complete SSE event |
| GOPLS_MANAGER_HTTP_BODY_BUDGET | 67108864 | Maximum reserved HTTP request-body bytes; must hold at least one maximum-size message |
| GOPLS_MANAGER_SSE_BUFFER_BUDGET | 67108864 | Shared live SSE frame capacity, including replacement buffers during growth; at least one maximum-size message |
| GOPLS_MANAGER_LOG_TRIM_BYTES | 67108864 | Offline `trim-logs` threshold |
| GOPLS_MANAGER_MAX_OPEN_FILES | 30000 | Owner, guardian and child FD reservations enforced before spawn |
| GOPLS_MANAGER_EXECUTION_TIMEOUT | 0s | Optional timeout after delivery; 0 disables it |
| GOPLS_MANAGER_METRICS | unset | Set to 1 for JSON metrics on stderr |
| GOPLS_MANAGER_DIRECTORY_FILTERS | unset | Comma-separated gopls `directoryFilters` rules, e.g. `-docs,-**/node_modules,+docs/go`; applied after each worktree's go.mod `ignore` directives to the roots gopls watches |

Counts must be positive. Duration settings accept Go durations such as `2m`.
Oversized input ends stdio, returns HTTP 413, or closes the upstream SSE
connection, depending on the transport. Exhausting SSE capacity also closes
the connection. Oversized upstream POSTs fail before delivery. A full
64-message client output queue fails the session.
The [resource ledger](SPEC.md#resource-ledger) lists what each budget covers.

Metrics go to stderr every 30 s and at session exit. `status` returns
read-only JSON with identity-checked RSS in KiB, log usage, and owner
operation counts. See [configuration and observation](SPEC.md#11-configuration-and-observation).

### Open-file budget

On macOS, fsnotify's kqueue backend opens every entry under watched roots,
including non-Go files. The owner reserves descriptor capacity before spawn
and limits watched entries through the roots it reports to gopls.

### Directory filters

The manager applies `go.mod` `ignore` directives from the worktree and modules
listed in `go.work` before `GOPLS_MANAGER_DIRECTORY_FILTERS`. Rules use gopls
directory-filter syntax. Last match wins. `-,+cmd,+internal` watches only those
subtrees.
Re-includes containing `**` can require a full walk. The watcher always skips
`.git`, other dot-prefixed directories, underscore-prefixed directories and
`testdata`.

Directories above an excluded subtree can have unwatched entries. The owner
checks for missed changes and deleted worktrees every 30 s. Changes requiring
new roots take effect with a new generation after restart or retirement.
Changing the environment rules requires a normal service restart.
[Roots and freshness contract](SPEC.md#4-server-initiated-requests)

## State on disk

Cleanup action locks use stable per-port names (`gopls-ports.map.cleanup-<port>.lock`),
so new locks are bounded by the 4001-port allocation range. Their inodes stay
in place for flock safety. Older PID/port-keyed locks are retained; upgrade all
sharing managers and stop them before manually removing historical lock files.

| Path | Contents |
| --- | --- |
| `~/.local/share/gopls-ports.map` | Generation records: worktree, port, child/guardian PIDs, owner boot, generation, start times and termination state; [format](SPEC.md#6-port-map-file) |
| `~/.local/share/gopls-ports.map.lock` | flock held around every read-modify-write |
| `~/.local/share/gopls-ports.map.owner.lock` | Singleton lock held for the owner's lifetime and transferred during hot restart |
| `~/.local/share/gopls-mcp-guardians/` | Guardian manifests, stable lifetime leases and private `telemetry-off/` state |
| `~/.local/share/gopls-mcp-logs/<hash>.log` | Bounded stdout+stderr log for that worktree |

The map is written atomically (temp file, `fsync`, rename) with mode `0600`.
Ports are picked from 61100–65100, seeded by a hash of the worktree path so the
same tree tends to get the same port, then probed forward until one is free.

`status` reports per-record log sizes and aggregate bytes, largest file, file
count and oversized count, including logs left after a record disappears.
The owner automatically retains managed logs within its aggregate byte/file cap.
Children write through guardian pipes into the bounded owner logger and do not
inherit append file descriptors. Do not mutate managed logs while the owner runs:
changed file identity/size fails the logger and fences delivery.
Offline `trim-logs` clears the **entire contents** of managed regular `.log` files above
64 MiB by default; save wanted diagnostics first. It scans in bounded batches,
skips unrelated names and symlinks, and truncates in place without renaming or
unlinking. Output reports sizes
observed before trimming and bytes observed in files cleared; concurrent writes
make these snapshots approximate. No archives are retained.

The registry rejects malformed records, symlinks and nonregular files.
Reads are capped at 4 MiB, 4001 lines and a 64 KiB scanner token. Repair corrupt
state offline after reconciling its reservations.
[Registry format and failure rules](SPEC.md#6-port-map-file)

## Legacy migration

Production generations are owned and reaped through guardians. Stop old manager
binaries and their gopls processes before migration. The new binary does not
adopt or automatically signal legacy processes. Without a live owner, list
refuses a nonempty registry and directs you to explicit offline cleanup; status
remains read-only. Do not remove reservations until their processes and guardian
state have been reconciled. See [SPEC.md](SPEC.md#persistent-owner-contract) for recovery rules.

## Development

- `internal/config` defines limits and validates environment overrides.
- `internal/transport` handles stdio/SSE framing and buffer accounting.
- `internal/protocol` adapts MCP capabilities, paths and results while
  preserving opaque fields.

The root command owns frontend routing and the shared process lifecycle.

| File | Responsibility |
| --- | --- |
| `main.go` | CLI commands and startup |
| `supervisor*.go`, `guardian*.go`, `ownerlog.go` | Persistent authority, private frontends, process/group ownership and bounded logs |
| `bridge.go`, `http.go` | Router construction, upstream replies, and HTTP frontend |
| `restart.go`, `restart_wire.go` | Restart coordination, fork/exec handoff and persistent frontend relays |
| `routing.go` | Admission, routing decisions and bounded resolution |
| `paths.go` | Path/worktree resolution, canonicalization and memos |
| `lane.go`, `requests.go` | Per-worktree delivery, cancellation, and request/operation accounting |
| `registry.go` | Manager construction, port selection, bounded durable registry storage |
| `process.go` | Readiness and read-only process/descriptor diagnostics |
| `status.go` | Status snapshots and explicit log trimming |

Tests and benchmarks live beside their implementations. See [JSON processing](SPEC.md#json-processing)
for field naming, framing and SDK boundaries.

```sh
go test -race -coverprofile=/tmp/gopls-manager.cover . ./internal/config ./internal/transport ./internal/protocol
python3 scripts/check_coverage.py /tmp/gopls-manager.cover --minimum 85
python3 -m unittest discover -s scripts
golangci-lint run ./...
```

CI uses the latest patch release matching `go.mod` (Go 1.27), builds for
`linux/amd64` and `darwin/arm64`, and requires 85% statement coverage in every
package and overall. Tests use real HTTP/stdio/SSE transports with SDK upstreams.
The optional real-gopls suite starts and reaps children in disposable worktrees
with a private registry; CI runs it against gopls v0.23.0 and v0.24.0:

```sh
go test -race -tags=integration -run '^Test(EndToEndRealGopls|AuditRealGopls.*|GuardianReal.*)$' -timeout=3m .
GOMAXPROCS=4 go test -tags=integration -run '^$' -bench '^BenchmarkRealGoplsRoundTrip$' -benchmem .
```

### Verify correctness

Start with the changed behavior in [SPEC.md](SPEC.md) and choose the checks
that can observe it.

| Behavior | Evidence to run or extend |
| --- | --- |
| Worktree routing, registry records, JSON-RPC framing and adapters | Go fuzz targets exercise malformed input, round trips, SDK agreement, and routing properties |
| Admission, cancellation, abandonment, idle retirement and capacity | TLC models and TLAPS safety proofs cover the transition rules. `FuzzSupervisorLifecycleAccounting` compares real owner transitions with an independent event ledger |
| Payload lifetime and backpressure | The payload model checks conservation and capacity. `FuzzRetainedPayloadAccounting` compares queued credits and retained bytes across two routers. Bubble tests exercise blocked writes and transport release |
| Hot-restart handoff | The handoff model checks exit-proof fencing and admission order. Native tests exercise lifetime-pipe EOF, inherited descriptors, and frontend recovery |
| Timers, cancellation and worker shutdown | Use `testing/synctest` for in-memory fixtures, assert quiescence with `synctest.Wait`, and advance fake time only for deadline behavior. Use native race tests for sockets, filesystem workers, subprocesses, and OS guarantees |

Run every Go fuzz target with Bun 1.4.4. CI gives each target three seconds and
two workers. Use a longer per-target budget for verification of changed contracts.

```sh
bun --install=force run scripts/fuzz.ts
bun --install=force run scripts/fuzz.ts 60s
go test -run '^$' -fuzz '^FuzzRetainedPayloadAccounting$' -fuzztime=60s -parallel=2 .
```

On a fuzz failure, use Go's printed seed replay command. Preserve the minimized
input in the target's `testdata/fuzz` corpus so ordinary tests replay it. CI
retains the corpus when the fuzz job fails.

Run the [formal checks](formal/README.md) with Java 25 and Bun 1.4.4. CI checks
the models and proofs on Linux and macOS and requires the guard-removal
mutations to produce the expected counterexamples. Use the conformance ledger
to pair model changes with Go regressions. Formal results apply to the models.

```sh
bun --install=force run formal/check.ts all
```

### Performance and resource checks

For warm frontend overhead, `BenchmarkHTTPRoundTrip` and `BenchmarkStdioRoundTrip`
include real wire framing, routing, queues and SSE calls to a minimal SDK server;
they exclude setup and gopls indexing/tool execution. `BenchmarkCallRoundTrip`
isolates the router with fake connections. Profile one benchmark at a time:

```sh
GOMAXPROCS=4 go test -run '^$' -bench 'Benchmark(HTTP|Stdio)RoundTrip$' -benchmem -count=10 .
GOMAXPROCS=4 go test -run '^$' -bench '^BenchmarkHTTPRoundTrip$' -benchtime=5s -cpuprofile=/tmp/manager.cpu -memprofile=/tmp/manager.mem -o /tmp/manager.test .
go tool pprof -top /tmp/manager.test /tmp/manager.cpu
go tool pprof -top -alloc_space /tmp/manager.test /tmp/manager.mem
```

Compare repeated before/after samples with `benchstat`; keep profiles and test
binaries outside the repository. Allocation profiles include SDK fixture/client
costs; inspect stacks before attributing them to the manager.
The integration benchmark includes actual gopls execution latency; its child
process CPU and allocations require separate profiling and are outside the
manager's pprof profile.

`TestAuditSessionChurn` checks ten batches of 100 synthetic backend lifetimes,
including roots planning, cancellation and unresolved-operation cleanup. To save
quiescent heap/goroutine profiles outside the repository:

```sh
GOPLS_MANAGER_AUDIT_PROFILES=/tmp/gopls-audit-profiles go test -race -v -run '^TestAuditSessionChurn$' .
```

For worker, watch-plan and descriptor ceilings, see the [resource ledger](SPEC.md#resource-ledger).
