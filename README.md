# gopls-mcp-manager

One MCP server for your editor or agent, many gopls servers underneath — one per
git worktree.

## Why

A gopls instance only serves the worktree it was started in. Ask it about a file
outside that tree and it does not route the question anywhere; it answers
"no package metadata", which is indistinguishable from a genuinely broken
workspace. So a single gopls cannot serve an agent that works across linked
worktrees — which is exactly what worktree isolation produces.

`gopls-mcp-manager` sits between the MCP client and gopls. It speaks stateless
Streamable HTTP or stdio MCP to the client, and routes each tool call to the
gopls that owns the named file, starting it on demand. The client sees one server
and performs one handshake.

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

Register it where you would have registered `gopls mcp`:

```json
{
  "mcpServers": {
    "gopls": { "command": "gopls-mcp-manager" }
  }
}
```

Commands:

| Command | Effect |
| --- | --- |
| `gopls-mcp-manager http [address [path]]` | Serve stateless MCP at `/mcp`; defaults to `127.0.0.1:6099` and home `.` |
| `gopls-mcp-manager [bridge [path]]` | Run the stdio bridge. Default; `path` defaults to `.` and needs the `bridge` word before it |
| `gopls-mcp-manager ensure [path]` | Start (or find) the gopls for `path`'s worktree, print its port |
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

Bridge, HTTP and `ensure` share one persistent owner and one gopls generation per
worktree. It survives zero clients and checks retirement every 30 seconds, using
`GOPLS_MANAGER_IDLE_TIMEOUT` (default `1h`) for idle and abandoned-work grace.
Quiet connections do not pin gopls; a fresh call reconnects before delivery.
Cancellation, timeout, disconnect and lost acknowledgements retain remote credits
until response or confirmed exit. Ordinary retirement sends SIGTERM and waits ten
seconds; ignored TERM, uncertain guardians and storage failures remain counted.
See [SPEC](SPEC.md#persistent-owner-contract) for recovery and failed-start rules,
the [limits table](#limits-and-measurements) for configuration and the
[resource ledger](SPEC.md#resource-ledger) for remaining OS/memory limits.

Stop legacy managers and gopls before migration; adoption is refused. Frontends
must match owner limits/protocol. The private versioned socket lives under
`/tmp/gopls-mcp-owner-<uid>-<registry-hash>`, independent of TMPDIR/HOME length;
its singleton lock is beside the registry. Budgets do not cap total RSS or
universally contain escaped project descendants.

Normal MCP initialization validates each upstream connection. The guardian sets
`GO_TELEMETRY_CHILD=2`, an internal Go telemetry convention that suppresses
sidecars in the tested gopls releases. It does not prove containment of future
versions. The selected Go toolchain must report the exact private
`TEST_TELEMETRY_DIR` and telemetry `off` before startup and each request; a
mismatch fails. Global telemetry, HOME and GOTOOLCHAIN are preserved. See
[SPEC P5](SPEC.md#9-spawning) for the compatibility contract.

### Restart and binary reload

Use these commands to replace gopls indexes or upgrade the shared manager while
keeping editors and agents connected:

| Command | What changes | When to use it |
| --- | --- | --- |
| `restart` | All retained gopls generations; the owner stays running | After updating gopls or when a fresh index is needed |
| `restart --hot` | The shared owner binary and all retained gopls generations | Install an updated manager without reconnecting clients |
| `reload` | Same fork/exec handoff as `restart --hot` | Short form for a manager binary upgrade |

To restart gopls, run this as the same OS user with the same manager configuration
as the running owner:

```sh
gopls-mcp-manager restart --timeout 5m
```

To build and activate an updated manager from its checkout:

```sh
mkdir -p "$HOME/.local/bin"
go build -o "$HOME/.local/bin/gopls-mcp-manager-next" .
gopls-mcp-manager reload --binary "$HOME/.local/bin/gopls-mcp-manager-next" --timeout 5m
gopls-mcp-manager status
```

`restart --hot` accepts the same `--binary` and `--timeout` options as `reload`.
Without `--binary`, either hot command selects the **invoking** executable:
`"$HOME/.local/bin/gopls-mcp-manager-next" reload` activates that binary.
Plain `restart` does not accept `--binary`. Keep the replacement at its selected
path for later child launches, and update client/service launch commands to use
it for future starts. A successful reload waits for the successor's restart to
finish and prints a JSON receipt. These commands require a running owner.

During either restart, admitted calls drain while cancellation and new client
attachments remain live. Once quiet, the owner closes admission and pauses
frontend traffic at message boundaries. Work racing that pause refuses the
restart; retry when clients are quiet. Tool requests already sent upstream are
never replayed. New calls may wait through the pause under their original
deadlines; a long pause can still cause client timeouts. `--timeout` bounds the
command's wait (default `5m`, positive and at most `1h`).

Hot restart checks the replacement's private protocol and complete resource
configuration before pausing clients. It forks/execs the new owner and transfers
the Unix listener, singleton lock, frontend sockets and bounded session
checkpoint. The successor waits for confirmed old-owner exit before resuming
traffic. Both owners' FD footprints must fit the budget during handoff.
After worktree startup attempts finish, the successor unblocks HTTP proxy
traffic and attempts to resume each paused frontend. It reopens general
admission after those attempts settle.

Client-facing stdio and HTTP frontend processes keep running their existing
binaries. They retain partial input and public connections; the successor restores
home, initialization and sticky routing state. gopls indexes are rebuilt. Restart
those frontends normally when they also need a binary update.

The replacement must support the same private protocol, limits and directory
filters; configuration changes require a normal service restart. Unconfirmed
abandoned work, uncertain exit, durable cleanup failure or insufficient FD
capacity refuses restart. After handoff commits, the old executable is not
relaunched on failure; generation startup errors appear in `status`. Process,
filesystem or binary failure can still interrupt service. See
[SPEC O8](SPEC.md#persistent-owner-contract) for the full contract.

**Upgrading a pre-handoff binary:** stop the old owner and reconnect clients once.
Subsequent compatible upgrades can use `reload`. Guardian recovery and
[legacy reconciliation](#legacy-migration) still apply.

### Stateless HTTP

Start the manager as a long-running local service:

```sh
gopls-mcp-manager http 127.0.0.1:6099 /absolute/path/to/repo
```

Configure your MCP client's Streamable HTTP server URL as
`http://127.0.0.1:6099/mcp`. The endpoint accepts MCP POST requests and returns
JSON responses. It issues no session ID and ignores supplied session IDs;
GET and DELETE return 405. The listener is restricted to loopback. Public HTTP
requests require a numeric loopback or `localhost` Host; browser cross-origin
POSTs are rejected before proxy rewriting or backend admission. Local MCP clients
without browser headers remain supported. These stateless
guarantees assume the SDK compatibility override
`MCPGODEBUG=allowsessionsinstateless=1` is not enabled; that trusted environment
setting restores the SDK's legacy session-header and DELETE behavior.

HTTP requests route independently: absolute `file`, `dir`, and `files`
arguments select a worktree; **pathless calls always use home**. There is no
sticky routing between HTTP requests or clients. Run a separate manager with
a different home and listening port when pathless tools must target another
worktree. The routing description below describes the stdio bridge's sticky mode.

Internally, the manager keeps bounded, reusable legacy SSE-over-HTTP connections
to gopls, including private initialization and worktree roots. Tool listing,
schemas, pagination, results and upstream errors are forwarded. Client request
IDs are translated to unique internal IDs, so concurrent clients cannot collide.
The frontend advertises tools; it does not relay unsolicited notifications,
progress streams, or client roots to the shared upstream sessions.

Cancel the HTTP request to cancel its internal call by its assigned ID. A separate
`notifications/cancelled` POST cannot identify another client's call in stateless
mode and has no cross-request effect. Cancellation still completes locally first;
an advisory SSE cancellation does not prove gopls stopped its work.

Existing outstanding-call, lane and memo limits apply across the HTTP service.
HTTP cache misses wait on a separate bounded 64-message resolution queue.
Pathless and fully cached calls can reach their lanes while a cold lookup is
pending. Delivery still waits for any required work on the shared filesystem
worker, including validation of the selected Go toolchain. Backend teardown
discards pending cold calls and waits for their payload reservations to be released.
Concurrent HTTP exchanges are capped by both the session outstanding limit and
the body budget divided by the maximum message size (16 exchanges by default).
Each exchange reserves a maximum-size body allowance until its response finishes,
including chunked and slow requests; excess exchanges receive HTTP 503.
Bodies are limited to 4 MiB by default, header/body reads
to 5/30 seconds, and response writes to 30 seconds after the response starts.
Long tool execution uses the optional execution timeout, with no separate proxy
response-header deadline before a JSON tool result is ready. Ping, startup,
attachment and management queries retain explicit bounded control budgets.
A failed cached HTTP backend returns an explicit error and retires independently
of idle age or repeated traffic. It remains charged until active handlers and
teardown finish; a later independent request can initialize a fresh backend.
Stopping an HTTP frontend
closes its attachment; the persistent owner retains shared gopls until completion
or retirement. The default command still runs stdio.

## Routing

Every `tools/call` is inspected for a `file`, `dir`, or `files` argument:

- **Every path argument is resolved, and they must agree.** If they all name one
  worktree, its gopls answers the call and becomes *sticky*. If they name two or
  more, the call is refused rather than routed — split it. No gopls knows a tree
  it was not started for, so an answer from either would look complete while
  covering only half the files.
- **Relative paths are ignored**, since they would resolve against the manager's
  own working directory rather than yours. gopls' schemas ask for absolute paths.
- **Calls with no usable path** (`go_workspace`, `go_search`, `go_package_api`)
  follow the sticky worktree, falling back to home.
- **A cancellation completes the call locally** and releases its outstanding
  slot. Queued or dialling delivery is cancelled. Delivered calls also use a
  separate bounded control queue to notify the exact owning connection; a late
  answer is suppressed. The advisory keeps the original notification parameters
  and carries the parsed ID to the lane, where the write gate checks for reuse.
  Local completion does not prove upstream work stopped.
- Everything else that is not a `tools/call` goes to home.

Resolution shells out to `git rev-parse --show-toplevel`, so successful answers
are memoized per path and directory. Cache misses submit directly to the shared
owner's filesystem worker. All paths in a call share a 10 s lookup budget,
including the wait for that worker, capped by the remaining ingress budget.
Pathless calls and complete memo hits need no filesystem lookup to route. A
separate reader admits calls into a bounded 64-message routing queue and handles
cancellation immediately, even while a lookup is stuck. Ordinary routing and
sticky updates remain in wire order.
Ingress queueing, routing and delivery share a 40 s ceiling; delivery additionally
has its own 30 s ceiling.

## What the bridge handles for you

- **One handshake.** Your client initializes once, with home. Every gopls opened
  later is initialized behind the client's back.
- **Each gopls gets roots covering its own worktree after directory filtering.**
  This lets it notice files created after it started within those roots. The
  bridge adds the optional `roots` capability when missing and answers
  `roots/list` itself. Any other server-initiated request is refused with
  `method not found`. Replies have a 30 s write ceiling (or the shorter remaining
  handshake deadline); a failed reply closes that connection and fails its calls.
- **A dying gopls does not end the session**, not even home. A known retired
  connection is replaced before the next caller write. An actual write that
  fails ambiguously returns an explicit "may have executed" error and is never
  transparently replayed. In-flight calls fail rather than hang. Clients that
  independently retry can still execute a mutation again.
- **Each worktree gets its own lane**, so starting a cold worktree — a lock wait,
  a `gopls` spawn, a handshake — costs that worktree's calls and nobody else's.
- **After routing, each message has a 30 s delivery budget**, including lane queue time,
  lock acquisition, readiness and handshake. An expired queued call
  cannot start a server. Filesystem syscalls cannot be interrupted by a Go
  context, and failed-child cleanup has a separate short budget. The budget
  is separate from the optional post-delivery execution timeout described below.
  The total ingress, routing and delivery lifetime is at most 40 s.
- **A full worktree queue rejects requests** with an overload error instead of
  blocking other worktrees. Each delivery and cancellation queue holds 64
  messages. Notifications are advisory and are dropped when their queue is full.
- **Outstanding calls have separate limits:** 128 per worktree and 1024 per
  session by default. A server accepting writes but never answering cannot evade
  those limits by draining its delivery queue.
- **Delivered operations retain a separate credit until a definitive upstream
  response or confirmed exit of the generation.**
  Local cancellation, execution timeout and connection failure release client
  tracking but retain that credit. Both sets use the outstanding limits above;
  the owner also retains credits across frontend teardown. A cancellation-ignoring
  server cannot accept unlimited replacements through session churn.
- Client EOF, `^C` or `SIGTERM` closes the stdio frontend's attachment without
  waiting for outstanding responses. EOF exits successfully; signal cancellation
  exits with an error. The shared owner keeps remote credits and manages
  failed-start cleanup; other clients stay attached. See [SPEC F6](SPEC.md#5-failure-semantics).

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

Counts must be positive. Execution timeout accepts Go durations such as 2m.
A stdio value that exceeds the message limit ends the transport with an error;
HTTP oversized bodies receive 413. An oversized SSE event or exhausted SSE
budget closes that upstream connection and fails its pending calls. Oversized
upstream POSTs fail before delivery. SSE framing uses an unbuffered handoff;
there is no SDK 100-event queue. The SDK still owns JSON-RPC validation.
The HTTP and SSE budgets cover wire bodies and live frame capacity, respectively,
not total heap, decoded objects, queued results, HTTP overhead or child indexes.
Decoded messages use the existing bounded queues; even rejected frames can leave
unreachable allocations awaiting GC. Increasing message size also increases the
possible memory retained by those queues. Large diagnostics may require higher limits.
A timeout releases local tracking and suppresses late results; upstream work may
continue. A full 64-message client output queue fails the session, including
ordinary results and handshake notifications, so reconnects cannot accumulate
blocked readers after their operation credits have been released. Transient
bursts can also reach this limit. Choose the execution timeout to accommodate
long diagnostics and vulnerability scans.

Late terminal results release abandoned-operation credits. The persistent owner
keeps those credits across router shutdown until a response or confirmed process
exit. It enforces global and per-session/worktree limits in one ledger. Same-ID
reuse on a connection is rejected while its old operation is unresolved. Local
waiters, payload reservations and remote credits have separate lifetimes; owner
restart uses guardian manifests and leases to preserve uncertain reservations.

Metrics include registry lock wait/hold and manager probe, readiness and ensure
durations. Session snapshots every 30 s and at exit report request counts,
rejection reasons, cancellation/expiry counts, retained lanes and memo sizes,
path-memo hits/misses, capacity rollovers and expiry epochs, unresolved/abandoned
operation counts and current/peak SSE frame capacity,
and count/total/max nanoseconds for routing, queue waits and handshake. Periodic
and final session metrics use the same worker; final output gets up to 100 ms
to flush after request cleanup. Blocked stderr can retain that one worker,
while session shutdown returns. Other synchronous CLI/maintenance diagnostics
require writable stderr. MCP stdout stays protocol-only. The status command reports RSS in KiB
only when the process identity matches; one bounded ps snapshot covers the
recorded PIDs. Missing values and active-client counts are null. The owner
snapshot reports queued credits separately from upstream operations. Each
generation's operation count includes caller and server-control operations that
still hold credit. Status never signals a process or rewrites the map.

### Open-file budget

On macOS a gopls holds one descriptor per file and directory in its worktree:
`gopls mcp` hard-codes an fsnotify watcher over the roots it is given, and
fsnotify's kqueue backend opens every entry of every watched directory — `.md`,
`.py` and `.yaml` included. Headless `gopls mcp` takes no `directoryFilters`
setting, so the manager filters the roots it reports instead (below), and bounds
how many trees are watched at once.

### Directory filters

gopls is told to watch the worktree minus what its filters exclude: the `ignore`
directives of the worktree's `go.mod` (and of the modules its `go.work` uses),
then GOPLS_MANAGER_DIRECTORY_FILTERS, with gopls `directoryFilters` semantics.
Planning and freshness checks apply the same rule order and resource ceilings
to the current module metadata.
`.git`, and every other directory gopls' watcher skips (`.*`, `_*`, `testdata`),
is never watched. With nothing excluded the root is the worktree, as before.
Otherwise the roots are the directories covering the rest, so the entries
directly inside a directory with an excluded descendant (the worktree root among
them) are not watched. gopls still sees edits to Go files there, but a manager
restarts the server when such a directory gains or loses a Go-relevant file or
subdirectory, or its `go.mod`/`go.sum`/`go.work` changes, and the next call pays
a cold index. A gopls never unwatches a root: changed module rules require a new
generation, through `restart` or stale/idle retirement. Changing
`GOPLS_MANAGER_DIRECTORY_FILTERS` changes shared-owner configuration and requires
a normal service restart of the owner and frontends; `reload` preserves it.

A bare `-` excludes everything and later `+` rules re-include, so the allowlist
idiom `-,+cmd,+internal` works. The manager looks into an excluded directory
only where a `+` rule can reach, so a `+` rule with a `**` segment is the one
that costs a full walk of the tree.


The persistent owner checks stale watches and deleted worktrees every 30 seconds.
Admission and retirement share a lock: queued or active work protects its
generation, and a draining generation cannot accept a new write. A replacement
waits for confirmed exit. FD reservations and kernel quotas enforce descriptor
admission before spawn. The `lsof` observations are diagnostic. The persistent owner
uses completion time and retained work to decide retirement.
[SPEC.md](SPEC.md#persistent-owner-contract) describes current ownership.

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

The file is meant to be readable and repairable by hand. A line that cannot be
parsed, or whose fields are out of range, fails the complete read without
rewriting anything. Blank lines are harmless but consume input budget. Reads
are capped at 4 MiB, 4001 lines and scanner's 64 KiB line limit; writers reject
an encoded record above 65535 bytes. Symlinks/nonregular state are refused.
Errors name the file, never its contents. Repair corrupt state explicitly:
silently discarding uncertain reservations could orphan a live index.

## Legacy migration

Production generations are owned and reaped through guardians. Stop old manager
binaries and their gopls processes before migration. The new binary does not
adopt or automatically signal legacy processes. Without a live owner, list
refuses a nonempty registry and directs you to explicit offline cleanup; status
remains read-only. Do not remove reservations until their processes and guardian
state have been reconciled. See [SPEC.md](SPEC.md#persistent-owner-contract) for recovery rules.

## Development

The repository is one Go module with three internal packages:

- `internal/config` owns resource limits, defaults and environment validation.
  Defaults are plain values; importing the package does not read the environment.
- `internal/transport` owns stdio/SSE connections, JSON-RPC framing and buffer
  accounting. Constructors return SDK connections; frame-budget observations
  use a synchronized snapshot. Codec and connection implementation types stay private.
- `internal/protocol` owns pure MCP adaptations: roots capabilities, physical
  path arguments, and complete tool results. It preserves opaque fields and
  client key spellings, and owns no routing, filesystem or concurrency state.

The root command owns routing, request generations, lanes and shared-process
lifecycle. Both frontends reuse the transport package and the same configuration.
Internal packages do not import the command; protocol adaptations are independent
of transport and configuration. Router, lanes and operation accounting stay
together so their lock, generation and cancellation ownership remains private.
Package-specific tests and transport benchmarks live beside their implementations;
integration and shared-process tests remain at the root.

Root files group related responsibilities:

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

Transport decoding and buffer accounting stay in `internal/transport`. Shared
test fixtures live in `main_test.go` and `bridge_test.go`.

Project JSON uses strict `encoding/json/v2` and `encoding/json/jsontext`:
field names are case-sensitive, duplicate names and invalid UTF-8 are rejected,
and raw payloads use `jsontext.Value`. Use the documented lowercase MCP argument
names and capitalized registry fields. HTTP rejects invalid JSON before SDK
handling; stdio and SSE reject it at framing/decoding. The SDK remains responsible
for MCP negotiation, writes, and batch correlation.

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

Worktree lookup, roots planning, FD estimation, and managed toolchain checks
share one owner-wide filesystem worker. Roots planning and FD estimation submit
separate jobs under the reply deadline. An uncancellable call can occupy the
worker after its caller stops waiting. Planning fails explicitly on
oversized/nonregular metadata, I/O errors or exhausted aggregate budgets; symlinks
to regular metadata files remain supported. Defaults cap 100000 entries,
32 MiB paths/names, depth 128, 4096 roots, 16384 partial snapshots, 256 metadata
files/16 MiB aggregate, and 1024 filters/1 MiB aggregate. Individual filters are
limited to 4096 bytes. Invalid UTF-8 rules fail startup.
Upstream SSE uses direct HTTP loopback connections, same-origin endpoint events,
and no redirects. Readiness probes likewise bypass proxies and refuse redirects.
These limits do not bound total decoded heap or shared gopls RSS. Managed logs
have aggregate retention. Loopback listeners and existing private state directories are
trusted: the service does not authenticate a TCP listener to its recorded child.

In-memory bridge, HTTP, and transport tests use `testing/synctest` to advance
deadlines without wall-clock waiting. They use channel handshakes and
`synctest.Wait` to verify ordering and quiescence. Native socket, subprocess,
and real-time integration tests run with the system clock.
