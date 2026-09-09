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
Streamable HTTP or stdio MCP to the client, and routes each tool call to the gopls that owns the file the
call names, starting that gopls on demand. The client sees one server and
performs one handshake.

## Install

```sh
go install github.com/ajvengo/gopls-mcp-manager@latest
```

`gopls` must be on `PATH` to start a server. `list` does not need it, so a
stranded server can still be found and reaped after gopls has moved.

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
| `gopls-mcp-manager list` | Sweep records and list retained servers, including terminating ones |
| `gopls-mcp-manager status` | Read-only JSON with recorded server count, identity-checked RSS, termination state and log sizes |
| `gopls-mcp-manager trim-logs` | Discard contents of managed log files above the configured threshold, preserving append descriptors |

`path` is resolved to the root of the worktree containing it, with symlinks
removed. The worktree the bridge starts in is its **home**.

### Stateless HTTP

Start the manager as a long-running local service:

```sh
gopls-mcp-manager http 127.0.0.1:6099 /absolute/path/to/repo
```

Configure your MCP client's Streamable HTTP server URL as
`http://127.0.0.1:6099/mcp`. The endpoint accepts MCP POST requests and returns
JSON responses. It issues no session ID and ignores supplied session IDs;
GET and DELETE return 405. The listener is restricted to loopback.

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

Cancel the HTTP request to cancel its internal call. A separate
`notifications/cancelled` POST cannot identify another client's call in stateless
mode and has no cross-request effect. Cancellation still completes locally first;
an advisory SSE cancellation does not prove gopls stopped its work.

Existing outstanding-call, lane and memo limits apply across the HTTP service.
HTTP cache misses wait on a separate bounded 64-message resolution queue.
Pathless and fully cached HTTP calls proceed while a filesystem lookup is stuck;
lane creation still has one owner and filesystem work still has one worker.
Concurrent HTTP exchanges are capped by both the session outstanding limit and
the body budget divided by the maximum message size (16 exchanges by default).
Each exchange reserves a maximum-size body allowance until its response finishes,
including chunked and slow requests; excess exchanges receive HTTP 503.
Bodies are limited to 4 MiB by default, header/body reads
to 5/30 seconds, and response writes to 30 seconds after the response starts.
Long tool execution uses the existing optional execution timeout. Shutdown closes
internal SSE connections and cancels calls, while shared gopls processes remain
available to other managers. The default command still runs stdio.

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
  answer is suppressed. Local completion does not prove upstream work stopped.
- Everything else that is not a `tools/call` goes to home.

Resolution shells out to `git rev-parse --show-toplevel`, so successful answers
are memoized per path and directory. One resolver worker gives filesystem
resolution a 10 s budget, shared by all paths in a call. Pathless calls and path
memo hits bypass that worker. A separate reader admits calls into a bounded
64-message routing queue and handles cancellation immediately, even while a
lookup is stuck. Ordinary routing and sticky updates remain in wire order.
Ingress queueing, routing and delivery share a 40 s ceiling; delivery additionally
has its own 30 s ceiling. A stuck filesystem syscall occupies at most one worker.

## What the bridge handles for you

- **One handshake.** Your client initializes once, with home. Every gopls opened
  later is initialized behind the client's back.
- **Each gopls is told its own worktree as its root**, which is what makes it
  watch that tree and notice files created after it started. Your client need
  not advertise the optional `roots` capability — the bridge adds it and answers
  `roots/list` itself. Any other server-initiated request is refused with
  `method not found`. Replies have a 30 s write ceiling (or the shorter remaining
  handshake deadline); a failed reply closes that connection and fails its calls.
- **A dying gopls does not end the session**, not even home. The bridge redials,
  replays the handshake and retries once; calls that were in flight when it died
  come back as errors rather than hanging, since MCP clients have no timeout.
- **Each worktree gets its own lane**, so starting a cold worktree — a lock wait,
  a `gopls` spawn, a handshake — costs that worktree's calls and nobody else's.
- **Each admitted message has a 30 s delivery budget**, including queue time,
  lock acquisition, readiness, handshake and retry. An expired queued call
  cannot start a server. Filesystem syscalls cannot be interrupted by a Go
  context, and failed-child cleanup has a separate short budget. The budget
  is separate from the optional post-delivery execution timeout described below.
- **A full worktree queue rejects requests** with an overload error instead of
  blocking other worktrees. Each delivery and cancellation queue holds 64
  messages. Notifications are advisory and are dropped when their queue is full.
- **Outstanding calls have separate limits:** 128 per worktree and 1024 per
  session by default. A server accepting writes but never answering cannot evade
  those limits by draining its delivery queue.
- **Delivered operations retain a separate credit until an upstream response.**
  Local cancellation, execution timeout and connection failure release client
  tracking but retain that credit. Both sets use the outstanding limits above;
  uncertain retries consume another operation credit. A cancellation-ignoring
  server therefore cannot accept unlimited replacements through one router.
- The bridge stops when the stdio client goes away, or on `^C` / `SIGTERM`.
  It waits for owned failed-child cleanup before exiting: SIGTERM, up to 2 s,
  then SIGKILL and up to 2 s to confirm exit. A failed child's record is removed
  only after exit is confirmed, with up to 2 s to acquire the cleanup lock.

## Limits and measurements

| Environment variable | Default | Meaning |
| --- | --- | --- |
| GOPLS_MANAGER_MAX_OUTSTANDING | 1024 | Outstanding calls per session |
| GOPLS_MANAGER_MAX_OUTSTANDING_PER_LANE | 128 | Outstanding calls per worktree |
| GOPLS_MANAGER_MAX_LANES | 64 | Retained worktree lanes per session; existing lanes remain usable at capacity |
| GOPLS_MANAGER_MAX_SERVERS | 64 | Recorded shared servers allowed before a new spawn; existing servers remain reusable |
| GOPLS_MANAGER_MAX_CACHE_ENTRIES | 4096 | Entries in each path/directory memo; a full memo clears before inserting |
| GOPLS_MANAGER_CACHE_TTL | 5m | Positive duration; both memo levels expire together, lazily on access or metrics snapshots |
| GOPLS_MANAGER_MAX_MESSAGE_BYTES | 4194304 | Maximum stdio JSON value (including preceding whitespace), HTTP body, upstream POST and complete SSE event |
| GOPLS_MANAGER_HTTP_BODY_BUDGET | 67108864 | Maximum reserved HTTP request-body bytes; must hold at least one maximum-size message |
| GOPLS_MANAGER_SSE_BUFFER_BUDGET | 67108864 | Shared live SSE frame capacity, including replacement buffers during growth; at least one maximum-size message |
| GOPLS_MANAGER_LOG_TRIM_BYTES | 67108864 | `trim-logs` clears files larger than this; no automatic trimming |
| GOPLS_MANAGER_EXECUTION_TIMEOUT | 0s | Optional timeout after delivery; 0 disables it |
| GOPLS_MANAGER_METRICS | unset | Set to 1 for JSON metrics on stderr |

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
continue. If the client output queue is full at expiry, the session fails rather
than accumulating blocked timer callbacks. Choose it to accommodate long diagnostics and vulnerability scans.

Late terminal results release abandoned-operation credits. A lost connection
cannot supply such proof, so its credits remain charged until this router shuts
down. Same-ID reuse on that connection is rejected while the old operation is
unresolved. Restarting a manager resets its accounting; it does **not** prove old
work stopped. These limits do not coordinate operations across multiple managers.
An exhausted router may need operator recovery after establishing upstream state.

Metrics include registry lock wait/hold and manager probe, readiness and ensure
durations. Session snapshots every 30 s and at exit report request counts,
rejection reasons, cancellation/expiry counts, retained lanes and memo sizes,
path-memo hits/misses, capacity rollovers and expiry epochs, unresolved/abandoned
operation counts and current/peak SSE frame capacity,
and count/total/max nanoseconds for routing, queue waits and handshake. MCP stdout stays protocol-only. The status command reports RSS in KiB
only when the process identity matches; one bounded ps snapshot covers the
recorded PIDs. Missing values and active-client counts are null rather than
guessed. Status never signals a process or rewrites the map.

New shared spawns are refused at the configured recorded-server ceiling under
the registry lock. Terminating and stale unrelated records consume capacity;
run `list` to reconcile dead records. All managers sharing the map must run this
version with the same ceiling. Older binaries, unrecorded servers and differing
settings can bypass the policy. The ceiling bounds cooperating recorded spawns,
not RSS. Existing records remain reusable even above a lowered ceiling.

There is no automatic lane or process eviction. [LEASES.md](LEASES.md) describes cross-client
attachment and operation ownership, fencing, and the evidence required before
introducing automatic reclamation. SPEC.md contains the behavioral contract.

## State on disk

| Path | Contents |
| --- | --- |
| `~/.local/share/gopls-ports.map` | One JSON object per line: `{"Worktree":…,"Port":…,"PID":…,"StartedAt":…}` |
| `~/.local/share/gopls-ports.map.lock` | flock held around every read-modify-write |
| `~/.local/share/gopls-mcp-logs/<hash>.log` | stdout+stderr of the gopls for that worktree |

The map is written atomically (temp file, `fsync`, rename) with mode `0600`.
Ports are picked from 61100–65100, seeded by a hash of the worktree path so the
same tree tends to get the same port, then probed forward until one is free.

`status` reports per-record log sizes and aggregate bytes, largest file, file
count and oversized count, including logs left after a record disappears.
`trim-logs` clears the **entire contents** of managed regular `.log` files above
64 MiB by default; save wanted diagnostics first. It scans in bounded batches,
skips unrelated names and symlinks, and truncates in place without renaming or
unlinking. Shared children keep their inherited `O_APPEND` descriptors and
continue logging after the manager exits or a trim occurs. Output reports sizes
observed before trimming and bytes observed in files cleared; concurrent writes
make these snapshots approximate. There is no background log trimming or hard
disk ceiling: arrange an operator-controlled trim cadence if needed. No archives
are retained, and files below the threshold are left alone.

The file is meant to be readable and repairable by hand. A line that cannot be
parsed, or whose fields are out of range, is skipped rather than fatal — every
command reads this file, so one bad line must not lock you out of the tool. The
next write drops it. The exception is a line over 64 KiB, which fails the read
naming the file: skipping it would hide records the next write then drops
without killing their processes, stranding the indexes below.

## Liveness and killing

A record is the only handle anyone has on its gopls, so dropping one without
killing the process strands a full workspace index — routinely 1–2 GB resident —
that no longer appears in `list`, still holds its port, and lives until reboot.

A record is therefore checked on several counts before it is dropped, and the
process is signalled when it is. Acquisition checks only records for the requested
worktree; unrelated records continue reserving their ports. Startup performs a
full sweep before admission, and running bridge/HTTP managers repeat it every
30 seconds so deleted temporary worktrees do not strand indexes. Run `list` for
an immediate full maintenance sweep. Existing worktrees with healthy servers
remain shared; this is not idle eviction. Probes run outside
the global registry lock. Reconciliation applies verdicts only to unchanged
record identities:

1. `kill(pid, 0)` — is anything there at all?
2. A record younger than 15 s is kept unprobed. A gopls between fork and bind
   refuses every probe, and refusal is the one verdict that kills.
3. An HTTP probe of the recorded port. **Connection refused is the only
   conclusive verdict.** A timeout is not — that is also what a gopls indexing a
   large tree looks like — nor is an answer we did not want, nor any other dial
   failure, which says something about this process rather than the server.
4. `ps` must still show that pid running a gopls with *our* listen address, since
   the map survives reboots that recycle every pid in it.

Probes report live, uncertain, terminate or gone without signalling. Termination
is persisted as `Terminating: true` before an action lock and fresh identity check
permit SIGTERM. A successful signal does not remove the record: later sweeps must
confirm that the old process is gone. Ensure refuses terminating endpoints and
does not spawn beside them. A process ignoring SIGTERM stays discoverable.

Update all manager binaries sharing the registry before relying on this field;
older versions ignore it. Shared-process sweep cleanup does not escalate to
SIGKILL. The manager's own failed children retain their bounded escalation and
reaping behavior. SPEC.md §8 describes reconciliation and identity safeguards.

## Development

The repository is one Go module with two internal packages:

- `internal/config` owns resource limits, defaults and environment validation.
  Defaults are plain values; importing the package does not read the environment.
- `internal/transport` owns stdio/SSE connections, JSON-RPC framing and buffer
  accounting. Constructors return SDK connections; frame-budget observations
  use a synchronized snapshot. Codec and connection implementation types stay private.

The root command owns routing, request generations, lanes and shared-process
lifecycle. Both frontends reuse the transport package and the same configuration.
Transport depends on config for default sizes; neither package imports the command.
Package-specific tests and transport benchmarks live beside their implementations;
integration and shared-process tests remain at the root.

```sh
go test -race . ./internal/config ./internal/transport
golangci-lint run ./...
```

CI runs tests, lint, and builds for `linux/amd64` and `darwin/arm64` on the
latest 1.26 patch release.

The bridge tests run under `testing/synctest`, whose clock only moves once every
goroutine in the bubble has blocked, which makes timeouts free and exact: a test
waits out the shipped 30 s budget instantly, and a write told to lose a race
loses it on every run. A test stays outside a bubble when it does something that
clock cannot account for — a child process, a real listener, or a real timeout.
