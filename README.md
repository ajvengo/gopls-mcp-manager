# gopls-mcp-manager

One MCP server for your editor or agent, many gopls servers underneath — one per
git worktree.

## Why

A gopls instance only serves the worktree it was started in. Ask it about a file
outside that tree and it does not route the question anywhere; it answers
"no package metadata", which is indistinguishable from a genuinely broken
workspace. So a single gopls cannot serve an agent that works across linked
worktrees — which is exactly what worktree isolation produces.

`gopls-mcp-manager` sits between the MCP client and gopls. It speaks stdio MCP
to the client, and routes each tool call to the gopls that owns the file the
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
| `gopls-mcp-manager [bridge [path]]` | Run the stdio bridge. Default; `path` defaults to `.` and needs the `bridge` word before it |
| `gopls-mcp-manager ensure [path]` | Start (or find) the gopls for `path`'s worktree, print its port |
| `gopls-mcp-manager list` | Sweep records and list retained servers, including terminating ones |
| `gopls-mcp-manager status` | Read-only JSON with recorded server count, identity-checked RSS and termination state |

`path` is resolved to the root of the worktree containing it, with symlinks
removed. The worktree the bridge starts in is its **home**.

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
are memoized per path and directory. One resolver worker gives the entire routing
operation a 10 s budget, shared by all paths in a call. Late results cannot change
sticky state. A stuck filesystem syscall occupies at most that one worker;
subsequent routing can time out while it remains stuck. The reader preserves
wire order, so cancellation behind resolution still waits for that bounded step.

## What the bridge handles for you

- **One handshake.** Your client initializes once, with home. Every gopls opened
  later is initialized behind the client's back.
- **Each gopls is told its own worktree as its root**, which is what makes it
  watch that tree and notice files created after it started. Your client need
  not advertise the optional `roots` capability — the bridge adds it and answers
  `roots/list` itself. Any other server-initiated request is refused with
  `method not found`.
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
- The bridge stops when the stdio client goes away, or on `^C` / `SIGTERM`.
  It waits for owned failed-child cleanup before exiting: SIGTERM, up to 2 s,
  then SIGKILL and up to 2 s to confirm exit. A failed child's record is removed
  only after exit is confirmed, with up to 2 s to acquire the cleanup lock.

## Limits and measurements

| Environment variable | Default | Meaning |
| --- | --- | --- |
| GOPLS_MANAGER_MAX_OUTSTANDING | 1024 | Outstanding calls per session |
| GOPLS_MANAGER_MAX_OUTSTANDING_PER_LANE | 128 | Outstanding calls per worktree |
| GOPLS_MANAGER_EXECUTION_TIMEOUT | 0s | Optional timeout after delivery; 0 disables it |
| GOPLS_MANAGER_METRICS | unset | Set to 1 for JSON metrics on stderr |

Counts must be positive. Execution timeout accepts Go durations such as 2m.
A timeout releases local tracking and suppresses late results; upstream work may
continue. If the client output queue is full at expiry, the session fails rather
than accumulating blocked timer callbacks. Choose it to accommodate long diagnostics and vulnerability scans.

Metrics include registry lock wait/hold nanoseconds and per-session request
counts. MCP stdout stays protocol-only. The status command reports RSS in KiB
only when the process identity matches; missing values and active-client counts
are null rather than guessed. Status never signals a process or rewrites the map.

There is no automatic eviction. [LEASES.md](LEASES.md) describes cross-client
attachment and operation ownership, fencing, and the evidence required before
introducing a resource ceiling. SPEC.md contains the behavioral contract.

## State on disk

| Path | Contents |
| --- | --- |
| `~/.local/share/gopls-ports.map` | One JSON object per line: `{"Worktree":…,"Port":…,"PID":…,"StartedAt":…}` |
| `~/.local/share/gopls-ports.map.lock` | flock held around every read-modify-write |
| `~/.local/share/gopls-mcp-logs/<hash>.log` | stdout+stderr of the gopls for that worktree |

The map is written atomically (temp file, `fsync`, rename) with mode `0600`.
Ports are picked from 61100–65100, seeded by a hash of the worktree path so the
same tree tends to get the same port, then probed forward until one is free.

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
process is signalled when it is. All records in an atomic snapshot are checked concurrently outside the global
registry lock. Reconciliation applies verdicts only to unchanged record identities:

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

```sh
go test -race ./...
golangci-lint run ./...
```

CI runs tests, lint, and builds for `linux/amd64` and `darwin/arm64` on the
latest 1.26 patch release.

The bridge tests run under `testing/synctest`, whose clock only moves once every
goroutine in the bubble has blocked, which makes timeouts free and exact: a test
waits out the shipped 30 s budget instantly, and a write told to lose a race
loses it on every run. A test stays outside a bubble when it does something that
clock cannot account for — a child process, a real listener, or a real timeout.
