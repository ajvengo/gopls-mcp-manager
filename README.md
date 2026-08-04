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
| `gopls-mcp-manager list` | Print the live servers, dropping and reaping dead records |

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
- **A cancellation follows the call it names**, reaching the gopls that owes that
  request id rather than home.
- Everything else that is not a `tools/call` goes to home.

Resolution shells out to `git rev-parse --show-toplevel`, so answers are
memoized for the session — per path argument, backed by a per-directory memo, so
each directory costs one `git` fork however many files you name in it.

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
- **Each client message is bounded** at 30 s (the readiness budget plus 20 s),
  retry included, so a gopls that accepts the connection and then says nothing
  fails that call instead of wedging the worktree for the session. It does not
  bound the answer: a gopls that takes a call and then goes quiet *without*
  dying still leaves its caller waiting.
- The bridge stops when the stdio client goes away, or on `^C` / `SIGTERM`.

SPEC.md §3–§5 has the rest: which party may answer a call, what the private
handshake ids are for, and what the budget deliberately excludes.

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
process is signalled when it is. All the records are checked at once, because
the map is locked for the whole sweep and each probe waits out its own timeout:

1. `kill(pid, 0)` — is anything there at all?
2. A record younger than 15 s is kept unprobed. A gopls between fork and bind
   refuses every probe, and refusal is the one verdict that kills.
3. An HTTP probe of the recorded port. **Connection refused is the only
   conclusive verdict.** A timeout is not — that is also what a gopls indexing a
   large tree looks like — nor is an answer we did not want, nor any other dial
   failure, which says something about this process rather than the server.
4. `ps` must still show that pid running a gopls with *our* listen address, since
   the map survives reboots that recycle every pid in it.

The rule throughout is that being unsure keeps the server: only a conclusively
dead record is signalled. A record that is no longer ours is dropped either way
but never signalled, since otherwise nothing would reap it. SPEC.md §8 has the
verdict table and the reasoning behind each.

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
