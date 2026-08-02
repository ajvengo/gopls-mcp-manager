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

- **First argument that resolves to a worktree wins.** That worktree's gopls
  answers the call, and becomes *sticky*.
- **Relative paths are ignored.** They would resolve against the manager's own
  working directory — the home worktree — and so would misroute every call
  outside home. gopls' own schemas ask for absolute paths.
- **Calls with no usable path** (`go_workspace`, `go_search`, `go_package_api`)
  follow the sticky worktree, falling back to home.
- **A cancellation follows the call it names.** `notifications/cancelled` goes to
  the upstream that still owes that request id — home has never issued an id
  belonging to another worktree, so sent there it would be dropped while the
  gopls actually working kept going. An id nobody connected owes goes to home,
  where it is the no-op it would have been anyway.
- Everything else that is not a `tools/call` goes to home.

Path→worktree resolution shells out to `git rev-parse --show-toplevel` at about
13 ms a call, so successful answers are memoized for the session — keyed by the
directory holding the path, since every file in one has the same answer.

## What the bridge handles for you

- **One handshake.** The client's `initialize` reaches home directly. Every
  later upstream — including a restarted home — is initialized behind the
  client's back, under a private id whose reply is swallowed.
- **Root support is added to `initialize` when absent**, then **`roots/list` is
  answered locally**, in both the handshake window and the
  steady-state read loop, naming that upstream's own worktree. This is what
  makes each gopls watch its own tree and notice files created after it started.
  Forwarded to the client, every gopls would be told about the one tree the
  session opened in. Clients need not advertise the optional capability.
- **Any other server-initiated request is refused** with `method not found`.
  The bridge asks the client nothing, so it has no way to route an answer back;
  a definite error beats an upstream waiting forever.
- **A dying gopls does not end the session**, not even home. Its cached
  connection fails the next write, which redials, replays the handshake and
  retries once. Calls that were in flight when it died are failed explicitly —
  nothing else will ever produce their ids, and MCP clients have no timeout.
  Exactly one party may answer a call: whoever takes its id off the pending
  list owns the reply, so a write that loses that race to the dying
  connection's own reader is not retried behind the answer already sent.
- **The handshake is bounded** — 30 s per client message, retry included. It
  runs on the goroutine reading the client, so an upstream that accepts the
  connection and then answers nothing would otherwise stall every later
  message for the rest of the session, with neither side timing out. That
  upstream is not hypothetical: a probe timeout is deliberately treated as
  "alive" (see below), so such a server is handed straight back. Neither the
  dial nor the call itself is covered: `flock` has no context form, and a gopls
  that accepts a call and then goes quiet without dying leaves its caller
  waiting — the failure path fires on a connection dying, not on one falling
  silent.
- Only the stdio client going away stops the bridge.

## State on disk

| Path | Contents |
| --- | --- |
| `~/.local/share/gopls-ports.map` | One JSON object per line: `{"Worktree":…,"Port":…,"PID":…}` |
| `~/.local/share/gopls-ports.map.lock` | flock held around every read-modify-write |
| `~/.local/share/gopls-mcp-logs/<hash>.log` | stdout+stderr of the gopls for that worktree |

The map is written atomically (temp file, `fsync`, rename) with mode `0600`.
Ports are picked from 61100–65100, seeded by a hash of the worktree path so the
same tree tends to get the same port, then probed forward until one is free.

The file is meant to be readable and repairable by hand. A line that cannot be
parsed, or whose fields are out of range, is skipped rather than fatal — every
command reads this file, so one bad line must not lock you out of the tool. The
next write drops it.

One exception: a line longer than 64 KiB fails the read, naming the file.
Skipping it would hide records that the next write then drops without killing
their processes, stranding the very indexes the liveness rules protect.

## Liveness and killing

A record is the only handle anyone has on its gopls, so dropping one without
killing the process strands a full workspace index — routinely 1–2 GB resident —
that no longer appears in `list`, still holds its port, and lives until reboot.

A record is therefore checked twice before it is dropped, and the process is
signalled when it is. All the records are checked at once, because the map is
locked for the whole sweep and each probe waits out its own timeout:

1. `kill(pid, 0)` — is anything there at all?
2. An HTTP probe of the recorded port. **Connection refused is the only
   conclusive verdict: nobody is listening.** A timeout is not — that is also
   what a gopls indexing a large tree under a GC pause looks like — nor is an
   answer we did not want, which still came from something holding that port,
   nor any other way the dial can fail: descriptors gone or host unreachable
   say something about this process, not about the server. Every command sweeps
   every record, so a wrong verdict here would kill another worktree's healthy
   server — and one read out of a failure this process caused would kill every
   record in the sweep at once.
3. `ps` must still show that pid running a gopls with *our* listen address. The
   map survives reboots, after which every pid in it belongs to whoever the
   kernel handed it to next — and the process most likely to be called `gopls`
   is the one the user's editor started. Identity decides both open questions:
   whether a conclusively dead record's process may be signalled, and whether an
   unsure verdict is worth another sweep. A record that is no longer ours is
   dropped either way, unsignalled — otherwise nothing would ever reap it, and
   that worktree would keep being sent to a port it lost.

## Development

```sh
go test -race ./...
golangci-lint run ./...
```

CI runs tests, lint and a `linux/amd64` build on the latest 1.26 patch release.

The bridge tests run under `testing/synctest`, whose clock only moves once every
goroutine in the bubble has blocked. Timeouts there are therefore free and exact:
a test can wait out the shipped 30 s handshake budget in no time at all, and a
write told to lose a race loses it on every run rather than on almost every run.
A test stays outside a bubble when it does something that clock cannot account
for — a child process, a real listener, or a real timeout it means to measure.
