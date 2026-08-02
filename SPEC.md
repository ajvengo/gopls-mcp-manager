# gopls-mcp-manager — behavioural spec

Normative description of what the bridge and the manager guarantee. Written
from the code as it stands; each invariant names the test that pins it.

## 1. Roles

```
MCP client ──stdio──▶ gopls-mcp-manager ──SSE/HTTP──▶ gopls mcp -listen 127.0.0.1:P   (home)
                                        ──SSE/HTTP──▶ gopls mcp -listen 127.0.0.1:Q   (worktree B)
                                        ──SSE/HTTP──▶ …
```

- **home** — the worktree the bridge was started in. Always the first upstream.
- **upstream** — one connection to one gopls, identified by its worktree path.
- **sticky** — the worktree of the most recent tool call that named a resolvable
  absolute path.

## 2. Routing

For each message read from the client:

| Message | Destination |
| --- | --- |
| `tools/call` whose `file`, `dir` or `files[i]` resolves to a worktree | that worktree; it becomes sticky |
| `tools/call` with no such argument | sticky, else home |
| `notifications/cancelled` naming a request a connected upstream still owes | the worktree owing it |
| any other request or notification | home |
| a response | dropped |

R1. **Only absolute paths are evidence.** A relative path resolves against the
manager's own working directory, which is home, so it would override correct
sticky routing for every worktree but home.
→ `TestToolCallRoutesToTheWorktreeOwningItsPath` (case "relative path")

R2. **An unresolvable path is not evidence either** — the call keeps its current
server, which then reports its own error for the path. A path under a directory
that does not exist yet resolves to nothing, not to the repository above it.
→ same test, case "unresolvable path";
  `TestWorktreeOfDoesNotResolveThroughAMissingDirectory`

R3. **Sticky survives calls that carry no path.**
→ `TestTargetFallsBackToTheLastPathBearingCall`

R4. **A linked worktree is a distinct destination**, and a nested directory
resolves to the worktree containing it. Resolution goes through
`git rev-parse --show-toplevel`, then `EvalSymlinks`, so two paths naming the
same tree agree. The subprocess is bounded at 10 s, for the same reason H5
bounds the handshake: it runs on the goroutine that reads the client, so a `git`
wedged on an unresponsive filesystem would otherwise stall the session for good.
On expiry the path is simply unresolvable, which is R2.
→ `TestWorktreePathSeparatesLinkedWorktreesAndSharesNestedDirectories`

R5. Successful resolutions are memoized for the session, **keyed by the
directory** holding the path rather than by the path itself: every path in a
directory has the same worktree, so one entry — one `git` fork — covers every
file a session names under it. What gets resolved is still the path, not the
key: reducing it a second time would climb past the directory the key names, and
R2 would stop holding. Failures are not cached, so a path that becomes
resolvable later still gets its own lookup. Nothing invalidates a hit, so a
worktree removed mid-session — or a directory that becomes part of a different
worktree, by being replaced with a linked one at the same path — keeps answering
with what it resolved to first, until the routed gopls reports the path itself.
Keying by directory neither causes that nor widens it: a path-keyed memo went
stale the same way, and less evenly, since a file it had never seen would route
somewhere its neighbours did not.
→ `TestWorktreeOfMemoizesByDirectory`,
  `TestWorktreePathReducesToADirectoryExactlyOnce`

R6. **The client never sends a response the bridge needs.** `roots/list` is
answered locally and every other server-initiated request is refused, so a
response from the client answers nothing and is discarded.

R7. **A cancellation follows its request, not the default route.** Home has
never issued an id belonging to another worktree, so a `notifications/cancelled`
delivered there is dropped while the gopls actually running the call keeps
going. The destination is recorded when the request is (F2), so no separate
table is needed. An id no connected upstream owes — answered, cancelled twice,
unparseable, or owed by a connection already gone — routes home as before, where
it is equally harmless. Never to a worktree we hold no connection to: dialling
one would have `ensure` spawn a whole gopls, on the client's reader, to hand it
a cancellation for a call it never received.
→ `TestCancellationFollowsTheRequestToItsUpstream`

## 3. Handshake

H1. The client's `initialize` is forwarded to home under the client's own id,
and with its parameters intact but for the roots capability S3 adds. The client
sees exactly one `initialize` result.

H2. Every later upstream — a new worktree, or a redialled home — is initialized
by replaying the client's stored `initialize` under the private id
`gopls-mcp-manager-init-<n>`. That reply is swallowed; anything else the
upstream volunteers in that window is forwarded to the client.
→ `TestSendReconnectsAndInitializesRestartedHome`

H3. If the *initial* `initialize` fails to write, the retry re-sends the
client's own request rather than a private handshake.
→ `TestSendRetriesInitialInitializeWithoutPrivateHandshake`

H4. The handshake ends with a `notifications/initialized` notification.

H5. **The handshake is bounded** (30 s per client message, F1's retry included —
a per-attempt deadline would stall the client twice over). It runs on the
goroutine that reads the client, so an upstream that accepts the connection and
then answers nothing would stall every later client message for the rest of the
session, with neither side timing out — and §8 deliberately keeps such a server
alive, so `ensure` hands one back. A budget already spent cancels F1's retry
rather than restarting it: redialling would pay §10's unbounded `flock` wait to
reach a handshake that expires on its first write. The deadline covers the
handshake — including the roots S2 answers inside it — but not the dial: the
connection keeps the session context, because the SSE stream is read under the
context it was dialled with.
→ `TestHandshakeGivesUpOnAnUpstreamThatNeverAnswers`

## 4. Server-initiated requests

S1. **`roots/list` is answered by the bridge**, never forwarded, with exactly one
root: the worktree that upstream serves, as a `file://` URI with its base name.
→ `TestUpstreamRootsAnsweredWithItsOwnWorktree`

S2. S1 holds **inside the handshake window too**. gopls asks for its roots as
soon as it has seen the capability in `initialize`, which is before the steady
-state reader exists.
→ `TestRootsAskedDuringHandshakeIsAnsweredLocally`

S3. **The bridge adds the roots capability to every valid `initialize` request
that omits it.** Existing capabilities and roots settings are preserved. This
makes S1/S2 independent of optional client behaviour; malformed parameters
remain gopls' error to report.
→ `TestWithRootsCapability`, `TestRootlessInitializeIsForwardedWithWorkingRoots`

S4. **Every other server-initiated request is answered with
`CodeMethodNotFound`** on the connection it arrived on, and is not forwarded.
→ `TestUnroutableUpstreamRequestIsRefusedNotForwarded`

Rationale for S1/S2: gopls' MCP mode runs a full in-process LSP session with an
fsnotify watcher, but it watches only the roots the client reports. Forwarded,
every upstream would be told about the single tree the session opened in, and
every other worktree would answer "no package metadata" for files on disk.

## 5. Failure semantics

F1. **An upstream dying never ends the session**, not even home. Its cached
connection fails the next write, which redials, replays the handshake, and
retries once.
→ `TestHomeUpstreamEOFDoesNotCloseClient`

F2. **A call in flight when its upstream dies is failed explicitly** with an
internal error carrying its id. Nothing else will ever produce that id, and MCP
clients have no timeout.
→ `TestUpstreamDeathFailsItsInFlightCalls`

F3. **Pending calls are tracked per connection, not per worktree.** A reconnect
gives a worktree a new connection under the same name; keyed by name, the dead
connection's reader would fail calls the new one had already accepted. The
worktree is recorded beside the connection, never matched on: it is what R7
routes a cancellation by, and only this step knows it.
→ `TestStaleUpstreamDeathSparesTheReconnectedCall`

F4. **A call is recorded as pending before it is written.** A local gopls can
answer before `Write` returns; recorded afterwards, the entry would outlive the
answer and F2 would later emit a second, contradictory reply to the same id.
→ `TestCallAnsweredBeforeItsWriteReturnsLeavesNothingOwed`

F5. **Whoever takes an id off the pending list owns the reply.** F2, F4 and the
retry all race for the same call: a failed write and
that connection's reader seeing the death are one event seen twice. If the
reader wins it has already answered under F2, and a retry that then succeeded
would produce a second reply to an id the client has closed — which the client
discards, so the call would stay failed while the retry looked fine. The rule is
symmetric: a reader that loses the claim **drops** the answer it read, because
the id it names has already been answered by whoever won.
→ `TestSendLeavesACallItsDyingUpstreamAlreadyFailed`,
  `TestUpstreamAnswerToAnAlreadyFailedCallIsDropped`

F5a. **A cached connection always has a reader**, because `cache` is the only
thing that writes the map and it starts the reader in the same step. It is
called only once the connection has taken a successful write, so a dial that
hands back an already-dead upstream fails on the writing goroutine, where the
retry is, rather than racing a reader that could claim the id first and answer
the client an error the retry is then not allowed to replace. The first attempt
on a new connection is single-goroutine. The converse does not hold: the failure
path drops a connection from the map while its reader is still winding down.
→ `TestSendRetriesInitialInitializeWithoutPrivateHandshake` (the retry, which
  only a single-goroutine first attempt can reach)

F6. Only the stdio client going away stops the bridge. `io.EOF`,
`context.Canceled` and `ErrConnectionClosed` exit zero.

## 6. Port map file

Path `~/.local/share/gopls-ports.map`, mode `0600`, one JSON object per line:

```json
{"Worktree":"/Users/x/src/repo","Port":62001,"PID":4242}
```

M1. Written atomically: temp file in the same directory, `fsync`, `rename`.
Mode is set explicitly, because `CreateTemp`'s `0600` is still subject to umask
and an exotic umask could leave the file unreadable to its own writer.

M2. Every read-modify-write is wrapped in an exclusive `flock` on
`<map>.lock`, so concurrent `bridge`/`ensure`/`list` processes serialize.

M3. **A line that cannot be parsed is skipped, not fatal.** Every command reads
this file; one bad line must not lock the operator out of the tool. The next
write drops it.
→ `TestReadMapSkipsUnparseableLines`

One exception, deliberate: a line longer than `bufio.Scanner`'s 64 KiB limit
fails the read outright. Skipping it would hide records the next write then
drops without terminating their processes, stranding exactly the indexes L2
exists to protect. Failing names the file, which is repairable; the alternative
is silent.

M4. **Fields are validated, not merely decoded.** `Worktree` must be non-empty,
`PID` must be positive, `Port` must lie inside the allocation range. Each is an
argument to `kill(2)` or to a probe that decides on a `kill`: pid `0` signals
the manager's own process group, a negative pid signals every process the user
owns, and an out-of-range port can only refuse a probe and condemn a live
server.
→ same test

M5. Round-tripping preserves any path, including one containing tabs, quotes or
newlines — JSON escaping keeps a record on one line.
→ `TestMapRoundTripEscapesPaths`

## 7. Port allocation

Range 61100–65100. The first candidate is
`firstPort + sha256(worktree)[:8] mod count`, so a given worktree tends to
return to the same port. Candidates already in the map, or where `net.Listen`
fails, are skipped in order.
→ `TestBasePortIsStableAndInRange`, `TestAllocatePortProbesPastMappedAndOccupiedPorts`

## 8. Liveness

Records are swept concurrently (L6). Within one record the steps run in order:

1. `kill(pid, 0)` — `ESRCH` means gone; the record is dropped, nothing to kill.
2. HTTP probe of `127.0.0.1:<port>`, 500 ms, expecting `200` and
   `Content-Type: text/event-stream`.
   - answered → alive, done
   - refused → dead; nobody is listening
   - **timed out → inconclusive.** A gopls indexing a large tree under a GC
     pause looks identical to a corpse.
   - **answered something else → inconclusive.** A 503, or a path a later gopls
     moved, still came from a process holding that port. Retrying would only
     reach the same verdict a moment later, since a server answering wrongly
     answers wrongly again.
   - **anything else → inconclusive.** Only refusal is about the server; a
     dial that fails for descriptors, an unreachable host or a cancelled
     context is about this process, and reading death into it would condemn
     every record in the sweep at once.
3. Identity, by `ps`: does that pid still run a `gopls` whose command line
   contains our listen address?
   - inconclusive **and** ours → alive; the record is kept, nothing is signalled
     → `TestRecordAliveSparesAServerThatIsMerelyBusy`,
       `TestRecordAliveSparesAServerAnsweringSomethingElse`
   - inconclusive **and** not ours → the record is dropped, nothing is signalled
     → `TestRecordAliveDropsAnInconclusiveRecordThatIsNoLongerOurs`
   - dead **and** ours → `SIGTERM`, and the record is dropped
     → `TestRecordAliveKillsTheServerItDeclaresDead`
   - dead **and** not ours → the record is dropped, nothing is signalled
     → `TestRecordAliveSparesAProcessThatIsNotOurGopls`

L1. **Every record is offered to the liveness check**, including a second record
for a worktree that `ensure` will never answer with. The record is the only
handle on that process, and its port really is taken.
→ `TestCleanRecordsOffersEveryRecordAndDropsOnlyTheDead`

L2. **A dropped record's process is always signalled while it is still ours.**
Otherwise a 1–2 GB index survives unreferenced, holding its port, until reboot,
and the next `ensure` for that worktree starts a second one beside it. A record
whose pid is no longer ours has no index behind it to strand.

L3. Identity is checked by listen address, not by process name. The process most
likely to be called `gopls` after pid reuse is the one the user's editor
started.

L5. **No verdict is a one-way door.** Nothing revisits a record kept as alive,
so a record kept on an inconclusive probe alone would send every later `ensure`
to a port its gopls no longer holds, with no command able to reap it. Identity
is what bounds this: an inconclusive probe buys a record another sweep only for
as long as the pid is still ours.
→ `TestRecordAliveDropsAnInconclusiveRecordThatIsNoLongerOurs`

L4. `list` and `ensure` rewrite the map after sweeping, so a dead record
disappears from the file as a side effect of any command that reaches a port.
`ensure` returning early — no port allocatable, or `gopls` failing to start —
leaves the swept records in the file. Only the file: the sweep signalled them
under L2 before the failure, so nothing is stranded, and the next command
re-probes corpses that answer nothing and drops them again. A `gopls` missing
from `PATH` therefore keeps the same dead lines in the map until a command
succeeds.
→ `TestListShowsLiveRecordsAndCleansDeadRecords`

L6. **Records are swept concurrently**, so the liveness check has to be safe to
call from several goroutines at once. The sweep runs inside M2's exclusive
`flock` and each probe waits out its own 500 ms, so in sequence the lock would be
held for the sum of the timeouts rather than the longest one — paid by every
other `bridge`, `ensure` or `list` process, and so by another worktree's next
tool call.
→ `TestCleanRecordsProbesEveryRecordAtOnce`

## 9. Spawning

Started as `gopls mcp -listen 127.0.0.1:<port>` with `cwd` set to the worktree
and `setsid`, stdout and stderr appended to
`~/.local/share/gopls-mcp-logs/<sha256(worktree)[:8]>.log`.

P1. The child is **reaped**, not released. `setsid` does not reparent, so an
unwaited gopls would stay a zombie child for the whole session — and
`kill(pid, 0)` succeeds against a zombie, which would make step 1 of §8 report a
dead server as alive.

P2. Readiness is polled for 10 s at 100 ms. On timeout the child is signalled
through its `*os.Process`, which knows whether the reaper already collected it;
a bare `kill(pid)` could land on a recycled pid.

P3. `startGopls` returns the `*os.Process`, so `ensure`'s write-failure path
signals the handle it holds instead of re-deriving identity through `ps` — a
check that could only refuse and leak the process it just started.

## 10. Known limits

- The flock is held across `startGopls`' 10 s readiness wait, and `ensure` is
  reached synchronously from the client's request path. A cold start therefore
  blocks other `gopls-mcp-manager` processes.
- A wedged server — accepting connections, answering nothing or answering
  something that is not our endpoint — is our own gopls under §8 step 3, so it
  is kept for as long as it runs, at one probe and one `ps` per sweep. H5 bounds
  the handshake such a server never answers, but not the `ensure` that precedes
  it: `flock` has no context form, so that wait stays unbounded.
- **Nothing bounds a call once it has been written.** H5 covers the handshake
  only; past it, the answer is whatever `readFromUpstream` reads, under the
  session context alone. An upstream that takes a `tools/call` and then neither
  answers nor dies leaves the client waiting forever — F2 fires on a connection
  dying, not on one going quiet, and the §8 sweep asks a fresh probe whether the
  server is up, not whether that call is progressing. Cancelling it (R7) reaches
  the right gopls but does not release the caller either.
- A cancellation for a call whose upstream is already gone routes home and does
  nothing, rather than dialling the worktree back (R7). The call itself is not
  left hanging: the dead connection's reader fails it under F2.
- Duplicate records for one worktree are preserved rather than reconciled;
  `ensure` answers with the first.
