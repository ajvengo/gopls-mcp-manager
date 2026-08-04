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
| `notifications/cancelled` naming a request still in flight | the worktree it was routed to, which drops it if it has no upstream left |
| any other request or notification | home |
| anything but `initialize`, before the client's `initialize` | refused, or dropped if it is a notification (H1a) |
| a response | dropped |

R1. **Only absolute paths are evidence.** A relative path resolves against the
manager's own working directory, which is home, so it would override correct
sticky routing for every worktree but home.
→ `TestToolCallRoutesToTheWorktreeOwningItsPath` (case "relative path")

R2. **An unresolvable path is not evidence either** — the call keeps its current
server, which then reports its own error for the path. A path under a directory
that does not exist yet resolves to nothing, not to the repository above it.
→ same test, case "unresolvable path";
  `TestWorktreeOfResolvesAndMemoizes` (case "a missing directory is no evidence")

R2a. **A call whose paths land in two worktrees is refused**, with
`CodeInvalidParams` naming both, rather than answered by either. Every path
argument is resolved, not just the first: tools like `go_diagnostics` take a
`files` array, and one gopls only knows the tree it was started for, so routing
such a call by its first path answers about that worktree while saying nothing
at all about the files in the other — and what comes back is a well-formed
result for the subset it did cover, an omission the client has no way to see. A
refusal it can act on is worth more than an answer it cannot trust. Sticky is
left untouched, since the call picked no worktree; a notification, having no id,
is dropped as under H1a. One worktree named by several paths is one destination
and is not affected.
→ `TestToolCallRoutesToTheWorktreeOwningItsPath` (cases "one worktree named
  twice", "paths in two worktrees"), `TestToolCallSpanningTwoWorktreesIsRefused`

R3. **Sticky survives calls that carry no path.**
→ `TestTargetFallsBackToTheLastPathBearingCall`

R3a. **Each worktree has a lane**: a queue and the one goroutine that owns its
upstream. The goroutine reading the client picks a destination and hands the
message over; dialling, handshaking, retrying and replacing that upstream all
happen on the lane. A cold worktree therefore costs only its own calls, where it
used to block every worktree's for as long as an `flock` wait, a `gopls` spawn
and a whole handshake took. Order within a worktree is the queue's; between
worktrees there is none, and never was.

What stays on the reader is picking the destination, which R4's `git` fork is
part of: a first-touch path on a stalled filesystem still costs every worktree
up to that subprocess's 10 s. Choosing has to be serialized there — sticky (R3)
and the cancel-behind-its-call ordering (R7) both depend on it — so what the
lanes removed is the `flock` wait, the spawn and the handshake, not every way one
worktree can delay another. A lane's queue is 64 deep, which is the other.
→ `TestAColdWorktreeDoesNotHoldUpAnother`

R4. **A linked worktree is a distinct destination**, and a nested directory
resolves to the worktree containing it. Resolution goes through
`git rev-parse --show-toplevel`, then `EvalSymlinks`, so two paths naming the
same tree agree. The argument is resolved the same way *before* it is reduced
to a directory, because that reduction is lexical: a symlinked file is not a
directory, so its parent would be the link's own, and a link pointing into
another worktree would route to the tree holding the link rather than the tree
holding the code. A linked *directory* needs no help — `git -C` resolves its
cwd physically — but a linked file never reaches git, since only its parent is
passed. Resolution that fails leaves the argument untouched rather than
rejecting it: `EvalSymlinks` requires every component to exist, and R2 turns on
a path under an existing directory still resolving. The subprocess is bounded at
10 s, for the same reason H5 bounds the handshake: it runs on the goroutine that
reads the client, so a `git` wedged on an unresponsive filesystem would otherwise
stall the session for good. On expiry the path is simply unresolvable, which is
R2.

The 10 s sits on top of the **caller's own context** — the session's on the
routing path — rather than on a fresh background one, so a `git` parked on a
dead mount ends when the session does instead of outliving it by up to its own
timeout, with nothing left to serve by the time it returns. The timeout stays,
because the caller's context need carry no deadline of its own: on the CLI path
it is the interrupt context, which is what makes `^C` reach a `git` that
`bridge` and `ensure` both run before any session exists.
→ `TestWorktreePathResolvesEachPathToItsOwnWorktree`

R5. Successful resolutions are memoized for the session, **keyed by the
directory** holding the path rather than by the path itself: every path in a
directory has the same worktree, so one entry — one `git` fork — covers every
file a session names under it. The key is also what gets resolved: the reduction
to a directory happens once, at the top, and `worktreeOfDir` accepts nothing
else, so a second one cannot be expressed — it would climb past the directory
the key names, and R2 would stop holding. Failures are not cached, so a path
that becomes resolvable later still gets its own lookup. Nothing invalidates a
hit, so a worktree removed mid-session — or a directory that becomes part of a
different worktree, by being replaced with a linked one at the same path — keeps
answering with what it resolved to first, until the routed gopls reports the
path itself.
Keying by directory neither causes that nor widens it: a path-keyed memo went
stale the same way, and less evenly, since a file it had never seen would route
somewhere its neighbours did not.
→ `TestWorktreeOfResolvesAndMemoizes` (cases "one directory, three spellings, one
  fork", "two worktrees stay apart across a memo hit");
  `TestWorktreePathResolvesEachPathToItsOwnWorktree`

R6. **The client never sends a response the bridge needs.** `roots/list` is
answered locally and every other server-initiated request is refused, so a
response from the client answers nothing and is discarded.

R7. **A cancellation follows its request, not the default route.** Home has
never issued an id belonging to another worktree, so a `notifications/cancelled`
delivered there is dropped while the gopls actually running the call keeps
going. An id nobody owes — answered, cancelled twice, unparseable — routes home
as before, where it is equally harmless.

**The destination is bound when the call is routed**, not when its lane finally
writes it. Both messages pass through the reader in the client's own order, so
there the answer always exists; bound at the write instead, a cancellation
arriving while its call is still queued behind a cold start — the slowest
moment, and so the likeliest one for a client to give up — would find nothing
owed and go home, naming an id home never issued.

The route is therefore recorded on the same in-flight entry F3 keeps, alongside
the connection rather than in a table of its own — it is simply known strictly
earlier, when routing has picked a worktree and no lane has a connection to name
yet, so the entry carries a worktree and a nil connection until a write is
placed. One entry rather than two is what stops the route and the connection
disagreeing about whether a call is in flight: they are dropped in one delete,
and a retry that just ended its failed attempt re-records both in one write.
Between the two the call is nowhere, and a cancellation arriving there goes
home. No upstream holds the call in that window — but the retry places it a
moment later, so the cancellation is not merely late, it is lost, and the call
runs to completion. Accepted rather than fixed: the window is two statements
wide on a single goroutine, and holding cancellations for ids mid-retry would
need a second table to answer a question MCP treats as advisory anyway.
→ `TestCancellationFindsACallItsLaneHasNotSentYet`,
  `TestCancellationFollowsACallOntoItsRetryConnection`

**A cancellation never opens a connection.** Dialling one would have `ensure`
spawn a whole gopls to hand it a cancellation for a call it never received. The
lane decides this, not the routing: it owns its connection, so it is the only
party whose answer cannot already be stale by the time it is acted on. Routing
therefore sends the notification to the worktree that owes the id whether or not
its upstream is still there, and a lane with none drops it. Nothing is stranded
— the call died with the connection, and that connection's reader failed it (F2).
→ `TestCancellationFollowsTheRequestToItsUpstream`,
  `TestCancellationForADisconnectedUpstreamIsDroppedNotDialled`

## 3. Handshake

H1. The client's `initialize` is forwarded to home under the client's own id,
and with its parameters intact but for the roots capability S3 adds. The client
sees exactly one `initialize` result.

H1a. **Nothing is routed before that `initialize` has been seen.** A request the
client sent ahead of its own handshake is refused with `CodeInvalidRequest`; a
notification, having no id to answer, is dropped. Refused on the reader, which
is the only place the client's own order is visible: a lane sees a queue, and
the request ahead of the `initialize` and the `initialize` itself are two
iterations of one loop. The first would dial, find `r.initialize` already
filled — the reader stores it before routing — and spend H2's private handshake
on that connection. Answering the offending message is all this does: the
connection then owed a second `initialize` refuses it itself under H2a, so the
bridge is asking for the order the protocol already requires, not depending on
it.
→ `TestAMessageBeforeInitializeNeverWakesALane`

H2. Every later upstream — a new worktree, or a redialled home — is initialized
by replaying the client's stored `initialize` under the one fixed private id
`gopls-mcp-manager-init`: an id only has to be unique on the connection it is
used on, and a connection is handshaken exactly once — by the lane that owns
it, before anything else is written to it. That reply is swallowed; anything else the
upstream volunteers in that window is forwarded to the client.
→ `TestSendReconnectsAndInitializesRestartedHome`

H2a. **"Exactly once" is answered from the connection, not from message order.**
A lane holding a connection holds a handshaken one, so the client's own
`initialize` reaching a lane that already has an upstream is refused there and
never written — the second `initialize` is what gopls answers with the error
that ends the session. This is the invariant's own guard, which is why H1a is
free to be nothing but a refusal of the offending message: no ordering race
between the reader and a lane can put a duplicate on the wire.
→ `TestTheClientInitializeIsRefusedByAnAlreadyHandshakenUpstream`

H3. If the *initial* `initialize` fails to write, the retry re-sends the
client's own request rather than a private handshake.
→ `TestSendRetriesInitialInitializeWithoutPrivateHandshake`

H4. The handshake ends with a `notifications/initialized` notification.

H5. **The handshake is bounded** (the readiness budget plus 20 s — 30 s today —
per client message, F1's retry included —
a per-attempt deadline would stall the client twice over). It runs on the lane's
goroutine, so an upstream that accepts the connection and then answers nothing
would stall every later call to that worktree for the rest of the session, with
neither side timing out — and §8 deliberately keeps such a server alive, so
`ensure` hands one back. A budget already spent cancels F1's retry
rather than restarting it: redialling would pay §10's unbounded `flock` wait to
reach a handshake that expires on its first write. The deadline covers the
handshake, including the roots S2 answers inside it.
→ `TestAWedgedUpstreamFailsItsCallWithinOneBudget`

H6. **The connect is bounded by the same deadline**, and separately from the
handshake, because a wedged server stalls it before a handshake exists to
bound: the SSE connect waits for a first event that never comes, and §8
deliberately keeps such a server alive so `ensure` hands its port back. The
connection cannot simply be dialled under a deadline context — the SSE stream
is read under the context it was dialled with for the whole of its life, so
that would cut a healthy upstream loose the moment the budget expired, which is
the failure H5 exists to prevent. So the context is cancellable, only a
watchdog cancels it, and the watchdog is disarmed the moment the dial returns;
a connection handed back after it fired is closed, since its stream is already
unreadable. What this bounds is the connect alone. `ensure`'s `flock` takes no
context and stays unbounded (§10).

**The context is the connection's, and is released with it.** Surviving the dial
is not the same as surviving the session: the connection is what still needs it,
so closing the connection is what drops it. Left to the session context instead,
the very server this exists for would accumulate them — a wedged upstream fails
its handshake on every attempt, so its lane redials on every call, and each dead
connection would leave a live child behind for as long as the client stayed.
→ `TestAWedgedUpstreamFailsItsCallWithinOneBudget`,
  `TestADialContextLivesExactlyAsLongAsItsConnection`

H7. **Delivering the call is bounded by the same deadline too**, for the same
reason and against the same server. The SSE transport delivers a message as an
HTTP POST on a client carrying no timeout of its own, so under the session
context alone a server that takes the POST and never completes it parks its lane
on that one call for the rest of the session — and the calls queueing behind it
eventually fill the lane and stall the reader for every other worktree. Only the
delivery is covered; how long gopls then takes to answer is its own business and
stays unbounded (§10).

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

The client's own spelling of a key is the one rewritten, because `encoding/json`
matches field names case-insensitively: a client that sent `Capabilities` has a
field gopls already reads, and adding `capabilities` beside it would leave two
keys mapping to one — with the capability this whole rule exists to add landing
in whichever of them gopls did not take.
→ `TestWithRootsCapability`, `TestRootlessInitializeIsForwardedWithWorkingRoots`,
  `FuzzWithRootsCapability`

S4. **Every other server-initiated request is answered with
`CodeMethodNotFound`** on the connection it arrived on, and is not forwarded.
→ `TestUnroutableUpstreamRequestIsRefusedNotForwarded`

S5. S1 and S4 make the upstream reader a **second writer** on a connection its
lane also writes to. `mcp.Connection` documents `Write` as safe to call
concurrently and `Close` as safe alongside a blocked `Read`, and both
implementations in use honour it: `sseClientConn.Write` shares nothing but an
`*http.Client` and a mutex-guarded closed flag, and `ioConn.Write` serialises on
its own `writeMu`.

There is never a second *reader*, which nothing promises: a connection is read
by its handshake until that finishes, and only then does `cache` start the
steady-state reader — the same ordering F5a needs for the retry.

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
connection's reader would fail calls the new one had already accepted.
Cancellation routing needs the worktree instead, and gets it from the same entry
— see R7.
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

F5a. **A lane's connection always has a reader**, because `cache` is the only
thing that sets `lane.conn` and it starts the reader in the same step. It is
called only once the connection has taken a successful write, so a dial that
hands back an already-dead upstream fails on the lane's own goroutine, where the
retry is, rather than racing a reader that could claim the id first and answer
the client an error the retry is then not allowed to replace. The first attempt
on a new connection is single-goroutine. The converse does not hold: the failure
path clears `lane.conn` while its reader is still winding down.
→ `TestSendRetriesInitialInitializeWithoutPrivateHandshake` (the retry, which
  only a single-goroutine first attempt can reach)

F6. Only the stdio client going away stops the bridge. `io.EOF`,
`context.Canceled` and `ErrConnectionClosed` exit zero.

## 6. Port map file

Path `~/.local/share/gopls-ports.map`, mode `0600`, one JSON object per line:

```json
{"Worktree":"/Users/x/src/repo","Port":62001,"PID":4242,"StartedAt":1754218800}
```

M1. Written atomically: temp file in the same directory, `fsync`, `rename`.
Mode is set explicitly, because `CreateTemp`'s `0600` is still subject to umask
and an exotic umask could leave the file unreadable to its own writer.

M2. Every read-modify-write is wrapped in an exclusive `flock` on
`<map>.lock`, so concurrent `bridge`/`ensure`/`list` processes serialize.

The write is therefore skipped when it would change nothing — the steady state,
a warm worktree whose record is present and still answering. The `fsync` in M1
costs some sixty times everything else inside that lock — 5.5ms against 90µs
for sixteen records (`BenchmarkWithRecords`) — and it is a lock every process
on the machine shares.
The skip needs both halves of the question: equal records, *and* a read that
dropped no lines, since M3's repair is otherwise owed and would never come.
→ `FuzzReadMap`, `TestReadMapSkipsUnparseableLines`

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

M4a. `StartedAt` is unix seconds, and is the one field that is *not* validated
into a range, because it is not an argument to anything: it only widens or
narrows the grace of L7. Absent — a map written before the field existed — it
reads as zero, which is 1970, which is no grace at all: exactly the behaviour
those records already had.

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
2. **Start grace (L7).** A record younger than the readiness budget plus 5 s
   (15 s today) is alive, full stop.
3. HTTP probe of `127.0.0.1:<port>`, 500 ms, expecting `200` and
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
4. Identity, by `ps`: does that pid still run a `gopls` whose command line
   contains our listen address?
   - inconclusive **and** ours → alive; the record is kept, nothing is signalled
     → `TestRecordAlive` (cases "a server that only timed out is spared",
       "a server answering 503 is spared")
   - inconclusive **and** not ours → the record is dropped, nothing is signalled
     → same test, case "an inconclusive record that is no longer ours is dropped
       unsignalled"
   - dead **and** ours → `SIGTERM`, and the record is dropped
     → same test, case "a process with no MCP endpoint is killed"
   - dead **and** not ours → the record is dropped, nothing is signalled
     → same test, cases "not a gopls at all", "a gopls we did not run",
       "a gopls on another port"

L1. **Every record is offered to the liveness check**, including a second record
for a worktree that `ensure` will never answer with. The record is the only
handle on that process, and its port really is taken.
→ `TestCleanRecordsOffersEveryRecordAndDropsOnlyTheDead`, and `FuzzCleanRecords`
  for the same two properties at any length: the probes run at once and write
  into a shared slice by index, so what survives is assembled rather than
  filtered, and the order has to survive too — `forget` matches a record by
  value.

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
→ `TestRecordAlive` (case "an inconclusive record that is no longer ours is
  dropped unsignalled")

L4. The sweep writes its own result back, so a dead record disappears from the
file as a side effect of any command that reaches a port — whatever the caller
does next. `ensure` returning early, with no port allocatable or `gopls` failing
to start, drops them just the same: the file is made to agree with the processes
that exist at the moment the sweep decides which ones those are, not at the
moment its caller happens to succeed.
→ `TestListShowsLiveRecordsAndCleansDeadRecords`

L6. **Records are swept concurrently**, so the liveness check has to be safe to
call from several goroutines at once. The sweep runs inside M2's exclusive
`flock` and each probe waits out its own 500 ms, so in sequence the lock would be
held for the sum of the timeouts rather than the longest one — paid by every
other `bridge`, `ensure` or `list` process, and so by another worktree's next
tool call.
→ `TestCleanRecordsProbesEveryRecordAtOnce`

L7. **A record inside its start grace is alive without being probed.** Between
fork and bind a starting gopls refuses every probe, and refusal is the one
conclusive verdict above — so a sweep landing in that window would `SIGTERM` a
server that is coming up exactly as it should. P4 is what puts that window in
another process's reach: the readiness wait no longer holds the map lock. The
grace is derived as the readiness budget plus 5 s, so that it outlasts it by
construction — the two are spent in different processes, and one number must not
be tunable without the other.

The grace is asked **after** step 1, never before it. It covers a process that
exists and has not bound yet, and a start that crashed instead is not that:
spared until its timestamp expired, such a record would answer every `ensure`
for its worktree with a dead port for the rest of the window. A timestamp in the
future buys nothing either — this field is the only way to make a record
immortal, and the file is meant to be hand-editable.
→ `TestRecordAlive` (cases "a gopls still inside its start grace is spared",
  "a start that died inside its grace is not spared", "a record dated in the
  future gets no grace"), and `FuzzWithinStartGrace` for the claim across the
  whole domain rather than three points of it — this is the one field M1 does
  not range-check, since unlike the other three it is no argument to `kill(2)`,
  and the arithmetic behind it both wraps (the conversion to Go's epoch) and
  saturates (the subtraction into a nanosecond `Duration`).

## 9. Spawning

Started as `gopls mcp -listen 127.0.0.1:<port>` with `cwd` set to the worktree
and `setsid`, stdout and stderr appended to
`~/.local/share/gopls-mcp-logs/<sha256(worktree)[:8]>.log`. The hash is not for
brevity: a worktree is arbitrary bytes off the command line or out of `git`, and
this is the one place one becomes a path this process opens for writing.
→ `FuzzLogPath`

P1. The child is **reaped**, not released. `setsid` does not reparent, so an
unwaited gopls would stay a zombie child for the whole session — and
`kill(pid, 0)` succeeds against a zombie, which would make step 1 of §8 report a
dead server as alive.

P2. Readiness is polled for 10 s, backing off from 10 ms to a 100 ms ceiling —
flat at the ceiling, a gopls that binds a few ms after the fork goes unnoticed
for the rest of a tick, and this wait runs on the lane with H5's budget already
running. On timeout the child is signalled
through its `*os.Process`, which knows whether the reaper already collected it;
a bare `kill(pid)` could land on a recycled pid.

P2a. **That `SIGTERM` is escalated to `SIGKILL` after 2 s.** Dropping the record
(P5) is what makes the pid unfindable — no sweep can reach a process the map no
longer names — so a gopls that sits in its `SIGTERM`, wedged in filesystem I/O
being enough, would run unowned until reboot, holding its port and its 1–2 GB
against a worktree whose next `ensure` starts a second one beside it. That is
exactly the leak L2 exists to prevent, arrived at from the other side. The
escalation is fired off a timer rather than waited for, so the lane is not
parked for another 2 s on a path that has already spent the full readiness
budget; the window it leaves is this process exiting first, which costs what
the bare `SIGTERM` already cost. A process that has already gone answers with an
`ESRCH` nobody hears, and P1's reaper turns the corpse into an exit status
either way.

P3. `startGopls` returns the `*os.Process`, so `ensure`'s write-failure path
signals the handle it holds instead of re-deriving identity through `ps` — a
check that could only refuse and leak the process it just started.

P4. **The map lock is released before the readiness wait.** Held across it, one
cold start made every other `gopls-mcp-manager` process — and so every other
worktree's next tool call — queue behind it for up to the full 10 s, which is
precisely what worktree isolation produces. The record written before the lock
is dropped is what makes this safe: it reserves the port, and L7 keeps another
process's sweep off the server until it has bound.
→ `TestClaimPortReturnsBeforeItsGoplsIsReady`

P5. A gopls that never becomes ready has its record **dropped explicitly**,
under the lock again, rather than left for the next sweep: L7 would spare it for
a whole grace window, during which every `ensure` for that worktree would be
answered with a port nothing listens on. Dropped by identity rather than by
worktree and port: L2 makes the port deterministic in the worktree, so the port
a failed start held is the one the next start is handed, and a process delayed
past its own grace would otherwise delete the live record that replaced its own.
→ `TestEnsureSignalsAndForgetsAGoplsOfItsOwnThatNeverBecameReady` drives a
  spawned-but-never-ready gopls through `ensure` and gates both halves — the
  signal and the drop — since either alone is a leak: without the signal the
  index outlives the map that named it (P2a), without the drop the port is
  answered with for a whole grace window.
  `TestForgetDropsOnlyTheNamedRecord` gates which record is dropped.
  The readiness wait itself is a hook on the manager, so these cost a call
  rather than the full 10 s budget.

P6. **An `ensure` that did not start the gopls still waits for it, when the
record it found is inside its start grace.** L7 vouched for that record without
probing it, so its port can still refuse; waiting is what holding the flock used
to do for this caller, and skipping it hands the very next dial an
`ECONNREFUSED` for a server that was coming up perfectly. The failure belongs to
the process that started it — that one signals and forgets — so this caller only
reports it.
→ `TestEnsureWaitsForAGoplsAnotherProcessIsStillStarting`,
  `TestEnsureLeavesAnotherProcessesFailedStartAlone`

P7. Concurrent `ensure` calls for one worktree start **one** gopls between them:
M2's `flock` covers the sweep, the allocation, the spawn and the record write as
one step, so no second caller can see the port free while a spawn is in flight.
→ `TestConcurrentClaimPortSpawnsOnce`

## 10. Known limits

- A cold start no longer blocks anyone (P4, R3a), but the `flock` it does take
  is still unbounded: `flock` has no context form, so a lane parked on it is
  parked until whoever holds the lock lets go. The readiness poll behind it
  takes no context either, so a cancelled session leaves that lane polling out
  its budget. That is one worktree's lane rather than the whole session, and it
  is why `closeLanes` does not wait for its lanes to finish — nothing does, so
  the wait costs the exit nothing.
- **A worktree path must be valid UTF-8.** A Unix path is arbitrary bytes, but
  the map file is JSON, and `json.Marshal` replaces invalid UTF-8 with U+FFFD
  rather than failing. Such a record would read back naming a worktree nobody
  asked for — never matching its own, so every `ensure` would start another
  gopls beside the last and `forget` would never find the record to drop.
  `writeMap` refuses it instead, so the failure is one error rather than a
  process leak.
  → `FuzzMapRoundTrip`
- A wedged server — accepting connections, answering nothing or answering
  something that is not our endpoint — is our own gopls under §8 step 4, so it
  is kept for as long as it runs, at one probe and one `ps` per sweep. Kept
  deliberately: a timeout is also what a gopls indexing a large tree under a GC
  pause looks like, and L5 would rather re-ask than kill a healthy 1-2GB index.
  So its port keeps being handed back, and every call to that worktree pays the
  budget again — H6 bounds the connect it stalls, H5 the handshake if it gets
  that far, and neither ends the record. What the bounds buy is that the lane
  fails each call and stays usable, rather than parking on the first one for the
  rest of the session.
- A worktree with more than 64 calls outstanding fills its lane's queue, and the
  reader waits on it — the one case where one worktree can still hold up
  another.
- **Nothing bounds a call once it has been written.** H5 covers the handshake
  only; past it, the answer is whatever `readFromUpstream` reads, under the
  session context alone. An upstream that takes a `tools/call` and then neither
  answers nor dies leaves the client waiting forever — F2 fires on a connection
  dying, not on one going quiet, and the §8 sweep asks a fresh probe whether the
  server is up, not whether that call is progressing. Cancelling it (R7) reaches
  the right gopls but does not release the caller either.
- A cancellation for a call whose upstream is already gone is dropped by that
  worktree's lane rather than dialling the worktree back (R7). The call itself
  is not left hanging: the dead connection's reader fails it under F2.
- A spawn that fails takes its own caller's call down, and briefly a concurrent
  one's too: between the starter's `SIGTERM` and its `forget` landing, another
  process can still find the record inside its grace, wait out P6's budget on a
  port that will never answer, and report the same failure. It self-heals — once
  `forget` lands, or once the process is gone and step 1 reaps the record, the
  next `ensure` starts a fresh gopls — but that one call is lost where the
  lock-across-readiness design would have made it wait and then succeed.
- Duplicate records for one worktree are preserved rather than reconciled;
  `ensure` answers with the first.
- **Nothing caps how many gopls run at once, and nothing evicts an idle one.**
  Every other quantity here is bounded — the lane queue, the handshake budget,
  the port range — but the process count is not: a session that touches N
  worktrees ends with N servers at 1–2 GB each, and the map outlives the
  session, so a box that is not rebooted accumulates them across invocations.
  Reaping is for servers that have already died (§8); a live one is never
  gathered for being unused. Deliberate rather than overlooked, and it is L5's
  position taken to its conclusion: the sweep would rather re-ask than kill a
  healthy index, and an eviction clock cannot tell a worktree that is finished
  from one between two calls — it would reclaim the memory by throwing away the
  index the next call needs, paying a full cold start for it. What is missing is
  a ceiling, not a clock: the operator's handle today is `list` plus killing the
  ones they know are done. An LRU cap belongs here the day someone observes the
  memory actually hurting, and it wants a real number, not a guess.
- **The identity check races the signal it guards.** §8 asks `ps` whether a pid
  is still our gopls and then calls `kill` on it, so a pid recycled between the
  two receives a `SIGTERM` meant for its predecessor. Unfixable as written and
  left alone rather than papered over: POSIX offers no "signal this pid only if
  it is still that process", and the portable handle that would — holding the
  child's own `*os.Process`, as P3 does — exists only in the process that
  started it, which is precisely not the one sweeping. The window is the two
  syscalls wide, it needs a pid to wrap around onto that exact number inside it,
  and the check already removes the far larger risk it was written for: a pid
  reused across a reboot, where the map's numbers are stale by definition.
