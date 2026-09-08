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

R1. Only absolute file, dir and files arguments are routing evidence. Relative
paths and ordinary resolution failures preserve sticky routing, falling back to
home. Every resolvable path must identify the same worktree; a mixed-worktree
call fails without changing sticky state.
→ `TestToolCallRoutesToTheWorktreeOwningItsPath` (case "relative path"),
`TestToolCallSpanningTwoWorktreesIsRefused`

R2. A missing immediate parent is not climbed past. Symlinked files resolve to
the worktree holding the target; existing and missing files under a symlinked
directory share its physical directory memo key.
→ `TestWorktreePathResolvesEachPathToItsOwnWorktree`,
`TestSymlinkedDirectorySharesMemoForExistingAndMissingFiles`

R3. Pathless calls follow the last successfully resolved single worktree.
Routing decisions commit in client wire order. Each worktree has one delivery
lane, so a cold dial or handshake does not block delivery on another lane.
→ `TestTargetFallsBackToTheLastPathBearingCall`,
`TestAColdWorktreeDoesNotHoldUpAnother`

R4. One total 10 s routing budget covers the resolver queue and every path in
one message, rather than a fresh timeout per path. A single worker with an
unbuffered job channel owns filesystem resolution; memo access is synchronized. A non-cancellable
filesystem syscall can occupy that worker after timeout, but cannot create
unbounded replacement workers or commit a late sticky result. A routing timeout
fails the call rather than falling back to a possibly wrong worktree.
→ `TestRoutingBudgetDoesNotMultiplyOrGrowWorkers`

A separate ingress reader admits calls before resolution and handles cancellation
immediately, even when the 64-message routing queue is full. Ordinary requests
commit routing in wire order. Pathless calls and complete path-memo hits bypass
the filesystem worker. The 40 s ingress deadline includes routing queue time,
resolution and delivery; delivery also has a 30 s ceiling after routing.
→ `TestIngressCancellationBypassesBlockedResolution`,
`TestFullIngressQueueStillAcceptsCancellation`

R5. Successful resolutions are memoized by verbatim path and physical containing
directory. Failures are not cached. Each memo defaults to at most 4096 entries;
a full memo clears before inserting a new result. Evicted entries are resolved
afresh. Retargeting a symlink or replacing a worktree can leave a stale hit until
capacity rollover or session restart; there is no time-based expiry.
→ `TestWorktreeOfResolvesAndMemoizes`, `TestMemoCapacityRevalidatesEvictedPaths`

R6. Each delivery and control queue holds 64 messages. Admission never waits for
space: excess requests receive overload error -32000; excess notifications are
dropped. Queue time counts against the delivery budget, and expired queued work
cannot start a server.
→ `TestFullLaneRejectsWithoutBlockingOtherWorktrees`,
`TestQueuedDeliveryExpiresWithoutDialling`

R7. Cancellation completes the named outstanding request locally with -32800,
releases its outstanding slot and suppresses a later upstream response. Queued
or dialling delivery is cancelled. Once assigned to a connection, advisory
cancellation carries that exact connection on the lane's control queue and waits
for the original write to finish before forwarding. A full control queue can
drop the advisory notification but cannot undo local completion. Unknown,
already completed or malformed cancellation ids are dropped. A notification
itself has no response; the terminal response belongs to the request it names.
→ `TestQueuedCancellationPreventsDelivery`,
`TestCancellationBypassesFullDeliveryQueue`,
`TestCancellationCannotOvertakeItsCall`,
`TestCancellationFollowsACallOntoItsRetryConnection`

R8. Outstanding requests are bounded independently of queue depth: 128 per
worktree lane and 1024 per session by default. A server accepting writes without
answering cannot bypass these limits by draining the delivery queue. Completion,
failure, execution timeout, local cancellation and session teardown release
tracking state. Retry retains the same admission slot.
Per-worktree counts are updated in constant time; all terminal paths remove
accounting centrally. Retained lanes have a separate default ceiling of 64.
Requests rejected by session or lane-retention limits do not create new lanes.
Existing lanes remain usable at the retention ceiling. No shared server is
evicted to make room.
→ `TestOutstandingLimitsSurviveFastDelivery`,
`TestRetentionLimitDoesNotAllocateRejectedLane`,
`TestOutstandingLimitDoesNotAllocateNewLane`,
`TestAccountingTerminalPathsAndSnapshots`

R9. Execution deadlines are optional and disabled by default. When configured,
the timer starts after successful delivery and competes with responses and
connection failure for one terminal completion. A timer carries a generation
token so a delayed callback cannot complete a newer request with the same id.
A timeout ends local waiting; it does not prove upstream work stopped.
→ `TestExecutionDeadlineCompletesOnce`

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
rather than restarting it. The deadline starts at queue admission and covers
lock acquisition, readiness, connect, handshake, roots replies and delivery.
Cleanup of an owned failed child has its own budget (P2a).
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
unreadable. The same cancellation reaches `ensure`: lock acquisition uses
nonblocking flock retries, HTTP probes carry the context, ps has a one-second
ceiling, and readiness polls wait on the context. A cancelled sweep writes
nothing and does not treat cancellation as evidence against a shared server.
→ `TestEnsureCancelledAtLockDoesNotSpawn`, `TestCancelledSweepPreservesRecords`,
  `TestReadinessProbeHonoursCancellation`

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
on that one call. Subsequent calls can fill the bounded lane queue, at which
point excess requests are refused rather than blocking other worktrees. Only the
delivery is covered by this budget. Post-delivery waiting is governed by the
optional execution timeout and outstanding limits (R8–R9).

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

S4a. Server-request replies have a 30 s ceiling, shortened by any existing
handshake deadline. A failed reply closes the connection and explicitly fails
its outstanding calls. The next call can reconnect using ordinary lane recovery.
→ `TestServerReplyFailureCompletesOutstanding`,
`TestServerReplyKeepsShorterHandshakeDeadline`

S5. S1 and S4 make the upstream reader a **second writer** on a connection its
lane also writes to. The cancellation control worker is another writer. `mcp.Connection` documents `Write` as safe to call
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

F5. **Whoever takes an id off the pending list owns the reply.** Responses,
connection failure, cancellation and execution timeout compete for completion.
A retry atomically changes the owning connection to nil while retaining the
admission slot and cancellation state; it cannot revive a completed call. In particular, a failed write and
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

F6. The stdio client going away or the session context being cancelled stops
the bridge. `io.EOF`, `context.Canceled` and `ErrConnectionClosed` exit zero.
Shutdown cancels delivery, closes lane queues and waits for lanes to finish
owned failed-child cleanup (P2a).

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
Acquisition uses nonblocking attempts separated by cancellable 10 ms waits.
All CLI commands install signal cancellation before manager operations.

The write is therefore skipped when it would change nothing — the steady state,
a warm worktree whose record is present and still answering. The `fsync` in M1
costs some forty times everything else inside that lock and up — around 6ms
against 150µs for sixteen records, the ratio moving with how busy the disk is
(`BenchmarkWithRecords`) — and it is a lock every process on the machine
shares.
The skip needs both halves of the question: equal records, *and* a read that
dropped no lines, since M3's repair is otherwise owed and would never come.
→ `TestWithRecordsWritesOnlyWhenTheFileWouldChange`, `FuzzReadMap`,
`TestReadMapSkipsUnparseableLines`

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

Probing is read-only and returns one of four verdicts: live, uncertain,
terminate, or gone. A boolean cannot express the distinction between a process
that should stop and one confirmed gone.

1. Check process existence with kill(pid, 0). A missing process is gone.
2. For a record already marked Terminating, check process identity. Keep it
   terminating while it still belongs to us; an identity error is uncertainty.
3. Otherwise, a living process inside the 15 s start grace is kept without an
   HTTP probe. Future timestamps receive no grace.
4. Probe the endpoint with a 500 ms ceiling. A valid SSE response is live;
   connection refused is the only conclusive endpoint failure.
5. On other results, check that ps still identifies our gopls and listen address.
   Identity failure is uncertainty. A different process means our old record is
   gone and is never signalled. Refusal plus matching identity requests
   termination; other matching results remain uncertain.

→ `TestRecordAlive`, `FuzzWithinStartGrace`

L1. Explicit full maintenance (`list`) probes every snapshot record concurrently,
including duplicate worktree records. Acquisition (`ensure` and bridge dials)
probes only records matching the requested worktree. All other records remain
reserved, including stale and terminating ones; maintenance is explicit.
→ `TestAcquisitionDoesNotProbeUnrelatedRecords`

Probes run outside the global registry flock. Reconciliation rereads
the current map under lock and applies a verdict only to an exactly matching
record. New or replaced records are retained; a stale result cannot delete or
mark a replacement.
→ `TestSweepReconcilesOnlyTheProbedIdentity`

L2. Before signalling, persist Terminating=true. This commit survives a later
caller mutation failure. A separate per-record action flock serializes signal
attempts. Recheck the atomic registry snapshot and process identity before
SIGTERM, without holding the global registry lock.
→ `TestSweepSignalsOnlyAfterPersistingTermination`

L3. Sending SIGTERM is not proof of exit. Retain the record on signal error,
ignored SIGTERM or delayed exit; ensure refuses its endpoint and does not spawn
another process for that worktree. A later sweep removes it only when the old
process is gone or identity belongs to a different process. This cross-process
path does not automatically escalate to SIGKILL; owned failed children use P2a.
→ `TestSweepRetainsAProcessIgnoringTermination`

L4. Ordinary reconciliation and caller mutation share a single atomic commit.
When the body refuses and no termination action is required, nothing is written.
A terminating transition is deliberately a separate prior commit: even if
ensure then refuses that worktree, its cleanup record must stay discoverable.
Cancelled probes do not reach reconciliation.
→ `TestWithRecordsWritesNothingWhenTheBodyRefuses`,
`TestCancelledSweepPreservesRecords`

L5. Start grace is readyTimeout plus 5 s by construction. Existence is checked
before grace, so an already dead start is not spared. The initial readiness wait
is outside the flock, while the persisted record reserves its port.
→ `TestClaimPortReturnsBeforeItsGoplsIsReady`,
`TestEnsureWaitsForAGoplsAnotherProcessIsStillStarting`

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

P2a. **Failed-child cleanup completes before return.** Send SIGTERM, wait up to
2 s for reaping, then SIGKILL and wait up to another 2 s. This uses the owned
process handle and its reaper's done channel, independent of request cancellation.
The CLI and bridge shutdown therefore cannot abandon a delayed escalation.
If exit cannot be confirmed, return an error naming the pid and retain any
existing registry record. Once exit is confirmed, forgetting has an independent
2 s lock-acquisition budget; a failed forget leaves a dead record for a later sweep.
→ `TestFailedChildIsReapedBeforeReturn`

P3. `startGopls` returns a child handle containing `*os.Process` and proof of
reaping. Map-write failure uses the same termination procedure, since the child
has no persisted record. Cancellation is checked before spawn; the running
shared server is not tied to the request context.

P4. **The map lock is released before the readiness wait.** Held across it, one
cold start made every other `gopls-mcp-manager` process — and so every other
worktree's next tool call — queue behind it for up to the full 10 s, which is
precisely what worktree isolation produces. The record written before the lock
is dropped is what makes this safe: it reserves the port, and L7 keeps another
process's sweep off the server until it has bound.
→ `TestClaimPortReturnsBeforeItsGoplsIsReady`

P5. An owned gopls that never becomes ready has its record **dropped after
confirmed exit**,
under the lock again, rather than left for the next sweep: L7 would spare it for
a whole grace window, during which every `ensure` for that worktree would be
answered with a port nothing listens on. Dropped by identity rather than by
worktree and port: §7 makes the port deterministic in the worktree, so the port
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
  `TestEnsureLeavesAnotherProcessesFailedStartAlone`,
  `TestCancelledReadinessLeavesAnotherProcessesServerAlone`

P7. Concurrent `ensure` calls for one worktree start **one** gopls between them:
M2's `flock` covers reconciliation, allocation, spawn and the record write as
one step, so no second caller can see the port free while a spawn is in flight.
→ `TestConcurrentClaimPortSpawnsOnce`

## 10. Known limits

- Routing waits are bounded even if the OS stalls a filesystem syscall, but the
  single resolver worker can remain occupied until that syscall returns.
  Session shutdown does not wait for that worker. Other filesystem operations,
  including registry reads, fsync and rename, are not context-interruptible.
- The shared stdout queue still backpressures forwarding when the client
  stops reading. An execution-timeout callback finding this queue full fails
  the session rather than parking an unbounded number of timer goroutines. Delivery/handshake code forwarding unsolicited messages can
  also encounter that output backpressure.
- With execution deadlines disabled, an accepted request can wait indefinitely;
  outstanding limits bound retained requests, not execution duration.
  Cancellation or timeout cannot prove that a gopls mutation stopped.
- Live shared-process count is not capped. Lane and memo caps bound one bridge
  session, not shared daemons or payload bytes. No idle process eviction is
  enabled. Cross-client attachment and operation ownership must be established
  first; see LEASES.md.
- A terminating process that ignores SIGTERM remains recorded and blocks ensure
  for its worktree. This preserves operator visibility. Owned failed children
  have bounded escalation; a sweeper has no owned process handle.
- PID identity checks still race signals on POSIX. Per-record action locks
  serialize cooperating managers but cannot make OS PID reuse atomic.
- Old manager binaries ignore Terminating. Upgrade all managers sharing the map
  before relying on termination retention; mixed-version safety is not promised.
- Duplicate records for a worktree are preserved, and ensure uses the first.
- Worktree paths must be valid UTF-8 to round-trip through JSON.
  → `FuzzMapRoundTrip`
- A starter failing readiness can briefly cause another caller waiting on its
  start-grace record to fail too. Once confirmed exit is forgotten, ensure can
  start a fresh generation.

## 11. Configuration and observation

Environment variables are parsed when constructing the manager:

| Variable | Default | Meaning |
| --- | --- | --- |
| GOPLS_MANAGER_MAX_OUTSTANDING | 1024 | Session outstanding-request ceiling |
| GOPLS_MANAGER_MAX_OUTSTANDING_PER_LANE | 128 | Per-worktree outstanding ceiling |
| GOPLS_MANAGER_MAX_LANES | 64 | Retained worktree lanes per session |
| GOPLS_MANAGER_MAX_CACHE_ENTRIES | 4096 | Entries per memo; clear on capacity rollover |
| GOPLS_MANAGER_EXECUTION_TIMEOUT | 0s | Optional post-delivery timeout |
| GOPLS_MANAGER_METRICS | unset | Set to 1 for JSON observations on stderr |

Counts must be positive integers. Execution timeout must be a nonnegative Go
duration. Manager metrics report lock wait/hold and probe, readiness and ensure
durations. Session snapshots every 30 s and at shutdown report request counts,
fixed rejection reasons, cancellation/expiry counts, retained lanes and memo
sizes, and count/total/max nanoseconds for routing, queue waits and handshake.
Snapshots copy mutable counters under their owning locks. Completed includes
local termination and shutdown cleanup, not just successful upstream answers.
→ `TestCallLimitConfiguration`, `TestNewRetentionSettings`,
`TestAccountingTerminalPathsAndSnapshots`

The status command is read-only, emits JSON and never invokes a sweep. It reports
recorded server count, registry integrity, termination state and RSS in KiB when
one bounded ps snapshot matches the recorded gopls identity. Missing measurements are null. Active
client count is always null until cross-client accounting exists.
→ `TestStatusMeasuresWithoutChangingRegistry`

Stdio uses a direct, case-sensitive parser for complete scalar messages; the
pinned SDK still handles writes, batch correlation and fallback validation.
Raw fields are owned copies. SSE remains on the SDK transport.
→ `TestStdioMatchesSDK`, `TestStdioPreservesBatchResponses`,
`TestStdioCloseUnblocksRead`, `FuzzDecodeScalarMatchesSDK`

Implementation remains one package. Session transport, routing, resolver/path
logic, lanes, request accounting, upstream protocol handling, registry,
process lifecycle and probes live in separate files. requests.go owns terminal
request completion and generation checks; registry.go owns persistent mutation.

## Stateless HTTP frontend

`http [address [path]]` exposes `/mcp` using the SDK's stateless Streamable HTTP
handler with JSON responses. Defaults are `127.0.0.1:6099` and home `.`; only
loopback listeners are accepted. Existing stdio behavior remains available.

The HTTP frontend terminates protocol negotiation locally and exposes gopls
tools through `tools/list` and `tools/call`. Home initialization supplies gopls's
instructions. A process-wide router owns bounded legacy SSE lanes, roots replies,
private handshakes and internal request accounting. No frontend session identifier
is issued or used. Internal request identifiers are unique across HTTP clients.

Unlike stdio, HTTP routing never reads or writes sticky state. A pathless call
always goes home; absolute path arguments retain the existing worktree resolution
and multi-worktree rejection rules. Limits and caches are shared across requests,
not recreated per POST. HTTP request cancellation completes the internal call and
sends advisory cancellation to its owning SSE connection. Cross-request cancellation
notifications and unsolicited upstream notifications are not relayed.

HTTP exchanges have a separate capacity gate using the session request limit,
4 MiB request bodies, 5 s header reads, 30 s body reads, and a 30 s response-write
budget starting when response output begins. The SDK owns HTTP validation and
stateless request lifecycle; gopls continues to use legacy SSE unchanged.

Covered by `TestHTTPStatelessRoutingAndSSEReuse`, `TestHTTPProtocolAndErrors`,
`TestHTTPCurrentSDKClient`, `TestHTTPDisconnectCancelsLegacySSECall`, and
`TestHTTPBackendShutdownReleasesCalls`.
