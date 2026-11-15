# gopls-mcp-manager — behavioural spec

Normative description of what the bridge and the manager guarantee. Written
from the code as it stands; each invariant names the test that pins it.

## 1. Roles

```
MCP client → stdio/HTTP frontend → private Unix owner → SSE/HTTP → guarded gopls
```

- **home** — the worktree the bridge was started in. Always the first upstream.
- **upstream** — one connection to one gopls, identified by its worktree path.
- **sticky** — the worktree of the most recent tool call that named a resolvable
  absolute path.

### Persistent owner contract

O1. Production frontends are thin transports. One persistent singleton hosts
routers, HTTP backends and every upstream write; no direct fallback is allowed.
Its private versioned Unix endpoint and configuration fingerprint reject mixed
clients. Canonical registry identity determines a short socket beneath `/tmp`,
independent of TMPDIR/HOME length. Singleton authority is a lifetime registry lock.
→ `TestSupervisorSocketUsesShortStablePrivateIdentity`,
`TestSupervisorProtocolAndConfigMismatchFailBeforeAllocation`

O2. One mutex serializes admission, target binding and drain eligibility. Queued,
active and protected abandoned operations prevent retirement. Quiet attachments
do not. Readiness starts the monotonic completion clock; definitive responses
advance it. The default idle period is positive one hour, sampled every 30 s,
and the age is rechecked at the drain verdict. Cancellation/disconnect does not
complete remote work. Abandoned work retains capacity through a separate idle
grace and controlled generation retirement.
→ `TestSupervisorIdleBoundaryAndQuietConnections`,
`TestSupervisorQueuedAndActiveWorkSurviveIdle`,
`TestSupervisorAbandonedOperationsRetireOnlyAfterGrace`,
`TestSupervisorIdleCandidateRechecksLastCompletion`

O3. Drain fences delivery before graceful SIGTERM. Guardian-owned handles and
anchored process groups provide signals and exit observations. Ordinary shared
cleanup waits ten seconds and never escalates to SIGKILL. Uncertain starts,
ignored TERM, guardian loss and storage failures retain discoverability and
capacity. Only confirmed child/group/guardian exit permits release. Durable
pre-fork manifests and stable lifetime locks fence restart and owner-crash windows.
Legacy records prevent migration. PID disappearance alone is not proof.
A successful process census must include the observing process in its actual
process group; an empty or incomplete view cannot prove group absence. Missing
initial ancestry evidence remains uncertain after later successful observations.
→ `TestGuardianSnapshotRequiresObserverIdentity`,
`TestGuardianFailedInitialObservationRetainsUncertainty`

O4. Defaults are 16 combined frontend attachments/cached HTTP backends, 1024
global queued/unconfirmed operations, and 64 generation slots. Private handshake
and server control requests consume that same operation cap. All routers share
64 MiB SSE capacity and HTTP maximum-body reservations (16 exchanges by default).
Per-router queues/lanes/memos compose under the frontend cap. One owner-wide
filesystem worker contains blocked calls across retired sessions. Managed logs
have one process-global disk worker, a bounded queue and aggregate retention;
queue/storage failure fences owner delivery.
Retained ingress/delivery, output and advisory payloads share64MiB maximum-message
reservations, released by actual consumption/discard/Write return. Local cancel
does not refund a queued object. Opaque string IDs are≤1024bytes and the retained
initialize template≤64KiB. These bounds exclude transient decoder/serializer/GC
overhead. Budget exhaustion fails admission/output explicitly; advisory overload
still completes local cancellation immediately.
→ `TestSupervisorGlobalAdmissionDoesNotMultiplyWithRouters`,
`TestSupervisorSharedFilesystemWorkerSurvivesSessionChurnBoundedly`

O5. Before spawn, complete watched-tree admission reserves child FD quota plus
guardian/control headroom. Kernel RLIMIT_NOFILE enforces managed child/owner
quotas. Prospective victims remain charged until confirmed exit. These limits
do not bound total RSS, arbitrary project process fanout, external caches or all
kernel watcher memory. Darwin process groups cannot prove universal containment
of arbitrary escaped project subprocesses. Managed telemetry compatibility is
checked explicitly; unsupported selected Go/gopls toolchains fail.

Each later roots reply is checked against the original immutable child quota
before snapshot publication or upstream Write. Expanded plans fail explicitly
and require a new generation, preserving the established watch snapshot and
active/unknown-work retirement protection. HTTP backend retirement keeps its
entry charged until actual router teardown completes, rejects reuse during
closing, and has one teardown owner across sweep/shutdown.
→ `TestSupervisorRetiringBackendKeepsCapacityUntilCloseCompletes`,
`TestSupervisorExpandedRootsRefuseBeforePublicationAndPreserveRetirement`,
`TestSupervisorSameAndSmallerRootsFitImmutableQuota`

The data proxy has no fixed response-header timeout that overrides Execution=0
or a longer execution policy. Request cancellation and body/write/socket limits
still apply. Ping/startup/CONNECT retain explicit budgets; management queries
use at most40seconds overall (an earlier caller deadline wins). A canceled cached
HTTP router fails the current request without replay, retires independently of
recent traffic, and keeps capacity through active handlers and actual teardown.
Only a later independent request initializes a replacement backend.
→ `TestSupervisorProxyExecutionFollowsCallerBudget`,
`TestSupervisorFailedHTTPBackendRetiresDespiteRecentUse`,
`TestSupervisorFailedHTTPBackendKeepsCapacityUntilActiveHandlersFinish`,
`TestSupervisorFailedHTTPBackendReinitializesOnIndependentRequest`

O6. The owner survives zero clients. The first normal call after proven idle
retirement reconnects before caller delivery. Actual uncertain caller writes
fail explicitly and are never replayed (F1). See section 9 for recovery scope.
Legacy process reservations are never adopted or signalled automatically;
nonempty offline registries require explicit operator reconciliation.
→ `TestSupervisorTwoBridgesAndHTTPShareAndReconnectAfterIdle`

O7. Each router requires an explicit owner and keeps only local waiters/timers.
The owner alone records remote operations and per-session/worktree counts, under
its mutex. Remote records retain separate connection/admission identity tokens
and counters, without routers, timers, payloads, callbacks or HTTP writers.
One upstream type owns transport, generation identity and cancellation lifetime.
For caller responses, releasing the remote identity and claiming the matching
local waiter occur under the router lock; merely reading a response cannot make
its ID reusable. Private handshake responses complete within the upstream.
Empty per-worktree count entries are removed on terminal completion or confirmed
exit. Disconnect abandons remote work; it never refunds its credits.
→ `TestResponseReadCannotReleaseIDBeforeWaiterClaim`,
`TestConfirmedExitRestoresLongSessionCapacity`,
`TestUnconfirmedExitRetainsRouterOperationCredits`

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
a full memo clears before inserting a new key. Both memo levels expire together
after GOPLS_MANAGER_CACHE_TTL (default 5m), checked lazily on access or a metrics
snapshot. Retargeting a symlink or replacing a worktree can leave a stale hit
until expiry; a directory hit cannot renew it into the next epoch. Slow or
cancelled lookups cannot repopulate a newer epoch. Updating a key at capacity
does not clear the memo. Expiry bounds validity, not the timing of idle heap release.
→ `TestMemoEpochExpiresBothLevels`, `TestMemoExpiryRevalidatesRetargetedSymlink`,
`TestMemoRefreshAtCapacityDoesNotEvict`
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
already completed or malformed cancellation ids are dropped. Fractional or
out-of-range decoded numeric ids are rejected before the SDK can truncate them
to another request id. A notification
itself has no response; the terminal response belongs to the request it names.
Advisory writes and operation placement share a cancellable lane gate. Under
that gate an advisory is dropped if its connection/id now belongs to another
request generation; a later reuse cannot overtake an advisory already writing.
→ `TestQueuedCancellationPreventsDelivery`,
`TestCancellationBypassesFullDeliveryQueue`,
`TestCancellationCannotOvertakeItsCall`,
`TestCancellationAfterUncertainWriteCannotReachANewConnection`
→ `TestDelayedCancellationCannotCancelReusedID`, `TestCancellationWriteFencesIDReuse`,
`TestCancellationNumericIDDoesNotAliasAnotherRequest`

R8. Outstanding requests are bounded independently of queue depth: 128 per
worktree lane and 1024 per session by default. A server accepting writes without
answering cannot bypass these limits by draining the delivery queue. Completion,
failure, execution timeout, local cancellation and session teardown release
tracking state. Retry retains the same admission slot.
Separate upstream-operation credits are acquired atomically before Write, using
the same session/per-worktree ceilings. Local completion and connection failure
do not release them. Terminal upstream responses release the exact connection/id
credit, even after local cancellation; a proven pre-POST size rejection releases
its reservation too. An uncertain retry on another connection needs another
credit. Lost-connection credits remain until authoritative generation exit or
router shutdown. A surviving router reclaims confirmed-exit credits before
rejecting delivery at its operation ceiling and when reporting usage. Connection
loss or unconfirmed termination cannot reclaim them. Reusing an id on the same
connection is refused while its old operation remains unresolved. The persistent
owner also accounts globally across frontend lifetimes.
→ `TestAbandonedOperationsBoundFurtherDelivery`,
`TestDisconnectedOperationsDoNotReleaseCredits`,
`TestAbandonedIDCannotBeReusedOnItsConnection`,
`TestConfirmedExitRestoresLongSessionCapacity`,
`TestUnconfirmedExitRetainsRouterOperationCredits`
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
It queues the same bounded exact-connection advisory cancellation as client
cancellation, after the original write completes.
→ `TestExecutionTimeoutQueuesAdvisoryCancellation`
→ `TestExecutionDeadlineCompletesOnce`

R10. Every absolute file, dir and files argument whose spelling this manager has
resolved is forwarded physically. gopls compares a path argument against the
view it loaded, and a symlinked spelling of a file inside that view does not
fail: it answers from the single package it can reach and reports the short
result as if it were complete, with no error and no marker the client could
notice. On macOS that is every path under /tmp and under $TMPDIR, since /tmp and
/var are symlinks, so an agent that copies the spelling `pwd` printed silently
receives less than it asked for — measured on gopls v0.23.0: three references
for a /tmp spelling against seven for the physical spelling of the same file in
the same session. Substitution reads the path memo only, never the filesystem,
so it cannot stall the ingress reader; a spelling that is not memoized,
unresolvable or relative is forwarded untouched, as is every other argument and
field of the message.
→ `TestToolCallForwardsPhysicalPathSpelling`,
`TestDeliveredToolCallCarriesThePhysicalSpelling`

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

H3. An uncertain write of the initial `initialize` fails explicitly and is not
replayed. No private handshake is sent in its place.
→ `TestSendDoesNotReplayAnUncertainInitialize`

H4. The handshake ends with a `notifications/initialized` notification.

H5. **The handshake is bounded** (the readiness budget plus 20 s — 30 s today —
per client message, including pre-delivery dial/private-handshake retries —
a per-attempt deadline would stall the client twice over). It runs on the lane's
goroutine, so an upstream that accepts the connection and then answers nothing
would stall every later call to that worktree for the rest of the session, with
neither side timing out — and §8 deliberately keeps such a server alive, so
`ensure` hands one back. A budget already spent cancels a pre-delivery retry
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
unreadable. The caller cancellation stops waiting for owner acquisition and closes any
connection returned after expiry. The shared start uses the owner's lifetime
context: a cancelled waiter cannot cancel another client's generation. Readiness
still has its own bounded polling budget.
→ `TestReadinessProbeHonoursCancellation`,
`TestSupervisorCancelledWaiterCannotCancelSharedStart`

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

S1. **`roots/list` is answered by the bridge**, never forwarded, with the roots
of the worktree that upstream serves, each a `file://` URI with its base name:
the worktree itself when no filter excludes anything under it (S1a).
→ `TestUpstreamRootsAnsweredWithItsOwnWorktree`

S1a. **Directory filters narrow the roots.** The rules are the `ignore`
directives of the worktree's `go.mod` and of every module its `go.work` uses
(`./dir` relative to that module, a bare `dir` at any depth, as in cmd/go),
followed by GOPLS_MANAGER_DIRECTORY_FILTERS, with gopls `directoryFilters`
semantics: last match wins, a rule covers its subtree, `**` is zero or more
segments. When a rule excludes a directory, the roots are the smallest set of
directories covering the worktree minus the excluded trees; a `+` rule
re-includes a subtree below an excluded one. Directories gopls' watcher skips
(names starting with `.` or `_`, and `testdata`) are neither roots nor
exclusions, so `.git` is never watched whatever a rule says.
Module metadata contributes filters only when it resolves to a regular file
of at most 4 MiB; devices, FIFOs and oversized files contribute none. Regular
file symlinks remain supported. Environment filter rules must be valid UTF-8.
Individual rules are capped at 4096 bytes before regexp compilation; invalid
decoded module paths and unusable module rules are ignored. Environment rules
exceeding the cap are rejected at startup.
→ `TestUpstreamRootsCoverTheWorktreeMinusFilters`,
`TestPlanWatchCoversTheWorktreeMinusExcludedTrees`,
`TestPlanWatchWithoutExclusionsIsTheWorktree`,
`TestPlanWatchReincludesBelowAnExcludedTree`, `TestGitDirectoryIsNeverWatched`,
`TestPathIncludeFuncMatchesGopls`, `TestModuleIgnoreFiltersReadGoModAndGoWork`,
`TestParseDirectoryFilters`
→ `TestModuleMetadataReadsAreBounded`, `FuzzDirectoryFilterPrecedence`

S1b. **A server that missed an uncovered change is restarted.** No root covers
the entries directly inside a "partial" directory, one with an excluded
descendant (the worktree itself whenever anything is excluded). gopls still
sees edits to Go files there, since every file-scoped MCP tool compares its
package's mtimes first, but not an added or removed file or directory, nor a
go.mod/go.sum/go.work change. Each manager keeps, per worktree, the Go-relevant
entry names (included subdirectories, `.go`/`.mod`/`.sum`/`.work`/`.s` files)
of every partial directory as of the first S1 answer its gopls received. gopls only
adds watches and reports no file that already exists, so a later answer to the
same gopls (another session) does not describe its watcher: a snapshot belongs
to one server, identified by the pid and `StartedAt` of the record `ensure`
claimed for the dial, and only an answer from a different server replaces it.
A server whose first answer left nothing uncovered keeps an empty snapshot, so
later answers cannot condemn it. A connection of unknown identity replaces the
snapshot on every answer. A maintenance sweep condemns a live, non-terminating
record whose partial directories no longer hold the same names, or hold a
module file modified after its start (`StartedAtMilli`, M4b), by the L6 path:
busy worktrees are spared and termination goes through L2. A snapshot of
another server is ignored (without identities: one taken before the record's
`StartedAt`). The snapshot is dropped only once the sweep has committed the
server's termination or removal, so one change restarts one server, and a server
spared (busy, an uncertain identity check, a record changed under the probe)
stays stale for the next sweep.
The next call starts a fresh one with a fresh plan.
→ `TestWatchStaleNoticesWhatNoRootCovers`,
`TestMaintenanceRestartsAServerThatMissedAnUncoveredChange`,
`TestRememberWatchForgetsAPlanWithNothingUncovered`,
`TestRememberWatchKeepsTheFirstSnapshotOfAServer`,
`TestWatchChangedIgnoresASnapshotFromAnotherServer`,
`TestRememberWatchPinsAServerThatHadNothingUncovered`,
`TestModuleFileMtimeIsComparedWithThePreciseStart`

S2. S1 holds **inside the handshake window too**. gopls asks for its roots as
soon as it has seen the capability in `initialize`, which is before the steady
-state reader exists.
→ `TestRootsAskedDuringHandshakeIsAnsweredLocally`

S3. **The bridge adds the roots capability to every valid `initialize` request
that omits it.** Existing capabilities and roots settings are preserved. This
makes S1/S2 independent of optional client behaviour; malformed parameters
remain gopls' error to report.

Names are case-sensitive under JSON v2: only `capabilities` and `roots` are
interpreted. Differently cased names remain opaque fields and cannot suppress
the canonical capability this bridge adds. Duplicate names and invalid UTF-8
are refused at the transport boundary.
→ `TestWithRootsCapability`, `TestRootlessInitializeIsForwardedWithWorkingRoots`,
  `FuzzWithRootsCapability`

S4. **Every other server-initiated request is answered with
`CodeMethodNotFound`** on the connection it arrived on, and is not forwarded.
→ `TestUnroutableUpstreamRequestIsRefusedNotForwarded`

S4a. Server-request replies have a 30 s ceiling, shortened by any existing
handshake deadline. A failed reply closes the connection and explicitly fails
its outstanding calls. The next call can reconnect using ordinary lane recovery.
Production planning shares this deadline through the owner's single filesystem worker.
An uncancellable syscall may occupy that worker after expiry, but reconnects
do not create replacement workers. Expired plans cannot publish snapshots,
and an older plan cannot replace a newer generation's snapshot. Connection
closure cancels its queued roots work even when its reader is backpressured.
The owner serializes roots replies per generation. Encoding failures and proven
pre-delivery rejections publish no watcher view. An uncertain write preserves
the first possible view and stays stale until generation retirement.
→ `TestServerReplyFailureCompletesOutstanding`,
`TestServerReplyKeepsShorterHandshakeDeadline`
→ `TestRootsPlanningHonoursBudget`, `TestRootsWorkersStayBoundedAcrossTimeouts`,
`TestExpiredRootsPlanCannotRememberSnapshot`,
`TestLateRootsPlanCannotReplaceNewerGeneration`,
`TestClosedConnectionCannotStartObsoleteRootsPlan`,
`TestRootsSnapshotTracksDeliveryCertainty`,
`TestRootsSnapshotCannotCrossGenerationReplacement`,
`TestInvalidRootNameCannotPublishWatcherSnapshot`,
`TestRootsFirstViewSurvivesAcknowledgementOrder`,
`TestWaitingRootsReplyCanCancelWithoutReleasingAnotherReply`,
`TestUncertainRootsStayStaleThroughLaterSuccessfulReplies`

S5. S1 and S4 make the upstream reader a **second writer** on a connection its
lane also writes to, and the steady-state reader answers `roots/list` from a
worker goroutine of its own, so that S1's walk of the worktree stalls no
response behind it. The cancellation control worker is another writer. `mcp.Connection` documents `Write` as safe to call
concurrently and `Close` as safe alongside a blocked `Read`, and both
implementations in use honour it: `sseClientConn.Write` shares nothing but an
`*http.Client` and a mutex-guarded closed flag, and `ioConn.Write` serialises on
its own `writeMu`.
→ `TestSlowRootsPlanDoesNotBlockResponses`

There is never a second *reader*, which nothing promises: a connection is read
by its handshake until that finishes, and only then does `cache` start the
steady-state reader — the same ordering F5a needs for the retry.

Rationale for S1/S2: gopls' MCP mode runs a full in-process LSP session with an
fsnotify watcher, but it watches only the roots the client reports. Forwarded,
every upstream would be told about the single tree the session opened in, and
every other worktree would answer "no package metadata" for files on disk.
Rationale for S1a/S1b: on darwin that watcher holds a descriptor per entry of
every watched directory, Go or not, and headless `gopls mcp` accepts no
settings, so the roots are the only lever on what it watches. gopls never stops
watching a root once told it, so a server keeps the roots it started with until
it exits.

## 5. Failure semantics

F1. **An upstream dying never ends the session**, not even home. A proven retired
cached connection is replaced before caller delivery. Dial/private-handshake
failures may retry within the original bounded budget. After an actual caller
Write fails uncertainly, the caller gets an explicit "may have executed" error,
the connection closes and the operation credit remains. No transparent replay is
permitted. Proven size/pre-delivery rejection releases its reservation. Explicit
client retries can still duplicate a mutation; no end-to-end at-most-once claim.
→ `TestHomeUpstreamEOFDoesNotCloseClient`,
`TestSupervisorUncertainPostDoesNotReplayExecutedTool`

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
An uncertain failed write and that connection's reader seeing death are one
event seen twice. Neither may revive or replay the completed local call.
A reader that loses the claim **drops** the answer it read, because
the id it names has already been answered by whoever won.
→ `TestSendLeavesACallItsDyingUpstreamAlreadyFailed`,
  `TestUpstreamAnswerToAnAlreadyFailedCallIsDropped`

Queued deliveries, operation reservation, local delivery errors
and execution-timer startup carry the original call generation. An old queued
entry or returning Write cannot alter a newer call that reused its id.
→ `TestDeliveryCompletionCannotTouchReusedID`,
`TestCancelledQueueEntryCannotClaimReusedID`

F5a. **A lane's connection always has a reader**, because `cache` is the only
thing that sets `lane.conn` and it starts the reader in the same step. It is
called only once the connection has taken a successful write. A failed first
caller write is handled on the lane's goroutine and never replayed. The converse
does not hold: the failure
path clears `lane.conn` while its reader is still winding down.
→ `TestSendDoesNotReplayAnUncertainInitialize`

F6. The stdio client going away or the session context being cancelled stops
the bridge. `io.EOF`, `context.Canceled` and `ErrConnectionClosed` exit zero.
Shutdown cancels delivery, closes lane queues and waits for lanes to finish
owned failed-child cleanup (P2a).

## 6. Port map file

Path `~/.local/share/gopls-ports.map`, mode `0600`, one JSON object per line:

```json
{"Worktree":"/Users/x/src/repo","Port":62001,"PID":4242,"StartedAt":1754218800,"StartedAtMilli":1754218800417}
```

M1. Written atomically: temp file in the same directory, `fsync`, `rename`.
Mode is set explicitly, because `CreateTemp`'s `0600` is still subject to umask
and an exotic umask could leave the file unreadable to its own writer.

M2. Owner registry transactions use an exclusive flock on `<map>.lock`, with
cancellable nonblocking acquisition. The owner lifetime lock excludes competing
owners. Port reservation, spawn and publication have distinct phases; no network
readiness wait holds either the owner mutex or registry transaction lock.
Unchanged transactions skip replacement; blank formatting may be normalised.
Malformed nonblank records fail without mutation.
→ `TestWithRecordsWritesOnlyWhenTheFileWouldChange`, `FuzzReadMap`,
`TestReadMapRefusesUncertainProcessReservations`

M3. **Every malformed nonblank or invalid-field record fails the complete read.**
No partial records or rewrite are allowed: the line may reserve a live process.
Blank lines are permitted but consume input budget. Errors name the file without
including contents. Explicit operator repair is required.
→ `TestReadMapRefusesUncertainProcessReservations`

Reads also fail above 4 MiB total, 4001 lines or scanner's 64 KiB token limit.
Writers reject more than 4001 records, 4 MiB total or an encoded record above
65535 bytes before rename, preserving the previous file. Nonregular/symlink state
is refused using nonblocking descriptor-checked reads.

M4. Fields are validated, not merely decoded: Worktree is nonempty, PID is
between 1 and 2^31−1, and Port is within the allocation range. Valid metadata never
authorises PID-based signalling or adoption. Owner/generation identity and the
guardian's owned handles remain authoritative.
→ `TestReadMapRefusesUncertainProcessReservations`

M4a. StartedAt is unix seconds; absent values remain zero for legacy decoding.
The owner uses readiness and monotonic completion time for retirement, with no
legacy start-grace or popularity eviction policy.

M4b. `StartedAtMilli` is the same instant in unix milliseconds, written beside
`StartedAt` from one clock reading and omitted when zero. Only S1b reads it, to
compare module-file mtimes with the start. Legacy connection identity remains in
whole seconds, so an older manager dropping the
field changes none of them. It is used only when
`StartedAtMilli / 1000 == StartedAt`: a map written by an older manager has none,
and a hand-edited one may contradict the seconds, and both fall back to
`StartedAt`. A line without it still parses.
→ `TestStartedPrecisely`, `TestMapKeepsTheMillisecondStart`

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

Only the persistent owner makes admission and retirement decisions. Quiet
attachments do not pin a generation; queued work, writes and protected remote
operations do. Idle/stale-worktree retirement rechecks eligibility under the
owner mutex, persists the matching generation as terminating, then asks its
guardian to signal. Failed persistence, uncertain observation and ignored TERM
retain the generation and its descriptor/slot reservations (O2–O5).
→ `TestSupervisorIdleCandidateRechecksLastCompletion`,
`TestMaintenanceRestartsAServerThatMissedAnUncoveredChange`,
`TestMaintenanceSparesUncertainWatchMetadata`,
`TestSupervisorGuardianLossRetainsCapacityThroughStorageFailure`

`list` contacts the owner for maintenance. Without an owner, an empty registry
prints the normal header; any retained records fail with offline-cleanup
instructions. There is no legacy PID-based signalling, advisory pending-file
publication, or sampled LFU eviction. Stop old writers and reconcile legacy
processes explicitly before migration. `status` remains observational.

Readiness probes bypass proxies and redirects. Process-command and lsof output
are bounded diagnostic observations, never authority to signal or refund slots.
Malformed census output, including ferr/fNOFD, is rejected completely.
→ `TestProbeDoesNotFollowRedirects`, `TestProcessIdentityRejectsSubstringMatches`,
`TestCountOpenFilesRejectsMalformedSnapshot`, `TestParseOpenFilesRejectsDescriptorErrors`

## 9. Spawning

P1. The owner reserves one generation and immutable FD quota, selects a port
excluding both persisted records and concurrent starts, then starts its guardian.
The guardian persists a manifest and inherits a stable lifetime lease before any
child spawn. Registry publication and readiness must succeed before delivery.
An owner-death pipe triggers autonomous guardian cleanup across an owner crash,
including the spawn-before-registry window. Confirmation requires direct-child
reaping, fresh anchored-group exit observation and actual guardian exit. An
unstarted failure can refund only after released lifetime-lease reconciliation
proves no child started. Recovery refuses unresolved manifests or busy leases;
age and PID guesses never authorize reuse. There is one startup implementation;
frontends never spawn gopls directly.

P2. Readiness polls for up to ten seconds, backing off from 10ms to100ms. Spawn
and readiness run outside the owner mutex and readiness runs outside the registry
lock. Concurrent callers for one worktree await the same generation. Cancelling
a waiter cannot cancel a shared start or signal another callers process.
→ `TestReadinessProbeHonoursCancellation`,
`TestSupervisorTwoBridgesAndHTTPShareAndReconnectAfterIdle`

P2a. A successfully spawned generation that fails registry publication or
readiness is cleaned through its guardian before that start returns: TERM,
two seconds for cleanup proof, then KILL and another two seconds. Ordinary
retirement never escalates. Failed cleanup retains the reservation. A partial
guardian start retains its typed error and lease/manifest proof obligation, even
when no child PID was learned. Reconciliation also ends its owner observer.
→ `TestAuditRealGoplsFailedStartReapsChild`,
`TestGuardianPartialStartReturnsRetainedCapacityContract`

P3. Only confirmed child/group/guardian exit releases unresolved operations.
Failed durable registry cleanup still retains the generation and FD capacity;
maintenance retries it. A stale completion cannot alter a replacement. Corrupt
storage cannot reach startup or silently discard reservations.
→ `TestSupervisorGuardianLossRetainsCapacityThroughStorageFailure`,
`TestSupervisorCorruptRegistryNeverLosesReservations`

P4. Guardian pipes feed the bounded owner logger. Log paths use a worktree hash,
not user path components. Offline trim-logs retains its explicit bounded scan and
in-place truncation behavior; it must not race the active owners managed files.
→ `FuzzLogPath`, `TestLogTrimPreservesInheritedAppendDescriptor`,
`TestLogTrimIgnoresUnrelatedFilesAndSymlinks`, `TestRunTrimLogs`

P5. Managed gopls sidecar telemetry is suppressed through a pinned dependency
contract. The selected per-worktree Go toolchain must report the exact private
telemetry-off directory before startup and each delivery. Incompatible contracts
fail explicitly without changing global telemetry, HOME or GOTOOLCHAIN.

## 10. Known limits

- Production roots planning and path resolution share one owner-wide filesystem
  worker. Shutdown does not wait for an uncancellable filesystem call, and session
  churn cannot create replacements. Direct test seams retain router-local workers.

- Routing waits are bounded even if the OS stalls a filesystem syscall, but the
  single resolver worker can remain occupied until that syscall returns.
  Session shutdown does not wait for that worker. Other filesystem operations,
  including registry reads, fsync and rename, are not context-interruptible.
- The shared 64-message output queue fails the session when full, including
  terminal replies, upstream failures and unsolicited handshake messages.
  Completed responses release operation credits before output, so parking
  their readers here would otherwise allow reconnects to retain unbounded
  readers and decoded results. A transient burst that fills the queue also
  fails the session. One stdout writer already blocked in an OS write can
  remain until the client reads or the CLI process exits; shutdown does not
  wait for that writer.
  → `TestClosedConnectionReleasesBackpressuredReader`,
  `TestFullOutputQueueReleasesTerminalProducers`,
  `TestOutputSaturationStopsSessionWithBlockedWriter`
- Periodic and final session metrics share one output worker. Final snapshots
  follow request cleanup and get up to 100 ms to flush before shutdown returns;
  direct test seams can retain one blocked stderr worker. Production owner
  diagnostics use the bounded nonblocking logger; its failure fences delivery.
  HTTP fatal errors cancel the backend before diagnostic output.
  → `TestHTTPErrorCancelsBeforeDiagnosticWrite`,
  `TestMetricsShutdownDoesNotWaitForStderr`, `TestMetricsFinalSnapshotFollowsCleanup`
- With execution deadlines disabled, an accepted request can wait indefinitely;
  outstanding limits bound retained requests, not execution duration.
  Cancellation or timeout cannot prove that a gopls mutation stopped.
- Managed generations and FD reservations are capped, but shared-process RAM is
  not. O2–O5 define production idle/stale retirement and active-work protection.
  Retained lane counts compose beneath the owner session cap; see the resource ledger below.
- Message and frame limits bound wire data, not total heap or decoded-object
  overhead. Queued decoded results and GC high-water allocations remain outside
  the HTTP and SSE byte budgets. Roots planning has separate entry/path/depth,
  filter and metadata limits and fails on incomplete observations. Managed log
  retention has aggregate file/byte limits; unrelated files are outside its scope.
- A terminating process that ignores SIGTERM remains recorded and blocks ensure
  for its worktree. This preserves operator visibility. Owned failed children
  have bounded escalation. Production signals delegate to guardian-owned handles.
- Loopback endpoints are unauthenticated and assume trusted local clients and
  listeners. Bind-and-close port selection does not authenticate the eventual
  listener to the recorded child. Diagnostic command-line matching does not
  protect against a local actor spoofing process arguments.
  Private per-user state directories and their existing ancestors are trusted.
- Old manager binaries ignore Terminating. Upgrade all managers sharing the map
  before relying on termination retention; mixed-version safety is not promised.
- Worktree paths must be valid UTF-8 to round-trip through JSON.
  → `FuzzMapRoundTrip`
- Callers sharing a failed start receive its failure. A later independent call
  can start a fresh generation after confirmed exit and durable cleanup.

## 11. Configuration and observation

Environment variables are parsed when constructing the manager. The
[configuration table](README.md#limits-and-measurements) lists their defaults and
meanings; resource ownership and release conditions are in the ledger below.

Counts must be positive integers. Execution timeout must be a nonnegative Go
duration. Manager metrics report lock wait/hold and readiness and ensure
durations. Session snapshots every 30 s and at shutdown report request counts,
fixed rejection reasons, cancellation/expiry counts, retained lanes and memo
sizes, path-memo hits/misses, expiry epochs and capacity rollovers, unresolved and
abandoned operations, current/peak SSE frame capacity, and count/total/max
nanoseconds for routing, resolution, queue waits and handshake.
Snapshots copy mutable counters under their owning locks. Completed includes
local termination and shutdown cleanup, not just successful upstream answers.
→ `TestFromEnvRejectsInvalidSettings`, `TestFromEnvOverrides`, `TestWithDefaults`,
`TestAccountingTerminalPathsAndSnapshots`

The status command is read-only, emits JSON and never invokes a sweep. It reports
recorded server count, registry integrity, termination state and RSS in KiB when
one bounded ps snapshot matches the recorded gopls identity, and lsof rows
(`OpenFiles`, with their total beside the budget) under the same match. A failed
ps snapshot is discarded in full; missing measurements are null. Active
client count for legacy records remains null. The separate Owner snapshot gives
exact bounded generation/attachment/operation state when the owner is reachable;
an unavailable owner reports an error rather than an invented zero.
Log observations include per-record sizes and aggregate managed-log bytes,
file count, largest size and count above the trim threshold.
→ `TestStatusMeasuresWithoutChangingRegistry`,
`TestStatusRejectsFailedProcessSnapshot`

Stdio uses a direct, case-sensitive parser for complete scalar messages; the
pinned SDK still handles writes, batch correlation and fallback validation.
The stdio adapter excludes notifications from the SDK's unresolved batch-response
set while preserving original message order. Notification-only batches produce
no response and retain no response ownership; mixed batches aggregate only
ID-bearing requests.
Raw fields are owned copies. SSE uses bounded framing with the SDK JSON-RPC codec.
Stdio decoding enforces a per-value byte budget including preceding whitespace,
accounting for read-ahead already buffered by the decoder. Oversized or incomplete
values fail before unbounded buffering.
→ `TestStdioMessageLimitAndReadAhead`
→ `TestStdioMatchesSDK`, `TestStdioPreservesBatchResponses`,
`TestStdioCloseUnblocksRead`, `FuzzDecodeScalarMatchesSDK`
Batch cases cover requests only, mixed notifications, and notifications only.
→ `FuzzStdioNotificationBatches`

Complete upstream SSE events, including framing fields and comments, have the
same per-message ceiling. Framing reserves allocated capacity from the shared
SSE budget before allocation (both old and replacement buffers during growth).
Exhaustion closes the connection rather than parking partially assembled events
waiting for each other. Each reader has one bounded frame and an unbuffered
handoff, plus a fixed 4 KiB bufio reader; there is no 100-event SDK queue.
Multiline data is compacted in place before SDK decoding. Close releases any
frame blocked at the handoff. Oversize events or invalid JSON-RPC close the
connection and fail its pending requests; oversize POSTs fail before HTTP Write.
The byte budget excludes decoded messages, network buffers and garbage awaiting
collection. It is not a process heap limit.
Upstream SSE connects only to numeric HTTP loopback addresses. Endpoint events
must retain the initial scheme and host/port, without credentials or fragments.
The dedicated client bypasses environment proxies and refuses GET/POST redirects.
→ `TestSSEFrameBoundsAndOwnership`, `TestSSEReadAheadAndSlowConsumerClose`,
`TestSSEOversizeBeforePost`
→ `TestSSERejectsEndpointEscape`, `TestSSERejectsRedirects`, `FuzzSSEFrameBudget`

Implementation remains one Go module. `internal/config` owns shared limits,
defaults and environment validation. `internal/transport` owns bounded stdio/SSE
connections, codecs and frame-budget accounting, and exposes connection constructors
plus synchronized budget observations. `internal/protocol` owns pure JSON
adaptations for roots capabilities, physical path arguments and complete results.
Internal packages do not depend on the root command. Routing, resolver/path
logic, lanes, request accounting, upstream request handling, registry, process
lifecycle and probes remain in the root package. requests.go owns terminal request
completion and generation checks; registry.go owns persistent mutation. Leaf tests
move with their packages, while the root retains integration and lifecycle tests.

## Stateless HTTP frontend

`http [address [path]]` exposes `/mcp` using the SDK's stateless Streamable HTTP
handler with JSON responses. Defaults are `127.0.0.1:6099` and home `.`; only
loopback listeners are accepted. The public listener validates the original Host
as a numeric loopback address or `localhost` before any proxy rewriting. Go's
cross-origin protection rejects unsafe browser requests using Origin and
Sec-Fetch-Site before forwarding or allocating an HTTP backend; failures return
403 with the normal bounded write deadline. Nonbrowser clients without those
headers remain supported. These checks do not authenticate arbitrary local
processes. Existing stdio behavior remains available.
→ `TestSupervisorHTTPRejectsUntrustedBrowserRequests`, `TestLoopbackHTTPProtection`

The HTTP frontend terminates protocol negotiation locally and exposes gopls
tools through `tools/list` and `tools/call`. Home initialization supplies gopls's
instructions. A process-wide router owns bounded legacy SSE lanes, roots replies,
private handshakes and internal request accounting. No frontend session identifier
is issued or used. Internal request identifiers are unique across HTTP clients.

Unlike stdio, HTTP routing never reads or writes sticky state. A pathless call
always goes home; absolute path arguments retain the existing worktree resolution
and multi-worktree rejection rules. Limits and caches are shared across requests,
and cold resolution uses a separate bounded 64-message queue. One coordinator
waits for the existing filesystem worker while the routing owner delivers ready
requests. Warm/pathless requests therefore bypass stalled resolution without
changing stdio ordering or growing filesystem workers.
→ `TestHTTPWarmCallsBypassBlockedResolution`

Limits and caches are not recreated per POST. HTTP request cancellation completes the internal call and
sends advisory cancellation to its owning SSE connection. Cross-request cancellation
notifications and unsolicited upstream notifications are not relayed.

HTTP exchanges reserve the configured maximum body size until completion.
Capacity is the smaller of the session request limit and HTTP body budget divided
by maximum message size (16 by default); excess exchanges receive 503.
This applies before reading bodies, including slow and chunked requests.
→ `TestHTTPBodyBudgetIncludesSlowReaders`
→ `TestHTTPRejectsOversizeAndReleasesReservation`

Answering `tools/list` and `tools/call` in the middleware, rather than through
the SDK's own handlers, skips the result defaults those handlers apply, and two
of them are required on the wire. `cacheScope` has no `omitempty`, so an unset
one ships as `""` and fails a client's `public`/`private` enum. `resultType`
lives in an unexported field of `CallToolResult`, which — like the other
multi-round-trip results — carries none of the marker the SDK's own defaulting
matches on, so nothing sets it and `omitempty` drops it; a client on protocol
revision 2026-07-28 rejects a tool call without it. Unmarshalling is the only
door into that field, so a missing value is spliced into the payload ahead of its
keys. An existing `resultType` is preserved, including escaped key spellings;
no duplicate key is inserted. Both
defaults are the value an absent field means, `"public"` and `"complete"`. A
case added to that middleware has to answer the same question for its result
type.
→ `TestHTTPProtocolAndErrors`, `TestWithCompleteResultType`

HTTP has configurable 4 MiB request bodies, 5 s header reads, 30 s body reads, and a 30 s response-write
budget starting when response output begins. The SDK owns HTTP validation and
stateless request lifecycle; the upstream protocol remains legacy SSE, with
bounded framing in the manager.

Covered by `TestHTTPStatelessRoutingAndSSEReuse`, `TestHTTPProtocolAndErrors`,
`TestHTTPCurrentSDKClient`, `TestHTTPDisconnectCancelsLegacySSECall`, and
`TestHTTPBackendShutdownReleasesCalls`.

## JSON processing

All project-owned serialization uses `encoding/json/v2`; streaming framing uses
`encoding/json/jsontext`, and opaque payloads use `jsontext.Value`. There are no
v1 compatibility options. Names are case-sensitive: routing reads only `arguments`,
`file`, `dir`, and `files`; cancellation reads only `requestId`. Unknown fields
remain allowed. Duplicate names and invalid UTF-8 are rejected. HTTP validates
bodies before SDK negotiation (invalid JSON receives 400); stdio rejects the
message, and malformed upstream SSE JSON closes the connection. Byte limits and
capacity reservations still apply before parsing.
→ `TestPathArgumentsUseStrictJSON`, `TestStdioRejectsAmbiguousJSON`,
`TestHTTPRejectsAmbiguousJSON`, `TestSSEReadContracts`

Protocol adapters rewrite only exact names and preserve opaque fields. They
return malformed input unchanged for the owning parser to reject. Result defaults
never introduce duplicate names. Registry JSON uses the existing capitalized
field names; differently cased manual entries no longer match those fields.
Zero-valued optional record fields use `omitzero`. Nil maps and slices use v2's
empty-object/empty-array defaults. SDK encoding remains an upstream dependency;
the HTTP adapter explicitly retains an empty tool arguments object.

## Resource ownership and evidence ledger

Audit baseline `0437e95`; implementation uses Go1.27.4/gopls0.23.0 on Darwin and
Linux. This inventory records bounded resources and exercised failure paths. It
does not prove absence of all bugs. Formal lifecycle invariants, runtime tests,
fuzzing and long-session measurements are separate evidence layers.

### Findings and response

Audit findings R1–R9 concern socket admission, removed pending-file state,
registry bounds, helper output, watch planning, log retention, FD reservations,
persistent idle authority and guardian recovery, respectively. Their current
contracts and residual limits are in the resource ledger below; normative
invariants above and [formal runtime conformance](formal/README.md#runtime-conformance) name the runtime evidence.

Cancellation rejects fractional/out-of-range decoded numeric IDs before SDK
conversion; `testdata/fuzz/FuzzCancelTarget/` retains the minimized counterexample.
Regressions cover both signs, zero and range boundaries. SDK float64 precision
remains a compatibility boundary. Uncertain caller Writes never replay and keep
credits until response/confirmed exit (F1); independent retries may still duplicate
execution. Stale transports reconnect before the first fresh call.

Managed log paths refuse symlinks, hardlinks and aliases; queue/storage failure
fences owner delivery. Original counterexamples remain in Git history.

### HTTP browser boundary

The [HTTP contract](#stateless-http-frontend) requires original loopback Host and
cross-origin checks before proxy rewriting/backend admission. Regressions verify
403 with the normal write deadline, no backend allocation/completion-clock change,
and supported IPv4/IPv6, localhost, same-origin and nonbrowser clients. Local
process authentication and the trusted-project/OS boundaries remain unchanged.

### Resource ledger

All defaults are configurable as documented in README/SPEC. Input, wire-capacity
and object-count limits are not byte-exact heap or RSS guarantees.

| Resource | Owner/scope; admission cap | Release/evidence | Residual limit |
| --- | --- | --- | --- |
| Frontends/backend cache | One persistent owner;16 combined attachments and retained HTTP backends, including failed/closing entries until active handlers and actual teardown finish | Socket EOF/close; idle or failed-backend retirement independently of recent retries; one teardown owner rejects closing-entry reuse; protocol/config mismatch refuses before allocation; real backend-failure/reinit and blocked-teardown synctest regressions | Public frontend processes and their bounded sockets remain local OS processes; clients are trusted local callers |
| Generations | Owner map;64 including starting/draining/uncertain/exited-but-storage-pending | Guardian child/group/actual guardian exit plus durable registry removal; never signal/EOF alone | Ignored TERM or uncertain cleanup intentionally consumes capacity; operator repair may be needed |
| Global operations | Owner map;1024 combined queued, caller, private handshake and server-control identities | Definitive response or proven pre-delivery rejection; generation teardown clears unknown work | Long active work has no default execution deadline; abandoned grace permits whole-generation recovery, not a false completion |
| Retained payloads | Shared64MiB maximum-message reservations covering ingress/delivery, output and cancellation advisories; string IDs≤1024bytes, retained initialize≤64KiB | Queue consumption/discard or actual Write return; cancellation does not refund queued objects; blocked-writer/churn synctest regressions | Reservation is conservative payload capacity, not decoded-object/GC/serialization heap. Transient readers remain bounded by session/lane/frame/socket products |
| Router queues/lanes |≤16 retained owner frontend slots;64 ingress/delivery/control/output entries and64 lanes per router;128/worktree and1024 session request caps | Lane shutdown drains payloads, contexts close transports, unknown owner credits survive router teardown | Small queue/control structures and transient decoded objects compose beneath explicit count caps |
| Memos | Router;4096 path and4096 directory entries, shared lazy5m epoch | Capacity rollover/expiry/router retirement | Path spelling bytes inherit bounded messages; idle heap may await GC |
| Filesystem execution | One unbuffered owner-wide worker | Context cancels caller; one active uninterruptible syscall may remain | No replacement workers across session churn. Kernel filesystem calls are not universally cancellable |
| Output/diagnostics | Socket writes bounded by context/deadlines/Close; owner diagnostics use bounded logger | Actual return/teardown; full terminal output fails session, never silent usable-session loss | One blocked synthetic/direct-test output can remain; its payload stays globally charged |
| HTTP body/exchanges | Owner-global min(session,64MiB/4MiB)=16 before handler body decode | Exchange return, including slow response writes;16KiB headers,5s header/30s read/30s response-write ceilings | SDK/result decoded objects and framing scratch are additional bounded products, not exact byte accounting |
| HTTP sockets |128 owner accepted connections plus one transient excess; frontend listener also bounded | Close/idle60s/header deadline; accepted excess closed before new reader goroutine | Local frontend process multiplication is outside owner process FD quota but active owner attachments remain capped |
| SSE wire buffers | One owner-shared64MiB capacity budget;4MiB complete event/POST | Transport frame release/Close; SSE lifecycle/budget tests | Completed decoded results leave wire budget and enter retained-message budget; decoder/GC peaks are additional |
| FD reservations | Owner transport/control formula plus per-generation watched-tree count+256 child headroom+64 guardian/observer | Only clean unstarted failure or confirmed retirement refunds; kernel RLIMIT_NOFILE enforced in owner/child | Does not constrain every arbitrary project descendant, RSS or kernel watch bytes. Budget exhaustion fails admission |
| Watch plans | Owner manager;≤generation cap keys;100k entries/32MiB names+paths/depth128/4096roots/16384partials; metadata256files/16MiB; filters1024/1MiB | Replaced per generation and pruned on confirmed exit; incomplete plan never published | Worktree mutation can invalidate snapshot; uncertainty preserves generation until a complete verdict |
| Registry | One read/temp descriptor per operation;4MiB/4001lines/64KiB scanner token | Close/unlink/atomic rename; failure preserves full prior contents, no probe/signal from uncertain rows | fsync/rename can block; crash can leave temp inode; private ancestor directories trusted |
| Lifetime/action locks/manifests | Stable per-port namespaces over4001ports; bounded manifest recovery scan | Descriptors close; lock inodes intentionally retained; manifest removed only on proof | Historical pre-upgrade lock/temp files need explicit offline cleanup; no PID/age adoption fallback |
| Logs | One process-global worker;128 jobs/≤1MiB allocated queued+active payload; owner64MiB disk and≤server cap+1 files | Bounded retention, serialized append, confirmed generation release, one terminal failure channel | Snapshot counters approximate active append; external mutation fails detection on touched files. Sole managed writer required |
| Usage/pending mirrors | Removed | Owner operation accounting replaces advisory activity files and sampled LFU | Old writers must be stopped before migration |
| Helper commands | Bounded stdout64KiB/4MiB, stderr64KiB, derived deadline and250ms pipe wait delay | Cancel/reap direct child on overflow/error; no partial snapshots | Arbitrary escaped helper grandchildren need OS containment; direct-child proof is not a universal process-tree proof |
| Telemetry | Managed gopls pinned contract suppresses sidecar; shared private mode-off metadata | Checked selected Go reports off/exact directory before spawn and delivery; actual Go/gopls integration | Internal compatibility contracts are explicit and fail on mismatch; external caches/global user telemetry untouched |

### Evidence and remaining work

The registry churn suite observes `/dev/fd` across256 failed registry cycles,
64 cancelled lock waits in private state. Pending-file churn coverage was retired
with that unreachable subsystem; owner startup failure and lease-reconciliation
regressions cover the remaining reservation lifecycle.
The boundary/error suite covers FIFO/symlink/nonregular/corrupt input and verifies
no eviction signal on uncertain activity. Supervisor pure concurrency/idle tests
use testing/synctest with in-memory storage; real socket/process tests stay outside
fake-time bubbles. Formal artifacts live in formal/ and are checked separately.

Audit evidence is saved outside the repository under
`~/.codex/artifacts/gopls-audit/` (resources, guardian, logging, reviews and owner
logs). Native Linux/macOS evidence and owner runtime coverage must accompany the
final audit report. Default-duration zero-client idle checks and24h/72h soaks are
still pending this implementation checkpoint; no long-soak conclusion is claimed.
Arbitrary trusted-project escaped subprocesses, external toolchain/cache growth
and universal POSIX descendant containment remain explicit OS boundary assumptions.
