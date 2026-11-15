# gopls-mcp-manager — behavioural spec

Normative guarantees for the manager and its stdio/HTTP frontends. Invariant IDs
are stable; cited tests pin their behaviour. Defaults are listed in the
[configuration table](README.md#limits-and-measurements); ownership, release
conditions and residual bounds are in the [resource ledger](#resource-ledger).

## 1. Roles

```
MCP client → stdio/HTTP frontend → private Unix owner → SSE/HTTP → guarded gopls
```

- **home**: startup worktree and first upstream.
- **upstream**: a connection to one worktree's gopls.
- **sticky**: the last worktree resolved from an absolute tool argument; stdio only.

### Persistent owner contract

O1. **Singleton authority.** One persistent owner hosts routers, HTTP backends
and every upstream Write; frontends have no direct fallback. Its lifetime
registry lock excludes other owners. A versioned private Unix endpoint and
configuration fingerprint reject incompatible clients before allocation.
Canonical registry identity selects a short `/tmp` socket, independent of
TMPDIR/HOME length.
→ `TestSupervisorSocketUsesShortStablePrivateIdentity`,
`TestSupervisorProtocolAndConfigMismatchFailBeforeAllocation`

O2. **Idle retirement.** One mutex serializes admission, target binding and drain
eligibility. Queued, active and protected abandoned work prevents retirement;
quiet attachments do not. Readiness starts a monotonic completion clock and
definitive responses advance it. The positive default idle period is one hour,
sampled every 30 s and rechecked when draining. Cancellation/disconnect does not
complete remote work: abandoned operations retain capacity through a separate
idle grace and controlled generation retirement.
→ `TestSupervisorIdleBoundaryAndQuietConnections`,
`TestSupervisorQueuedAndActiveWorkSurviveIdle`,
`TestSupervisorAbandonedOperationsRetireOnlyAfterGrace`,
`TestSupervisorIdleCandidateRechecksLastCompletion`

O3. **Exit evidence.** Drain fences delivery before guardian-owned handles signal
the anchored process group. Ordinary retirement waits ten seconds after SIGTERM
and never escalates to SIGKILL. Uncertain starts, ignored TERM, guardian loss or
storage failure retain discoverability and capacity until confirmed
child/group/guardian exit. Durable pre-fork manifests and stable lifetime locks
cover owner-crash/restart windows; legacy records prevent migration.
PID disappearance, an empty/incomplete census, or missing initial ancestry is
not exit proof. A valid census includes the observer in its actual process group;
later observations cannot erase missing initial ancestry evidence.
→ `TestGuardianSnapshotRequiresObserverIdentity`,
`TestGuardianFailedInitialObservationRetainsUncertainty`

O4. **Global bounds.** Defaults are 16 frontend attachments/cached HTTP backends,
1024 queued/unconfirmed operations and 64 generation slots. Handshakes and
server controls share the operation cap. Routers share 64 MiB SSE capacity and
maximum-body HTTP reservations (16 exchanges by default); their bounded queues,
lanes and memos compose beneath the frontend cap. One owner-wide filesystem
worker survives session churn; one process-global log worker has bounded queue
and retention, and queue/storage failure fences delivery.

Ingress/delivery, output and advisory payloads share 64 MiB maximum-message
reservations, released on consumption, discard or actual Write return. Local
cancellation never refunds a queued object. String IDs are ≤1024 bytes and the
retained initialize template ≤64 KiB. These are capacity bounds, excluding
transient decoding/serialization/GC overhead. Exhaustion fails admission/output;
advisory overload still completes local cancellation immediately.
→ `TestSupervisorGlobalAdmissionDoesNotMultiplyWithRouters`,
`TestSupervisorSharedFilesystemWorkerSurvivesSessionChurnBoundedly`

O5. **Immutable FD quota.** Complete watched-tree admission reserves child quota
plus guardian/control headroom before spawn; RLIMIT_NOFILE enforces child/owner
quotas. Victims stay charged until confirmed exit. Every later roots reply must
fit the original child quota before publication or Write. Expansion fails and
requires a new generation, preserving the prior snapshot and active/unknown-work
protection. These limits do not bound RSS, arbitrary project subprocess fanout,
external caches or all kernel watcher memory. Darwin process groups cannot prove
containment of arbitrary escaped descendants. Unsupported selected Go/gopls
telemetry contracts fail explicitly (P5).
→ `TestSupervisorExpandedRootsRefuseBeforePublicationAndPreserveRetirement`,
`TestSupervisorSameAndSmallerRootsFitImmutableQuota`

O6. **Recovery.** The owner survives zero clients. After proven idle retirement,
the first normal call reconnects before delivery; uncertain caller Writes never
replay (F1). Legacy reservations are neither adopted nor signalled automatically:
nonempty offline registries require operator reconciliation. See section 9.
→ `TestSupervisorTwoBridgesAndHTTPShareAndReconnectAfterIdle`

O7. **Separate lifetimes.** Routers require an explicit owner and retain local
waiters/timers only. The owner alone records remote operations and session/
worktree counts under its mutex, using connection/admission identities without
retaining routers, timers, payloads, callbacks or HTTP writers. One upstream type
owns transport, generation identity and cancellation lifetime. For caller
responses, remote release and matching local-waiter claim occur under the router
lock; reading a response alone cannot free its ID. Private handshake responses
complete within the upstream. Terminal completion/confirmed exit removes empty
worktree counters; disconnect abandons remote work without refunding credits.
→ `TestResponseReadCannotReleaseIDBeforeWaiterClaim`,
`TestConfirmedExitRestoresLongSessionCapacity`,
`TestUnconfirmedExitRetainsRouterOperationCredits`

## 2. Routing

R1. Only absolute `file`, `dir` and `files` arguments are routing evidence.
Relative paths and ordinary resolution failures preserve sticky/home routing.
All resolvable paths must name one worktree; mixed-worktree calls fail without
changing sticky state.
→ `TestToolCallRoutesToTheWorktreeOwningItsPath` (case "relative path"),
`TestToolCallSpanningTwoWorktreesIsRefused`

R2. Resolution never climbs past a missing immediate parent. File symlinks route
to their targets; existing/missing files under a directory symlink share its
physical directory memo key.
→ `TestWorktreePathResolvesEachPathToItsOwnWorktree`,
`TestSymlinkedDirectorySharesMemoForExistingAndMissingFiles`

R3. Pathless stdio calls use the last resolved worktree. Routing commits in client
wire order; each worktree has its own delivery lane, so one cold dial/handshake
does not block another lane.
→ `TestTargetFallsBackToTheLastPathBearingCall`,
`TestAColdWorktreeDoesNotHoldUpAnother`

R4. One 10 s routing budget covers queueing and all paths in a message. Resolution
uses one synchronized worker with an unbuffered job channel. A syscall may
outlive cancellation, but cannot create replacement workers or commit a late
sticky result. Routing timeout fails the call rather than selecting another tree.

A separate ingress reader admits calls and handles cancellation even when the
64-message routing queue is full. Pathless calls and complete path-memo hits
bypass the filesystem worker without changing wire-order routing. The 40 s
ingress deadline includes queueing, resolution and delivery; delivery also has
a 30 s ceiling after routing.
→ `TestRoutingBudgetDoesNotMultiplyOrGrowWorkers`,
`TestIngressCancellationBypassesBlockedResolution`,
`TestFullIngressQueueStillAcceptsCancellation`

R5. Successful resolutions use verbatim-path and physical-directory memos;
failures are not cached. Each defaults to 4096 entries. A new key at capacity
clears its memo; updating an existing key does not. Both levels expire together
on the lazy GOPLS_MANAGER_CACHE_TTL epoch (default 5m), checked on access/metrics.
Symlink/worktree replacement may remain stale until expiry, but directory hits
cannot renew an old epoch and slow/cancelled lookups cannot populate a new one.
Expiry bounds validity, not idle heap reclamation.
→ `TestMemoEpochExpiresBothLevels`, `TestMemoExpiryRevalidatesRetargetedSymlink`,
`TestMemoRefreshAtCapacityDoesNotEvict`, `TestWorktreeOfResolvesAndMemoizes`,
`TestMemoCapacityRevalidatesEvictedPaths`

R6. Delivery and control queues hold 64 messages each. Admission never waits:
excess requests receive -32000 and excess notifications are dropped. Queue time
uses the delivery budget; expired queued work cannot start a server.
→ `TestFullLaneRejectsWithoutBlockingOtherWorktrees`,
`TestQueuedDeliveryExpiresWithoutDialling`

R7. Cancellation completes the named request locally with -32800, releases its
outstanding slot, suppresses later responses, and cancels queued/dialling work.
Once assigned, the advisory carries the exact connection and waits for the
original Write to finish. Full control queues may drop advisories without
reversing local completion. Unknown/completed/malformed IDs are dropped;
fractional/out-of-range decoded numbers are rejected before SDK truncation.
The notification has no response; the terminal response belongs to its target.

Advisory Writes and operation placement share a cancellable lane gate. Under
that gate, stale connection/ID generations are dropped; ID reuse cannot overtake
an advisory already writing.
→ `TestQueuedCancellationPreventsDelivery`,
`TestCancellationBypassesFullDeliveryQueue`, `TestCancellationCannotOvertakeItsCall`,
`TestCancellationAfterUncertainWriteCannotReachANewConnection`,
`TestDelayedCancellationCannotCancelReusedID`, `TestCancellationWriteFencesIDReuse`,
`TestCancellationNumericIDDoesNotAliasAnotherRequest`

R8. **Local requests and remote operations are separate bounds.** Defaults are
128 outstanding requests/worktree, 1024/session and 64 retained lanes. Fast
Writes cannot bypass outstanding limits. Local completion, failure, timeout,
cancel or teardown removes local tracking; retry retains its admission slot.
Per-worktree counts update in constant time through centralized terminal paths.
Session/lane-retention rejection allocates no lane; existing lanes remain usable
at capacity and no shared server is evicted to make room.
→ `TestOutstandingLimitsSurviveFastDelivery`,
`TestRetentionLimitDoesNotAllocateRejectedLane`,
`TestOutstandingLimitDoesNotAllocateNewLane`, `TestAccountingTerminalPathsAndSnapshots`

Remote credits use the same session/worktree ceilings and are acquired atomically
before Write. Local completion, disconnect and router teardown do not prove
remote completion (O7). Only the exact response, proven pre-POST size rejection
or authoritative generation exit refunds them. A separate retry on another
connection needs another credit while prior work remains unresolved (F1).
Surviving routers reclaim confirmed-exit credits before rejecting at capacity
and when reporting usage; unconfirmed termination cannot reclaim them. An ID
cannot be reused on a connection with an unresolved prior operation. The owner
also enforces its global cap across frontend lifetimes.
→ `TestAbandonedOperationsBoundFurtherDelivery`,
`TestDisconnectedOperationsDoNotReleaseCredits`,
`TestAbandonedIDCannotBeReusedOnItsConnection`,
`TestConfirmedExitRestoresLongSessionCapacity`,
`TestUnconfirmedExitRetainsRouterOperationCredits`

R9. Execution timeout defaults to disabled. When enabled, its timer starts after
successful delivery and competes for the one terminal completion. Generation
tokens prevent late callbacks completing reused IDs. Timeout ends local waiting,
not upstream execution, and queues R7's bounded exact-connection advisory after
the original Write completes.
→ `TestExecutionTimeoutQueuesAdvisoryCancellation`, `TestExecutionDeadlineCompletesOnce`

R10. Resolved absolute path arguments are forwarded with their physical spelling;
gopls can otherwise return silently incomplete results for symlink spellings
(including Darwin `/tmp` and `$TMPDIR` paths). Substitution reads only the path
memo and cannot stall ingress. Unmemoized, unresolvable or relative paths, other
arguments and opaque message fields remain untouched.
→ `TestToolCallForwardsPhysicalPathSpelling`,
`TestDeliveredToolCallCarriesThePhysicalSpelling`

## 3. Handshake

H1. Home receives the client's `initialize` with its original ID/parameters,
except for S3's roots capability. The client sees exactly one initialize result.

H1a. Before `initialize`, the ingress reader rejects requests with
`CodeInvalidRequest` and drops notifications without waking a lane. H2a
independently guards against duplicate initialization on a connection.
→ `TestAMessageBeforeInitializeNeverWakesALane`

H2. Each later connection (another worktree or redialled home) replays the stored
initialize once under private ID `gopls-mcp-manager-init`, before other delivery.
That response is swallowed; other messages volunteered during handshake reach
the client. The owning lane provides per-connection ID uniqueness.
→ `TestSendReconnectsAndInitializesRestartedHome`

H2a. A cached lane connection is already handshaken. A later client initialize
is refused there without Write, preventing the duplicate that would make gopls
end the session, regardless of reader/lane ordering.
→ `TestTheClientInitializeIsRefusedByAnAlreadyHandshakenUpstream`

H3. An uncertain initial initialize Write fails explicitly: neither replay nor
a private replacement handshake is permitted.
→ `TestSendDoesNotReplayAnUncertainInitialize`

H4. Handshake ends with `notifications/initialized`.

H5. One delivery deadline starts at queue admission and covers lock acquisition,
readiness, connect, private handshake, roots replies, delivery and pre-delivery
retries. Its ceiling is readiness budget +20 s (30 s today); spent budget cannot
restart on retry. Owned failed-child cleanup has a separate budget (P2a).
→ `TestAWedgedUpstreamFailsItsCallWithinOneBudget`

H6. Connect has the same deadline, but the resulting SSE stream needs a
connection-lifetime context. A cancellable watchdog bounds dial, is disarmed on
return, and closes connections returned after expiry. Caller cancellation also
ends waiting for owner acquisition. Shared startup uses the owner's lifetime,
so one cancelled waiter cannot cancel another client's generation; readiness
retains its own polling bound. Closing a connection releases its context rather
than retaining failed-handshake contexts until session shutdown.
→ `TestReadinessProbeHonoursCancellation`,
`TestSupervisorCancelledWaiterCannotCancelSharedStart`,
`TestAWedgedUpstreamFailsItsCallWithinOneBudget`,
`TestADialContextLivesExactlyAsLongAsItsConnection`

H7. The same deadline bounds the delivery POST, even if a peer never completes
it. A stalled lane may fill its bounded queue but cannot block other worktrees.
After delivery, R8–R9's outstanding limits/optional execution timeout apply.

## 4. Server-initiated requests

S1. The bridge answers `roots/list` locally with that upstream's roots as
`file://` URIs and base names. With no exclusions, the root is the worktree itself.
→ `TestUpstreamRootsAnsweredWithItsOwnWorktree`

S1a. **Filtered roots.** Rules come from go.mod `ignore` directives for the
worktree and go.work modules, then GOPLS_MANAGER_DIRECTORY_FILTERS. `./dir` is
module-relative; bare `dir` matches at any depth. gopls directoryFilters semantics
apply: last match wins, rules cover subtrees, `**` matches zero or more segments,
and `+` re-includes excluded subtrees. Report the smallest covering directory
set, excluding trees the watcher skips (`.`/`_` prefixes and `testdata`); `.git`
is never watched, regardless of rules.

Metadata must resolve to regular files ≤4 MiB; file symlinks are supported.
Missing/malformed metadata contributes no filters. Other read errors (including
devices/FIFOs), size overflow or planning-budget exhaustion fail the complete
plan. Individual rules are capped at 4096 bytes before regexp compilation;
invalid decoded module paths and unusable module rules are ignored. Environment
rules must be valid UTF-8; oversize rules fail startup. Aggregate bounds are in
the ledger.
→ `TestUpstreamRootsCoverTheWorktreeMinusFilters`,
`TestPlanWatchCoversTheWorktreeMinusExcludedTrees`,
`TestPlanWatchWithoutExclusionsIsTheWorktree`,
`TestPlanWatchReincludesBelowAnExcludedTree`, `TestGitDirectoryIsNeverWatched`,
`TestPathIncludeFuncMatchesGopls`, `TestModuleIgnoreFiltersReadGoModAndGoWork`,
`TestParseDirectoryFilters`, `TestModuleMetadataReadsAreBounded`,
`FuzzDirectoryFilterPrecedence`

S1b. **Missed changes require retirement.** A partial directory has an excluded
descendant and is not itself covered by a root. File-scoped tools recheck package
mtimes for Go edits there, but can miss added/removed entries and module changes.
The manager records its Go-relevant names (included subdirectories and
`.go`/`.mod`/`.sum`/`.work`/`.s` files) at the first roots view that may reach a
server (S4a). gopls adds watches without reporting pre-existing files and never
removes roots, so later sessions cannot replace that first view.

Snapshots belong to the dialled server's PID/StartedAt. A different server
replaces the view; unknown-identity connections replace it on each answer. An
initially complete view stays empty and cannot be condemned by later answers.
Maintenance marks a live, non-terminating generation stale when partial names
change or module mtimes exceed its precise start (M4b). Foreign snapshots are
ignored; without identities, snapshots predating StartedAt are ignored. Busy work,
uncertain identity or a record changed under the probe leaves the view stale for
the next sweep. Retirement follows O2–O5/P3. Confirmed exit prunes the snapshot;
the next generation gets a fresh plan.
→ `TestWatchStaleNoticesWhatNoRootCovers`,
`TestMaintenanceRestartsAServerThatMissedAnUncoveredChange`,
`TestRememberWatchForgetsAPlanWithNothingUncovered`,
`TestRememberWatchKeepsTheFirstSnapshotOfAServer`,
`TestWatchChangedIgnoresASnapshotFromAnotherServer`,
`TestRememberWatchPinsAServerThatHadNothingUncovered`,
`TestModuleFileMtimeIsComparedWithThePreciseStart`

S2. Roots requests are answered locally during handshake too, before the
steady-state reader exists.
→ `TestRootsAskedDuringHandshakeIsAnsweredLocally`

S3. Every valid initialize gets a missing roots capability. Existing capabilities
and roots settings are preserved; malformed parameters remain gopls' error.
Only exact `capabilities`/`roots` names count: differently cased fields stay opaque
and cannot suppress the canonical capability. Transport validation rejects
duplicate names and invalid UTF-8.
→ `TestWithRootsCapability`, `TestRootlessInitializeIsForwardedWithWorkingRoots`,
`FuzzWithRootsCapability`

S4. All other server-initiated requests receive `CodeMethodNotFound` on their
originating connection and are never forwarded.
→ `TestUnroutableUpstreamRequestIsRefusedNotForwarded`

S4a. Replies have a 30 s ceiling, shortened by a handshake deadline. Failure
closes the connection and fails outstanding calls; the next call may reconnect.
Roots planning shares that deadline and the single owner filesystem worker;
uncancellable calls cannot create replacements. Expired/closed-connection work
cannot publish, and old plans cannot replace a newer generation's snapshot.
Closure cancels queued roots work even behind a backpressured reader.

The owner serializes roots replies per generation. Encoding/proven pre-delivery
failure publishes no view. Uncertain Write preserves the first possible watcher
view and stays stale until retirement.
→ `TestServerReplyFailureCompletesOutstanding`,
`TestServerReplyKeepsShorterHandshakeDeadline`, `TestRootsPlanningHonoursBudget`,
`TestRootsWorkersStayBoundedAcrossTimeouts`, `TestExpiredRootsPlanCannotRememberSnapshot`,
`TestLateRootsPlanCannotReplaceNewerGeneration`,
`TestClosedConnectionCannotStartObsoleteRootsPlan`,
`TestRootsSnapshotTracksDeliveryCertainty`, `TestRootsSnapshotCannotCrossGenerationReplacement`,
`TestInvalidRootNameCannotPublishWatcherSnapshot`,
`TestRootsFirstViewSurvivesAcknowledgementOrder`,
`TestWaitingRootsReplyCanCancelWithoutReleasingAnotherReply`,
`TestUncertainRootsStayStaleThroughLaterSuccessfulReplies`

S5. Lane delivery, cancellation and server replies may Write concurrently;
connections support that and Close alongside blocked Read. Steady-state roots
planning runs separately so it cannot hold up responses. Reads are single-owner:
handshake finishes before cache starts the steady-state reader (F5a).
→ `TestSlowRootsPlanDoesNotBlockResponses`

## 5. Failure semantics

F1. Upstream death, including home, does not end the session. Proven-retired cached
connections reconnect before delivery; dial/private-handshake failure may retry
within the original budget. An uncertain caller Write instead returns "may have
executed", closes the connection and retains its credit: never replay it.
Proven pre-delivery/size rejection refunds its reservation. Explicit client
retries may duplicate mutations; there is no end-to-end at-most-once guarantee.
→ `TestHomeUpstreamEOFDoesNotCloseClient`,
`TestSupervisorUncertainPostDoesNotReplayExecutedTool`

F2. Upstream death explicitly fails each in-flight local call with its ID and an
internal error, rather than leaving clients waiting indefinitely.
→ `TestUpstreamDeathFailsItsInFlightCalls`

F3. Pending calls are keyed by connection, not worktree, so an old reader cannot
fail a replacement connection's calls. The same entry retains the worktree for
cancellation routing (R7).
→ `TestStaleUpstreamDeathSparesTheReconnectedCall`

F4. Register pending calls before Write: a local server can reply before Write
returns, and later registration would leave a duplicate reply owed.
→ `TestCallAnsweredBeforeItsWriteReturnsLeavesNothingOwed`

F5. Exactly one claimant removes a pending ID and owns its response. Upstream
answers, failed Write/death, cancellation and execution timeout compete; losers
drop answers and cannot revive/replay completed local calls. Queued delivery,
reservation, local errors and timer startup carry the original call generation,
so an old queue entry or returning Write cannot alter a reused ID.
→ `TestSendLeavesACallItsDyingUpstreamAlreadyFailed`,
`TestUpstreamAnswerToAnAlreadyFailedCallIsDropped`,
`TestDeliveryCompletionCannotTouchReusedID`,
`TestCancelledQueueEntryCannotClaimReusedID`

F5a. cache is the only setter of lane.conn and starts its reader in the same
step, after a successful Write. A failed first caller Write is handled by the
lane without replay. Failure may clear lane.conn before its reader finishes.
→ `TestSendDoesNotReplayAnUncertainInitialize`

F6. Client EOF or session cancellation stops stdio. `io.EOF`, `context.Canceled`
and `ErrConnectionClosed` exit zero. Shutdown cancels delivery, closes lane
queues and waits for owned failed-child cleanup (P2a).

## 6. Port map file

`~/.local/share/gopls-ports.map` is mode `0600`, one JSON object per line:

```json
{"Worktree":"/Users/x/src/repo","Port":62001,"PID":4242,"StartedAt":1754218800,"StartedAtMilli":1754218800417}
```

M1. Write a same-directory temporary file, fsync, then rename atomically. Set mode
explicitly so even an exotic umask cannot leave the writer unable to read it.

M2. Registry transactions use exclusive `<map>.lock` flock with cancellable,
nonblocking acquisition; a separate lifetime lock excludes competing owners.
Reservation, spawn and publication are distinct phases. Readiness waits hold
neither owner mutex nor registry transaction lock. Unchanged transactions skip
replacement; blank formatting may normalize. Malformed nonblank records fail
without mutation.
→ `TestWithRecordsWritesOnlyWhenTheFileWouldChange`, `FuzzReadMap`,
`TestReadMapRefusesUncertainProcessReservations`

M3. Every malformed nonblank or invalid-field record fails the whole read: no
partial rows or rewrite, since it may reserve a live process. Blank lines consume
budget. Errors name the file without revealing contents; repair is explicit.
Reads reject >4 MiB, >4001 lines, >64 KiB scanner tokens and nonregular/symlink
state using nonblocking descriptor checks. Writers reject >4001 records,
>4 MiB total or >65535 encoded bytes/record before rename, preserving prior data.
→ `TestReadMapRefusesUncertainProcessReservations`

M4. Worktree must be nonempty, PID 1…2^31−1 and Port within the allocation range.
Valid metadata never authorizes PID signalling/adoption; owner/generation
identity and guardian-owned handles remain authoritative.
→ `TestReadMapRefusesUncertainProcessReservations`

M4a. StartedAt is Unix seconds and defaults to zero for legacy records. Retirement
uses owner readiness/monotonic completion, not legacy start-grace or popularity.

M4b. Optional `StartedAtMilli` stores the same clock reading in Unix milliseconds.
Only S1b uses it for module-mtime comparison, and only when
`StartedAtMilli / 1000 == StartedAt`; missing/contradictory values use StartedAt.
Legacy connection identity stays in seconds, unaffected by older writers omitting
milliseconds. Both old and new records parse.
→ `TestStartedPrecisely`, `TestMapKeepsTheMillisecondStart`

M5. JSON escaping round-trips paths containing tabs, quotes and newlines while
keeping each record on one line.
→ `TestMapRoundTripEscapesPaths`

## 7. Port allocation

Range: 61100–65100. Start at `firstPort + sha256(worktree)[:8] mod count`, then
probe in order past mapped ports and net.Listen failures. The stable hash tends
to return a worktree to the same port.
→ `TestBasePortIsStableAndInRange`, `TestAllocatePortProbesPastMappedAndOccupiedPorts`

## 8. Liveness

Only the owner admits/retires generations. It rechecks idle/stale eligibility
under its mutex, persists the matching generation as terminating, then signals
through its guardian. Queued work, Writes and protected operations pin a
generation; quiet attachments do not. Failed persistence, uncertainty or ignored
TERM retains slots/FDs (O2–O5).
→ `TestSupervisorIdleCandidateRechecksLastCompletion`,
`TestMaintenanceRestartsAServerThatMissedAnUncoveredChange`,
`TestMaintenanceSparesUncertainWatchMetadata`,
`TestSupervisorGuardianLossRetainsCapacityThroughStorageFailure`

`list` asks the owner to maintain. Offline, an empty registry prints the normal
header; retained records fail with cleanup instructions. No legacy PID signals,
pending-file publication or sampled LFU remain. Stop old writers and reconcile
legacy processes before migration. `status` is observational.

Readiness bypasses proxies/redirects. Bounded process/lsof output is diagnostic,
never signal/refund authority. Malformed census output, including ferr/fNOFD,
is rejected completely.
→ `TestProbeDoesNotFollowRedirects`, `TestProcessIdentityRejectsSubstringMatches`,
`TestCountOpenFilesRejectsMalformedSnapshot`, `TestParseOpenFilesRejectsDescriptorErrors`

## 9. Spawning

P1. The owner reserves a generation/immutable FD quota and a port excluding both
persisted records and concurrent starts. Its guardian persists a pre-fork
manifest and inherits a stable lifetime lease. Delivery waits for registry
publication and readiness. The owner-death pipe triggers autonomous cleanup,
including spawn-before-publication. Confirmation requires direct-child reaping,
fresh anchored-group exit and actual guardian exit. Unstarted failure refunds
only after released-lease reconciliation proves no child started. Recovery
refuses unresolved manifests/busy leases; no age/PID adoption guesses or direct
frontend spawns are allowed.

P2. Readiness polls for up to ten seconds, backing off 10–100 ms, outside owner mutex
and registry lock. Callers for one worktree share the same start; cancelling a
waiter cannot cancel it or signal another caller's process.
→ `TestReadinessProbeHonoursCancellation`,
`TestSupervisorTwoBridgesAndHTTPShareAndReconnectAfterIdle`

P2a. Spawned generations failing publication/readiness clean up through the
guardian before returning: TERM, two seconds for proof, then KILL and two more
seconds. Ordinary retirement never escalates. Failed cleanup retains capacity.
Partial starts retain typed error and lease/manifest proof obligations even
without a known child PID; reconciliation also ends their owner observer.
→ `TestAuditRealGoplsFailedStartReapsChild`,
`TestGuardianPartialStartReturnsRetainedCapacityContract`

P3. Confirmed child/group/guardian exit releases unknown operations. Failed
registry cleanup still retains generation/FD capacity and is retried by
maintenance. Stale completion cannot change a replacement; corrupt storage
cannot reach startup or discard reservations silently.
→ `TestSupervisorGuardianLossRetainsCapacityThroughStorageFailure`,
`TestSupervisorCorruptRegistryNeverLosesReservations`

P4. Guardian pipes feed the bounded owner logger. Paths use a worktree hash and
refuse symlinks, hardlinks and aliases. Queue/storage failure fences delivery.
Offline trim-logs uses bounded scans and in-place truncation; it must not race
the active owner's files.
→ `FuzzLogPath`, `TestLogTrimPreservesInheritedAppendDescriptor`,
`TestLogTrimIgnoresUnrelatedFilesAndSymlinks`, `TestRunTrimLogs`

P5. A pinned dependency contract suppresses managed gopls telemetry sidecars.
Before startup and every delivery, the selected Go toolchain must report the
exact private telemetry-off directory. Mismatch fails without changing global
telemetry, HOME or GOTOOLCHAIN.

## 10. Known limits

- One owner-wide worker bounds production resolution/roots planning even across
  session churn. An uncancellable syscall may occupy it indefinitely; shutdown
  does not wait or create replacements. Direct test seams retain router-local
  workers. Registry reads, fsync and rename are also not context-interruptible.
- A full 64-message output queue fails the session, including on transient bursts
  of terminal replies/failures/handshake messages. Readers never park after
  releasing operation credits and thereby retain unbounded decoded results.
  A blocked stdout writer may remain until the client reads or the CLI exits;
  shutdown does not wait for it, and its payload stays charged.
  → `TestClosedConnectionReleasesBackpressuredReader`,
  `TestFullOutputQueueReleasesTerminalProducers`,
  `TestOutputSaturationStopsSessionWithBlockedWriter`
- Periodic/final metrics share one worker. The final snapshot follows cleanup
  and gets 100 ms to flush; direct seams may retain one blocked stderr worker.
  Production diagnostics use the bounded nonblocking logger. Fatal HTTP errors
  cancel their backend before diagnostics.
  → `TestHTTPErrorCancelsBeforeDiagnosticWrite`,
  `TestMetricsShutdownDoesNotWaitForStderr`, `TestMetricsFinalSnapshotFollowsCleanup`
- With execution timeout disabled, accepted work may wait indefinitely.
  Outstanding counts bound retention, not duration; cancellation/timeout cannot
  prove a mutation stopped. Slots/FDs are bounded, shared gopls RSS is not.
- Wire budgets exclude decoded objects, framing/serialization scratch, GC peaks
  and total heap/RSS. Roots and managed logs have separate count/byte limits;
  unrelated files, external caches and arbitrary project fanout remain outside.
- Ignored SIGTERM leaves a visible record and blocks that worktree's replacement.
  Only owned failed-child cleanup has bounded escalation; signals use guardians.
- Loopback clients/listeners and private state/ancestors are trusted.
  Bind-and-close port selection does not authenticate the eventual listener, and
  diagnostic command lines can be spoofed. Arbitrary escaped descendants cannot
  be universally contained by POSIX process groups.
- Old binaries ignore Terminating. Upgrade all writers sharing the map before
  relying on termination retention; mixed-version safety is not promised.
- Paths must be valid UTF-8 to round-trip through JSON. → `FuzzMapRoundTrip`
- Waiters share a start's failure; only a later independent call can start a new
  generation after confirmed exit and durable cleanup.

## 11. Configuration and observation

Environment is parsed when constructing the manager. Counts must be positive;
execution timeout must be a nonnegative Go duration. The README configuration
table is canonical; the ledger below defines ownership/release.

Metrics include lock wait/hold, readiness and ensure durations. Session snapshots
every 30 s and at shutdown report requests, fixed rejection reasons, cancels/
expiry, lanes/memos, path hits/misses, expiry epochs/rollovers, unresolved/abandoned
operations, SSE current/peak capacity, and count/total/max nanoseconds for routing,
resolution, queue waits and handshake. Counters copy under their owning locks;
Completed includes local termination and shutdown cleanup, not just successes.
→ `TestFromEnvRejectsInvalidSettings`, `TestFromEnvOverrides`, `TestWithDefaults`,
`TestAccountingTerminalPathsAndSnapshots`

`status` is read-only JSON and never sweeps. It reports registry integrity,
server/termination state, matching-identity RSS in KiB and lsof OpenFiles/total
against budget. One bounded ps snapshot covers recorded PIDs; failure discards
it entirely. Missing values and legacy active-client counts are null. The Owner
snapshot reports exact bounded state when reachable, otherwise an error rather
than invented zeros. Logs report record sizes, aggregate bytes/file count,
largest file and count above trim threshold.
→ `TestStatusMeasuresWithoutChangingRegistry`, `TestStatusRejectsFailedProcessSnapshot`

**Stdio.** A direct case-sensitive scalar parser owns raw fields as copies; the
SDK handles writes, batch correlation and remaining validation. Notification-only
batches have no response/response ownership; mixed batches aggregate ID-bearing
requests only, preserving order. Per-value limits include preceding whitespace
and decoder read-ahead; oversized/incomplete values fail before unbounded growth.
→ `TestStdioMessageLimitAndReadAhead`, `TestStdioMatchesSDK`,
`TestStdioPreservesBatchResponses`, `TestStdioCloseUnblocksRead`,
`FuzzDecodeScalarMatchesSDK`, `FuzzStdioNotificationBatches`

**SSE.** Complete events, including comments/framing, and POSTs share the message
ceiling. Reserve allocated frame capacity before allocation, charging both old
and replacement buffers during growth. Exhaustion closes the connection rather
than waiting with partial frames. Each reader has one bounded frame, unbuffered
handoff and fixed 4 KiB bufio reader; there is no 100-event SDK queue. Multiline
data compacts in place before SDK JSON-RPC decoding. Close frees blocked handoff
frames. Oversize/invalid events close and fail pending calls; oversize POSTs fail
before Write. Wire accounting excludes decoded messages, network buffers and GC.
Connections require numeric HTTP loopback addresses; endpoint events must retain
scheme/host/port without credentials/fragments. The dedicated client bypasses
environment proxies and refuses GET/POST redirects.
→ `TestSSEFrameBoundsAndOwnership`, `TestSSEReadAheadAndSlowConsumerClose`,
`TestSSEOversizeBeforePost`, `TestSSERejectsEndpointEscape`,
`TestSSERejectsRedirects`, `FuzzSSEFrameBudget`

**Package boundary.** One Go module: internal/config owns policy/validation,
internal/transport framing/connections/budgets, and internal/protocol pure roots,
physical-path and complete-result adaptations. They never import the root
command. Root owns routing, lanes, requests/terminal generations, upstreams,
processes/probes and registry mutation. Leaf tests accompany packages; integration
and lifecycle tests remain at root.

## Stateless HTTP frontend

`http [address [path]]` serves SDK stateless Streamable HTTP with JSON responses
at `/mcp`; defaults are `127.0.0.1:6099` and home `.`. Listeners are loopback-only.
Before rewriting/backend admission, validate the original numeric loopback or
localhost Host and apply Go cross-origin protection to Origin/Sec-Fetch-Site.
Unsafe browser requests receive 403 under the normal write deadline; nonbrowser
clients without those headers remain supported. This is not local-process
authentication. Stdio remains available.
→ `TestSupervisorHTTPRejectsUntrustedBrowserRequests`, `TestLoopbackHTTPProtection`

Negotiation terminates locally; home supplies instructions, and tools/list/call
use a shared router's legacy SSE lanes, roots replies, handshakes and accounting.
No frontend session identifier is issued/used. Internal IDs are unique across
clients. HTTP never reads/writes sticky state: pathless calls go home; absolute
paths retain R1/R2/R10 rules. Limits/caches persist across POSTs. Cold resolution
has a separate 64-message queue/coordinator so warm/pathless calls bypass a
stalled filesystem worker without changing stdio order or creating workers.
→ `TestHTTPWarmCallsBypassBlockedResolution`

Request cancellation completes the internal call and sends its owning SSE
connection an advisory. Cross-request cancellation and unsolicited upstream
notifications are not relayed. Failed/cancelled cached backends fail the current
request without replay and retire despite recent traffic. Only a later independent
request initializes a replacement. Closing entries reject reuse and stay charged
through active handlers and actual teardown, with one sweep/shutdown teardown owner.
→ `TestSupervisorRetiringBackendKeepsCapacityUntilCloseCompletes`,
`TestSupervisorFailedHTTPBackendRetiresDespiteRecentUse`,
`TestSupervisorFailedHTTPBackendKeepsCapacityUntilActiveHandlersFinish`,
`TestSupervisorFailedHTTPBackendReinitializesOnIndependentRequest`

Exchanges reserve maximum body size until completion, including slow writes.
Capacity is min(session limit, HTTP body budget / maximum message size), default
16; excess receives 503 before body reads, including slow/chunked bodies.
→ `TestHTTPBodyBudgetIncludesSlowReaders`, `TestHTTPRejectsOversizeAndReleasesReservation`

Middleware supplies missing SDK result defaults: cacheScope=`public` for tool
lists and resultType=`complete` for tool calls (required by protocol 2026-07-28).
The latter is inserted into opaque JSON because its SDK field is unexported.
Existing values/escaped key spellings remain and no duplicate key is introduced.
New middleware methods must supply their corresponding SDK defaults too.
→ `TestHTTPProtocolAndErrors`, `TestWithCompleteResultType`

Defaults: 4 MiB body, 5 s header read, 30 s body read and a 30 s response-write
budget starting at first output. The data proxy has no fixed response-header
timeout overriding Execution=0 or a longer execution policy. Caller cancellation
and body/write/socket limits still apply. Ping/startup/CONNECT have explicit
budgets; management queries allow at most 40 seconds (earlier caller deadline
wins). The SDK owns validation/stateless lifecycle; upstream framing stays bounded.
→ `TestSupervisorProxyExecutionFollowsCallerBudget`

→ `TestHTTPStatelessRoutingAndSSEReuse`, `TestHTTPProtocolAndErrors`,
`TestHTTPCurrentSDKClient`, `TestHTTPDisconnectCancelsLegacySSECall`,
`TestHTTPBackendShutdownReleasesCalls`

## JSON processing

Project serialization uses encoding/json/v2, framing encoding/json/jsontext and
opaque payloads jsontext.Value, with no v1 options. Names are case-sensitive:
routing recognizes arguments/file/dir/files; cancellation recognizes requestId.
Unknown fields remain allowed; duplicate names and invalid UTF-8 are rejected.
Limits/reservations apply before parsing. Invalid HTTP JSON receives 400 before
SDK negotiation; stdio rejects the message and malformed SSE closes its connection.
→ `TestPathArgumentsUseStrictJSON`, `TestStdioRejectsAmbiguousJSON`,
`TestHTTPRejectsAmbiguousJSON`, `TestSSEReadContracts`

Adapters change exact names only, preserve opaque fields and leave malformed
input to its owning parser. Defaults never duplicate keys. Registry JSON retains
capitalized names; differently cased manual fields do not match. Optional zero
fields use omitzero; nil maps/slices encode as empty objects/arrays. SDK encoding
is an upstream dependency; HTTP explicitly keeps empty tool arguments objects.
SDK float64 ID precision remains a compatibility boundary; R7 rejects numeric
cancellation aliases first, with a saved counterexample in
testdata/fuzz/FuzzCancelTarget/ (both signs, zero and range boundaries covered).

## Resource ownership and evidence ledger

Audit baseline: `0437e95`, Go 1.27.4/gopls 0.23.0 on Darwin/Linux. These are
bounded resources and exercised failure paths, not proof that all bugs are absent.

### Findings and response

Historical R1–R9 cover sockets, retired pending state, registry/helper/watch/log
bounds, FD reservations, idle authority and guardians. Contracts above and the
ledger replace the old subsystem descriptions; [formal runtime conformance](formal/README.md#runtime-conformance)
maps evidence and assumptions. Original counterexamples remain in Git history.

### HTTP browser boundary

The [HTTP contract](#stateless-http-frontend) also pins no backend allocation or
completion-clock update on rejected requests. Regressions cover IPv4/IPv6,
localhost, same-origin and nonbrowser clients; trusted local-client/project/OS
assumptions remain.

### Resource ledger

Environment overrides are in README. Wire/count reservations are not byte-exact
heap, RSS or kernel-memory bounds. Release requires actual consumption/teardown,
not merely the caller stopping its wait.

| Resource | Scope/default admission | Release | Boundary |
| --- | --- | --- | --- |
| Frontends/backends | Owner: 16 combined attachments/cached backends, including failed/closing entries | Socket close or idle/failed retirement; actual router teardown after active handlers; no closing reuse; one teardown owner | Protocol/config mismatch fails before allocation. Frontend OS processes/sockets remain local |
| Generations | Owner: 64, including starting/draining/uncertain/exited-with-storage-pending | Confirmed child/group/guardian exit and durable registry removal, never signal/EOF alone | Ignored TERM/uncertainty intentionally consumes capacity; repair may be needed |
| Operations | Owner: 1024 queued/caller/handshake/server-control identities | Definitive response, proven pre-delivery rejection or confirmed generation exit | Active work has no default execution deadline; grace retirement is not false completion |
| Retained payloads | Shared 64 MiB max-message reservations for ingress/delivery/output/advisories; IDs ≤1024 bytes, initialize ≤64 KiB | Consumption/discard/Write return; local cancel never refunds queued objects | Decoder/GC/serialization overhead and bounded transient reader products are additional |
| Queues/lanes | Per router: 64 ingress/delivery/control/output entries, 64 lanes, 128 requests/worktree, 1024/session; ≤16 owner frontend slots | Shutdown drains payloads/closes transports; unknown owner credits survive router teardown | Count-bounded queue/control structures and decoded objects compose beneath frontend cap |
| Memos | 4096 paths +4096 directories/router; shared lazy 5m epoch | Rollover/expiry/router retirement | Spelling bytes inherit message bounds; idle heap may await GC |
| Filesystem worker | One unbuffered owner-wide worker | Caller cancellation; syscall itself may remain | No replacements across churn; syscalls are not universally cancellable |
| Output/diagnostics | Context/deadline/Close-bounded sockets; bounded owner logger | Actual Write return/teardown; full terminal queue fails session | One blocked stdout/synthetic writer may remain, with payload charged |
| HTTP exchanges | Owner: min(session, 64 MiB/4 MiB)=16, reserved before decode; 16 KiB headers, 5 s header/30 s read/30 s output budgets | Exchange return, including slow output | SDK/result objects and framing scratch add bounded products |
| HTTP sockets | 128 accepted owner connections +one transient excess; frontend listener also bounded | Close, 60 s idle or header deadline; excess closes before reader goroutine | Multiplied frontend processes lie outside owner FD quota; active owner attachments stay capped |
| SSE frames | Shared 64 MiB allocated capacity; 4 MiB complete event/POST | Frame release/Close | Decoded results transfer to retained-message accounting; decoder/GC peaks are additional |
| FDs | Owner transport/control formula; watched-tree count +256 child +64 guardian/observer headroom per generation | Clean unstarted failure or confirmed retirement; owner/child RLIMIT_NOFILE | No universal descendant/RSS/watch-memory bound; exhaustion fails admission |
| Watch plans | ≤generation-cap keys; 100k entries, 32 MiB names/paths, depth 128, 4096 roots, 16384 partials; metadata 256 files/16 MiB; filters 1024/1 MiB | Replacement per generation; prune on confirmed exit; never publish an incomplete plan | Mutation may invalidate views; uncertainty preserves generation until a complete verdict |
| Registry | One read/temp FD per operation; 4 MiB/4001 lines/64 KiB scanner token | Close/unlink/atomic rename; failure preserves prior contents and forbids uncertain-row probing/signals | fsync/rename may block; crash may leave temp inode; ancestors trusted |
| Locks/manifests | Stable lifetime/action names over 4001 ports; bounded recovery scan | FD close; retain lock inodes; remove manifests only on proof | Old lock/temp files need offline cleanup; no PID/age fallback |
| Logs | One global worker; 128 jobs/≤1 MiB allocated queued+active payload; owner 64 MiB disk/≤server-cap+1 files | Bounded retention, serialized append, confirmed release; one terminal failure channel | Active-append counters approximate; mutation checks cover touched files; sole managed writer required |
| Usage/pending mirrors | Removed; owner operations replace advisory files/sampled LFU | Not applicable | Stop old writers before migration |
| Helpers | Stdout 64 KiB/4 MiB, stderr 64 KiB, derived deadline, 250 ms pipe-wait delay | Cancel/reap direct child on error/overflow; reject partial snapshots | Escaped grandchildren need OS containment |
| Telemetry | Pinned managed-sidecar suppression; shared private mode-off state | Check selected Go's exact off directory before spawn/delivery | Compatibility mismatch fails; external caches/global telemetry unchanged |

### Evidence and remaining work

Registry churn checks /dev/fd across 256 failed cycles and 64 cancelled lock
waits. Boundary tests cover FIFO/symlink/nonregular/corrupt input and no signals
on uncertain activity. Retired pending-file tests are replaced by owner-startup
and lease-reconciliation regressions. Pure concurrency/idle tests use synctest
and in-memory storage; real sockets/processes stay outside fake time.

Formal artifacts are checked separately in formal/. Audit artifacts remain
outside the repo under ~/.codex/artifacts/gopls-audit/. Final evidence must bind
native Linux/macOS results and owner coverage to its source. Default-duration
zero-client checks and 24h/72h soaks remain pending at this checkpoint; no
long-soak or universal POSIX descendant-containment conclusion is claimed.
