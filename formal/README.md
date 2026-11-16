# Lifecycle verification

These specifications check the shared-process ownership protocol.
They do not verify the Go binary, gopls, the MCP SDK, or the operating system.
The supervisor implementation and its conformance tests must be reviewed
against the transition obligations below before these results support a release.

## Reproduce

Install Java 17 or newer and Bun, then run from the repository root:

```sh
bun --install=force run formal/check.ts all
```

The runner downloads official TLC/TLAPS releases, checks pinned SHA-256 digests,
and keeps tools under `~/.cache/gopls-mcp-formal`. It writes copied specifications,
checker logs, generated configurations, state directories, proof caches and
`results.json` outside the checkout, under
`~/.codex/artifacts/gopls-audit/formal/<timestamp>/`. No repository data is uploaded.

Model configurations are declared in `check.ts` and written into the evidence
directory before checking. Their recorded `.cfg` names remain reproducible case
identifiers; all six `.tla` specifications stay checked in.

Use `GOPLS_FORMAL_TOOLS` and `GOPLS_FORMAL_OUT` to select absolute external tool
and evidence directories. `GOPLS_FORMAL_REVISION` can record the implementation
revision alongside the source hashes. TLAPS downloads use fixed GitHub asset IDs
and SHA-256 digests; archives and installations are named by digest. CI caches
these tools with an exact OS/architecture/runner-source key and rechecks archive
hashes before extraction. A deleted upstream asset with no retained cache fails;
review and update the pin explicitly. There is no alternate verifier or silently
accepted partial result.

The `models`, `mutations`, and `proofs` subcommands run individual groups. The
runner uses one TLC worker, a deterministic seed/fingerprint polynomial, a
512 MiB Java heap and a 180-second deadline per checker invocation. Timeouts,
syntax errors, unsupported proof platforms and unproved obligations fail the
run. Negative checks require the specific safety/liveness/proof failure class;
a generic nonzero exit is not sufficient. It copies proof sources into the
evidence directory before mutating guards, leaving repository sources intact.

The pinned TLAPS release provides Darwin/arm64 and Linux/amd64 binaries. Model
checks work wherever the pinned Java toolchain runs. Proof runs on other
platforms fail explicitly. The native binary is extracted outside the repo.

## What is modeled

`SupervisorLifecycle.tla` models a serialized owner with shared server
instances `(worktree, generation)`, attached clients carrying boot tokens,
protected operations, monotonic idle age, live physical processes, and states
Starting, Ready, Draining, Terminating and Exited.

- Admission and idle drain compete in the same state transition domain.
  Admission protects work before delivery. Passive attachments and renewals do
  not reset idle age or prevent draining. Definitive completion resets idle age.
- Idle drain requires no protected operation and an elapsed idle interval.
  Draining and terminating generations still consume capacity. New generations
  cannot overlap a retained generation for the same worktree.
- Termination requires owned identity. Loss of identity models a child exit/PID
  reuse before registry reconciliation. A successful signal alone cannot free
  the reservation; exit must be observed.
- A crash preserves operation ownership and server reservations. Recovery fences
  old delivery transports, increments the boot token and drains old generations
  before new admission. This is explicit recovery and can interrupt abandoned
  work; it is not classified as idle eviction.
- A stale attachment cannot authorize delivery to a replacement generation or
  new owner boot. Storage failure is a stuttering transition: no mutation may
  become visible without a successful atomic commit.

`OperationAccounting.tla` separately explores clients reusing a wire ID across
local request generations and multiple upstream connections. Operations are
keyed by request generation and exact connection. Reservation precedes a
possibly executing write. Cancellation/timeout and disconnect leave global
credits charged. Definitive responses release only their own credit and can
complete only the matching local request/connection. An uncertain retry requires
an additional credit. Confirmed server exit releases remaining credits.

`AbandonedRetirement.tla` separately checks same-boot controlled retirement for
one worktree with two successive generations. Admission reserves a credit before
delivery. Cancellation, local timeout, or disconnect marks that operation
abandoned without releasing its credit. Active operations protect indefinitely;
each abandoned operation independently protects until a full grace interval has
elapsed. Retirement also requires the generation's last definitive completion
to be at least one idle interval old. A late definitive result resets that idle
age. Passive attachments do not appear in the protection predicate.

Retirement fences admission atomically and preserves every unresolved credit
through Draining and Terminating. Only a definitive result or confirmed child
and owned-group exit releases it. Draining generations keep the worktree/slot
reservation. This is controlled interruption of uncertain abandoned work, not
evidence that it finished. It covers the runtime's abandoned-operation grace
policy, which is distinct from `SupervisorLifecycle.IdleDrain` requiring an empty
operation set. The model omits startup, queued requests, boot changes, storage
steps and per-connection wire IDs; those remain separate model obligations.

The accounting model permits results delayed across disconnects and retries,
and multiple unresolved local generations. It is deliberately more permissive
about retry scheduling than the runtime; implementation retries must still
respect its own tool-execution/replay contract. Notifications and handshakes
are not operations in this model and need separate bounded-lifetime tests.

## Composed startup and cleanup core

`LifecycleCore.tla` separates reservation, possible process start, readiness,
uncertainty fencing, confirmed exit, durable record removal and final refund.
Operations are exact connection/generation/request tokens. Abandonment preserves
credits; grace expiry only removes protection. Confirmed exit can release remote
credits while the worktree and process-slot reservations remain charged until
durable cleanup succeeds.

Its 104 checked obligations establish safety over arbitrary sets:

- Unique retained reservation per worktree and slot; every possible process is
  covered by a reservation. Finite slots therefore bound possible processes.
- Uncertainty fences readiness without removing process or operation ownership.
  Final refund requires both confirmed exit and durable cleanup.
- Finish preserves every other operation token and its protection.
- Idle retirement excludes protected operations, and new holds cannot enter an
  idle-retired generation.

Two checked refinement theorems connect this model to `ReservationCore` and
`AdmissionCore`. Reservation refinement conservatively maps every retained,
unconfirmed reservation to an abstract possible process, including before fork.
Admission refinement maps current protected tokens to held operations and the
history of idle retirement to draining generations. Grace expiry maps to abstract
completion of protection; it does not claim that remote execution completed.
Ghost retirement/exit histories never recycle identities and are not runtime
containers. This composition proves neither actual Go execution nor long-run
metadata bounds.

Eight additional proof mutations must be rejected: overlapping reservation,
unreserved start, publication after fencing, busy idle drain, unconfirmed refund,
uncommitted refund, wrong-token completion and uncertainty releasing credits.
The checker uses the existing pinned tools and rejects syntax errors/timeouts as
negative-check successes. Runtime mapping and deterministic regression anchors
are in [Runtime conformance](#runtime-conformance).

## Safety proofs over arbitrary sets

The original two TLAPS proof cores do not depend on finite TLC configurations:

- `AdmissionCore.tla` proves that an operation cannot be held by a draining
  generation when acquire excludes draining generations and drain excludes held
  operations. Completion and confirmed exit preserve that invariant. This
  theorem applies to pure idle drain. It does not apply to controlled abandoned
  retirement, where unresolved abandoned credits deliberately remain held.
- `ReservationCore.tla` proves unique reservation per slot and per worktree,
  and that every physical process retains a reservation. Marking termination
  changes no reservation; forgetting is permitted only after physical exit.
  Unique slots give a process bound when the chosen slot set is finite.

Each module proves initialization, all next-state cases including stuttering,
and temporal invariance. The current proofs have 34 accepted obligations each.
They use TLAPS's SMT and propositional temporal backends. Those tools and their
translations are trusted; this is not an independently checked Isabelle proof
certificate, and no Go-to-model refinement theorem is supplied.

Proof mutation checks remove the acquire/drain exclusion and the exit-before-
forget guard. Both must produce an unproved obligation. The proofs do not
establish arbitrary program termination, scheduler fairness, OS signal identity,
process-tree cleanup, or implementation memory safety.

## Liveness assumptions

`LifecyclePureIdle.cfg` checks `PureIdleReclaims` without assuming that running
operations finish: after admission is quiesced, a Ready generation already
holding no operations eventually exits. This requires the owner to run,
monotonic time to advance, successful maintenance steps to receive weak fairness,
and the child to cooperate with termination (`CooperativeExit = TRUE`).

`LifecycleLiveness.cfg` additionally assumes weak fairness of every definitive
operation completion and checks that all reservations eventually disappear after
admission quiesces. It describes responsive operations only. Production cannot
assume this after lost replies, hung execution, or owner loss. An unresolved
operation is outside the pure-idle guarantee and must remain visible or enter
an explicit recovery policy.

`RetirementLiveness.cfg` checks `AbandonedReclaims`: after admission quiesces,
generations with only abandoned operations (or none) eventually exit. It assumes
weak fairness of Tick, retirement, termination, and confirmed exit, plus
cooperative termination. It assumes no fairness of definitive completion or
local abandonment. A still-active hung operation is therefore protected and
outside this guarantee. Maintenance blocked forever on storage, a suspended
clock that never resumes, and failed TERM cooperation invalidate liveness
assumptions while leaving safety obligations in force.

The negative `CooperativeExit = FALSE` run finds a liveness counterexample even
with no busy operation. This demonstrates why TERM-only cleanup cannot promise
reclamation of every uncooperative child. The `never-drain` mutation likewise
fails pure-idle liveness. A process retaining its reservation after ignored TERM
is correct safety behavior, not successful resource reclamation.

No symmetry reduction or state/action constraint is used in liveness runs.
Idle time is abstracted to two monotonic ticks; these checks do not verify the
shipped one-hour duration or a real-time deadline. Those need deterministic
boundary tests and the real default-duration integration run.

## Finite model coverage

The checked configurations deliberately use small capacities to expose races:

| Configuration | Scope | Distinct states |
| --- | --- | ---: |
| LifecycleSafety | 2 clients, 2 generations, 2 boots, 1 worktree/slot | 22,230 |
| LifecycleLiveness | same, responsive-operation fairness | 22,230 |
| LifecyclePureIdle | same, no response fairness | 22,230 |
| LifecycleMultiTree | 2 clients, 2 worktrees/slots, 1 generation, 1 boot | 13,802 |
| LifecycleThreeClients | 3 clients, 2 generations, 1 worktree/slot, 1 boot | 20,250 |
| OperationSafety | 2 clients, 2 local generations, 2 connections/credits | 6,645 |
| OperationThreeClients | 3 clients, 2 local generations, 2 connections/credits | 158,093 |
| RetirementSafety | 2 clients, 2 same-boot generations, 1 worktree/slot, 2 credits | 11,730 |
| RetirementLiveness | same, no response-completion fairness | 11,730 |
| RetirementThreeClients | 3 clients, 2 same-boot generations, 1 worktree/slot, 2 credits | 189,786 |

These are complete reachable-state explorations for the given configurations,
not a proof for all instance sizes. TLC uses fingerprints, so its collision
estimates are part of the raw evidence. The default runner retains those logs.

Ten safety mutations must fail: busy drain, early slot reuse, stale token,
unowned signal, early idle, cancellation releasing credit, disconnect releasing
credit, wrong local completion, same-connection ID reuse, and wrong-generation
credit release. The maintenance and noncooperative-exit liveness negatives and
two proof mutations must also fail for their intended reason. Four additional
retirement safety mutations must fail: retire active work, skip abandonment
grace, release credits on retirement, and reuse a slot before confirmed exit.
Disabling retirement and disabling cooperative exit each produce a retirement
liveness counterexample. Thus there are fourteen safety negatives, four
liveness negatives, and ten rejected proof mutations in the complete runner.

## Payload accounting abstraction

The runtime policy under review bounds retained wire payload reservations
globally to 64 MiB by default and limits opaque string request IDs to 1,024
bytes. `supervisor.reserveMessage` charges a full configured maximum message
size, independently of operation credits. Cancellation can release a queued
operation credit while its queued payload remains charged until the queue owner
discards it. Likewise a definitive response can release an operation credit
while a blocked writer still holds the response's payload reservation. The
release-once reservation travels in SDK message `Extra`, which is not on wire.

These specifications count operation identities and intentionally abstract
payloads to finite uninterpreted values. Their operation bound cannot establish
the byte budget, ID limit, or Go heap usage. In particular, the byte budget is
not an RSS ceiling: decoding, copied buffers, SDK objects, in-flight readers,
and fixed per-session allocations require separate amplification and global
admission analysis. Boundary/churn tests must check independent payload lifetime
and operation lifetime. No byte-conservation theorem or Go refinement proof is
claimed here.

## Runtime conformance

This ledger maps the Go supervisor to the protocol specifications and their
proof obligations. Symbols and named tests are implementation anchors; saved
execution artifacts record exact source hashes and runtime results. The checked
LifecycleCore refinements are model-to-model theorems, not a Go refinement proof.

| Obligation | Implementation symbols | Inspected regression evidence and scope |
| --- | --- | --- |
| Unique generation/worktree; shared capacity | `supervisor.ensureRecord`, `startGeneration`, `generations` under `supervisor.mu` | `TestSupervisorTwoBridgesAndHTTPShareAndReconnectAfterIdle` exercises two bridges plus HTTP sharing/replacement; `TestSupervisorGlobalAdmissionDoesNotMultiplyWithRouters` checks global operation admission |
| Queued admission competes atomically with drain | `router.admit`, `bindWorktree`, `beginOperation`; `supervisor.admit`, `bind`, `begin`, `protectedLocked`, `drain` share owner mutex | `TestSupervisorQueuedAndActiveWorkSurviveIdle`; queued reservations protect their resolved worktree, active operations remain protected after two hours |
| Pure idle boundary; passive attachments do not pin | `ownedUpstream.available`, `supervisor.idleSweep`, `drain` recheck completion/protection under mutex | `TestSupervisorIdleBoundaryAndQuietConnections`; `TestSupervisorIdleCandidateRechecksLastCompletion` covers completion after candidate selection |
| Same-boot abandoned retirement preserves credits | `router.removeLocked`, `abandonOperationLocked`, `clearRequests`; `supervisor.abandon`, `protectedLocked`, `drain` | `TestSupervisorAbandonedOperationsRetireOnlyAfterGrace` checks full grace, retained credit through drain, then exit release. `AbandonedRetirement` models this policy separately from pure idle |
| Definitive result releases exact operation | `operationKey{conn,id}`, `callState`, `router.ownerLocked`, `finish`, `supervisor.completeOperationLocked`; `ownedUpstream.Read`, `supervisor.answered` | `TestDeliveryCompletionCannotTouchReusedID`, `TestAbandonedIDCannotBeReusedOnItsConnection`, `TestCancelledQueueEntryCannotClaimReusedID`, `TestDelayedCancellationCannotCancelReusedID` and `TestResponseReadCannotReleaseIDBeforeWaiterClaim` cover local-generation/connection reuse and atomic response claiming |
| Cancellation/timeout/disconnect do not prove completion | `router.startExecution`, `removeLocked`, `clearRequests`; `ownedUpstream.Close`, `supervisor.abandon` | `TestAbandonedOperationsBoundFurtherDelivery`, `TestDisconnectedOperationsDoNotReleaseCredits`, `TestExecutionTimeoutQueuesAdvisoryCancellation` preserve unresolved credits. Local abandonment starts retirement grace; it does not release credit |
| Uncertain delivery is not replayed as a new execution | `router.beginOperation`, transport delivery certainty, `supervisor.rejectedBeforeDelivery` | `TestSupervisorUncertainPostDoesNotReplayExecutedTool` executes once, loses POST acknowledgement, then observes abandoned ownership |
| Signals require owned identity; TERM is not exit | `startOwnedGuardian`, `guardianChild.Signal`, `guardianController`, `runGuardian`, `supervisor.terminate`, `fenceUncertain` | Guardian OS tests and `TestGuardianCleanupProofDoesNotReleaseBeforeGuardianExit` distinguish child/group proof from guardian Wait. Unexpected guardian EOF/death is uncertainty |
| No slot/FD refund while startup ownership uncertain | `guardianStartError.Released`, stable port lifetime lease, `startOwnedGuardian`, `supervisor.maintenanceSweep`, `confirmExited` | `TestGuardianPartialStartReturnsRetainedCapacityContract`, `TestGuardianPreForkCrashReconcilesOnlyAfterLeaseRelease`, `TestGuardianInheritedLifetimeLeaseSurvivesParentCopyClosing` |
| Boot/session fencing and conservative restart | `ownerNonce`, `startGeneration`, `connection`, `available`; private versioned Unix owner endpoint; `withSupervisorSingleton`, `guardianRecovery`, `validateOwnerRegistry` | `TestSupervisorProtocolAndConfigMismatchFailBeforeAllocation`, `TestGuardianRecoveryKeepsPositivePIDAndLegacyUncertainty`; a complete two-owner stale-session/crash-window runtime matrix remains a separate integration obligation |
| Storage errors cannot erase reservations | `supervisor.terminate` persists matching boot/generation before signal; `confirmExited` retains generation/FD quota until registry removal succeeds | `TestSupervisorCorruptRegistryNeverLosesReservations` covers malformed storage. Native injected failures at every registry/manifest fsync/rename boundary are not established by TLC stuttering |
| Finite FD allocation | `supervisorFDAllowance`, `reserveGenerationFDs`, `countWatchDescriptors`, child RLIMIT and guardian allowance | Runtime budget/OS pressure tests must establish the concrete bounds; `ReservationCore` proves only abstract retained-slot containment |
| Payload and ID ownership independent of operation credits | `validOwnerID`, `reserveMessage`, `ownerMessageReservation.release`, `retainMessage`, `releaseMessage`, `releaseRetainedMessages` | `TestSupervisorCancellationDoesNotRefundQueuedPayload`, `TestSupervisorBlockedWriterKeepsPayloadChargedAcrossChurn`, `TestSupervisorOpaqueIDBoundaryAndControls` cover reservation lifetime and ID boundary |
| Server requests/handshakes share protection/accounting | `supervisor.serverControl`, `ownedUpstream.Write`, `Read` | `TestSupervisorOpaqueIDBoundaryAndControls`, `TestSupervisorUnknownOrFailedControlCannotRefreshIdle`; finite accounting model omits these message classes |
| One bounded shared filesystem worker | `ownerFilesystemCall`, `newSupervisor.filesystem` | `TestSupervisorSharedFilesystemWorkerSurvivesSessionChurnBoundedly` blocks one worker across expired sessions under `bubble`/synctest; it does not assume syscall cancellation |

### Composed-core implementation mapping

The additional LifecycleCore proofs check model-to-model refinement. They do not
execute or translate Go. The runtime uses the existing single owner mutex;
network/process/storage effects remain outside it. The independent runtime
checks below exercise the same release and fencing obligations.

| Core action/property | Runtime boundary and regression |
| --- | --- |
| Reserve / Start / Publish | `ensureRecord` reserves the worktree under mu; `prepareGeneration` reserves FD/port capacity and performs start/publication/readiness effects; `startGeneration` rechecks shutdown under mu before Ready. `TestSupervisorConcurrentStartsReserveDistinctPorts`, `TestSupervisorShutdownDuringStartCannotPublishReady` |
| Fence / UncertaintyRetains | `fenceUncertain` removes readiness while retaining remote credits and generation/FD capacity. `TestSupervisorGuardianLossRetainsCapacityThroughStorageFailure`, `TestSupervisorPartialStartKeepsLeaseUntilReconciliation` |
| ObserveExit / RemoveRecord / Forget | Guardian confirmation or an unstarted released lease authorizes `confirmExited`; its cleaning flag serializes durable cleanup, and only successful cleanup removes the generation/refunds FDs. `TestSupervisorExitCleanupHasOneOwner`, `TestSupervisorStartupFailureCleanup` |
| ExactFinish | Operations include exact connection plus wire ID and are bound to the generation. Caller/control namespaces remain separate. `TestSupervisorLateResponseCannotCompleteReplacement`, `TestSupervisorCallerAndControlNamespacesRemainDistinct` |
| Hold / Abandon / Expire / IdleDrain | Queued work, writes, active operations and unexpired abandonment grace feed `protectedLocked`; `drain` checks protection under mu. `TestSupervisorQueuedAndActiveWorkSurviveIdle`, `TestSupervisorAbandonedOperationsRetireOnlyAfterGrace` |

Core proof assumptions: operation tokens represent the correct concrete
connection/generation/request namespace; exit and durable-cleanup evidence are
truthful; grace expiry is not early; serialized runtime critical sections
implement the abstract actions; generation identities are fresh. Runtime tests
exercise these obligations but do not prove their universal implementation.
FD/payload byte arithmetic, OS behavior, parser/SDK behavior and wall-clock
progress are outside this proof. Existing finite models retain their separate
coverage of local ID reuse, boot fencing and bounded liveness.

### Refinements and limits

- `SupervisorLifecycle.Spawn` combines reservation and physical start.
  `LifecycleCore` now models reservation, start, readiness, fencing, exit,
  durable cleanup and refund separately, and refines both proof cores.
  Runtime `prepareGeneration` performs the effects; `startGeneration` publishes
  readiness or finishes failure cleanup. The model-to-Go mapping remains a
  reviewed obligation, not a mechanically checked program refinement.
- `SupervisorLifecycle.Recover` is atomic. Runtime starts a new boot only after
  singleton exclusion, guardian recovery, and old-registry validation. It
  refuses uncertain old ownership rather than adopting it. New boot nonces
  cannot alone prove old transport fencing or child exit.
- `AdmissionCore` excludes all held operations from draining. Its theorem
  supports pure idle admission exclusion; controlled abandoned retirement is
  checked by a distinct finite model and deliberately retains held credits.
- Queue reservations bind to a worktree before exact generation delivery.
  Admission after an old generation entered drain reserves future replacement
  work and waits for its retained generation to exit. This is more detailed
  than the lifecycle model's single Admit transition.
- Generation state Exited in runtime can still retain its slot/FD reservation
  while durable registry cleanup fails. LifecycleCore models this directly as
  exited but still reserved; its Forget requires removed record evidence.
  The older finite SupervisorLifecycle model combines that cleanup with Exited,
  so mapping every model by the runtime state's string alone remains wrong.
- Operation models omit notifications. Runtime protects a notification's local
  queue/write lifetime; a successful unacknowledged notification cannot prove
  that all resulting asynchronous server work finished.
- Server-originated requests and initialization handshakes are protected in the
  runtime. Their exact byte/operation lifetimes need the named runtime tests;
  they are not silently included in the smaller accounting state space.
- Default idle duration is `time.Hour`, with 30-second maintenance scheduling.
  The boundary test uses synctest. TLC proves neither wall-clock deadlines nor
  suspend/resume behavior; progress requires advancing monotonic time, runnable
  maintenance, successful storage, and cooperative child/group termination.
- Guardian signal identity relies on its anchored session/process group,
  private inherited pipes, actual direct-child Wait, bounded process observation,
  and stable inherited flock lifetime. These are trusted OS/runtime boundaries,
  not TLA+/TLAPS theorems. Arbitrary escaped/reparented descendant sessions on
  Darwin cannot be proven contained by group TERM. Detected escapes retain
  uncertainty; no claim of universal descendant cleanup follows.
- Pinned gopls/x/telemetry and selected Go private-telemetry-off compatibility
  checks constrain supported subprocess behavior. Formal models do not prove
  telemetry containment, dependency validation, or third-party code resource use.
- No automated trace-to-model replay or complete runtime suite for every formal
  mutation is supplied. In particular, the retirement active-work/grace/credit
  traces have corresponding assertions, but early-slot replacement while TERM
  is ignored, exhaustive same-boot delayed responses through replacement, and
  every owner-crash/storage boundary still require independent runtime evidence.
- Finite generations never recycle and all model clocks saturate. Long-run map,
  log, file, inode, heap and goroutine bounds require measured churn/soak evidence.
  A green finite model is not a proof that all possible Go bugs have been caught.

Runtime release evidence also requires native FD/process pressure,
crash-at-every-commit checks and a greater-than-70-minute default-idle run,
including suspend/resume. Generation nonces cannot close an open transport or
make PID-based signalling safe against PID reuse.

The finite models are not mechanically composed with each other or the Go
implementation. LifecycleCore refines both proof cores; byte/FD/heap/disk
arithmetic, decoded amplification and blocked-worker accumulation still require
independent runtime evidence.

## Tool pins and observed verification

The official `tlaplus/tlaplus` v1.8.0 asset used here reports TLC
`2026.11.15.002146` and SHA-256
`7e53aa636dac8a3cb0533f343115c0548e03fc0ce5edc7739becf58d3d0fb84f`.
The official `tlaplus/tlapm` assets pinned here report commit `bf665f4`.
Darwin/arm64 asset `753937802` and Linux/amd64 asset `753940334` have their
GitHub-published SHA-256 digests pinned in `check.ts`. The runner verifies the
prover's reported revision before proofs. The rolling `1.6.0-pre` download URLs
are not used because upstream replaces their contents. Sources:
[TLC](https://github.com/tlaplus/tlaplus/releases/tag/v1.8.0),
[TLAPS](https://github.com/tlaplus/tlapm/releases/tag/1.6.0-pre),
[reviewed TLAPS changes](https://github.com/tlaplus/tlapm/compare/ff40755...bf665f4).

The runner's source-hashed `results.json` and per-check logs are authoritative
for their recorded platform. Native runtime conformance is checked independently.
