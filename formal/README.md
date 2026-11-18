# Formal verification

Models and proofs cover shared-process ownership, retained payloads and hot
restart handoff. The [conformance ledger](#runtime-conformance) maps model
obligations to Go code and runtime tests.

## Reproduce

Use Java 25 on `PATH` and Bun 1.4.4. Run from the repository root.

```sh
bun --install=force run formal/check.ts all
```

Use `models`, `mutations` or `proofs` to run one group. TLAPS binaries support
Darwin/arm64 and Linux/amd64. TLC needs Java and Bun.

| Setting | Default or effect |
| --- | --- |
| `GOPLS_FORMAL_TOOLS` | Absolute tool cache path. Defaults to `~/.cache/gopls-mcp-formal` |
| `GOPLS_FORMAL_OUT` | Absolute evidence path outside the checkout. Defaults to `~/.codex/artifacts/gopls-audit/formal/<timestamp>/` |
| `GOPLS_FORMAL_REVISION` | Record the implementation revision alongside source hashes |
| `GITHUB_TOKEN` | Optional token sent only to `api.github.com` |

The runner verifies pinned SHA-256 digests and writes model copies, generated
configs, checker logs and `results.json` to the evidence directory. CI uploads
these files. Mutations edit model copies. TLC uses one worker, fixed seed and
fingerprint polynomial, a 512 MiB heap and a 180 s deadline per invocation.
Each negative check requires its expected safety, liveness or proof failure.
Timeouts and syntax errors fail the run.

## What is modeled

| Model | Transition coverage |
| --- | --- |
| `SupervisorLifecycle.tla` | Admission and pure idle drain serialize. Definitive completion resets idle age. Termination requires ownership and confirmed exit before refund. Recovery preserves reservations, fences old transports and changes the boot token before admission. Failed storage commits stutter. |
| `OperationAccounting.tla` | Reserve before possible execution. Match responses by request generation and connection. Cancellation, timeout and disconnect keep credit. Uncertain retries need additional credits. Confirmed exit releases remaining credits. |
| `AbandonedRetirement.tla` | Same-boot retirement waits for the idle interval and each abandoned operation's grace. Active work protects indefinitely. Draining fences admission. Responses or confirmed exit release credits. Only confirmed exit releases the slot. |

The accounting model permits retry schedules beyond the Go replay contract.
It omits notifications and handshakes. The retirement model omits startup,
queued requests, boot changes, storage and wire IDs.

## Composed startup and cleanup core

`LifecycleCore.tla` separates reservation, possible process start, readiness,
uncertainty fencing, confirmed exit, durable record removal and final refund.
Operations are exact connection/generation/request tokens. Abandonment preserves
credits; grace expiry only removes protection. Confirmed exit can release remote
credits while the worktree and process-slot reservations remain charged until
durable cleanup succeeds.

Its 104 obligations establish these properties over arbitrary sets.

- Each worktree and slot has at most one reservation. Every possible process
  has a reservation, so finite slots bound possible processes.
- Uncertainty fences readiness without removing process or operation ownership.
  Final refund requires both confirmed exit and durable cleanup.
- Finish preserves every other operation token and its protection.
- Idle retirement excludes protected operations, and new holds cannot enter an
  idle-retired generation.

Two checked refinement theorems connect this model to `ReservationCore` and
`AdmissionCore`. Reservation refinement conservatively maps every retained,
unconfirmed reservation to an abstract possible process, including before fork.
Admission refinement maps current protected tokens to held operations and the
history of idle retirement to draining generations. Grace expiry ends abstract
protection while remote execution may continue. Model-only histories retain
fresh generation identities indefinitely.

Eight proof mutations remove guards for overlapping reservations, unreserved
starts, publication after fencing, busy idle drain, unconfirmed or uncommitted
refund, wrong-token completion and credit release under uncertainty.

## Safety proofs over arbitrary sets

Both proof cores quantify over arbitrary sets.

- `AdmissionCore.tla` proves that an operation cannot be held by a draining
  generation when acquire excludes draining generations and drain excludes held
  operations. Completion and confirmed exit preserve that invariant. This
  theorem applies to pure idle drain. It does not apply to controlled abandoned
  retirement, where unresolved abandoned credits deliberately remain held.
- `ReservationCore.tla` proves unique reservation per slot and per worktree,
  and that every physical process retains a reservation. Marking termination
  changes no reservation; forgetting is permitted only after physical exit.
  Unique slots give a process bound when the chosen slot set is finite.

Each module has 34 obligations covering initialization, all next-state cases
including stuttering, and temporal invariance. The proofs trust TLAPS's SMT
and temporal backends. Removing acquire/drain exclusion or the
exit-before-forget guard must leave an unproved obligation.

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
| RestartHandoffSafety | 2 inherited frontends and 1 abstract worktree start | 32 |
| RestartHandoffLiveness | same, fair decision and recovery | 32 |
| PayloadSafety | 3 call identities and 3 fixed-size payload charges | 171 |

The counts cover reachable states for each configuration. Checker logs retain
TLC's fingerprint collision estimates.

Ten safety mutations must fail: busy drain, early slot reuse, stale token,
unowned signal, early idle, cancellation releasing credit, disconnect releasing
credit, wrong local completion, same-connection ID reuse, and wrong-generation
credit release. The maintenance and noncooperative-exit liveness negatives and
two proof mutations must also fail for their intended reason. Four additional
retirement safety mutations must fail: retire active work, skip abandonment
grace, release credits on retirement, and reuse a slot before confirmed exit.
Disabling retirement and disabling cooperative exit each produce a retirement
liveness counterexample. The handoff and payload mutations below bring the
complete runner to 24 safety negatives, 5 liveness negatives and 10 rejected
proof mutations.

## Payload accounting abstraction

`PayloadAccounting.tla` checks conservation of fixed-size charges when queued
calls cancel and output moves from queue to writer. Payload credit survives
cancellation until discard and survives dequeue until write return. Output
discard refunds queued output only. Mutations check overbooking and early
refund at cancellation, dequeue and discard.

In Go, `retainMessage` stores a release-once reservation in the SDK's local
`Extra` field. Variable-size initialize and SSE reservations, ID limits,
transient copies and heap usage require separate evidence.
[Runtime budgets](../SPEC.md#resource-ledger)

## Hot restart handoff

`RestartHandoff.tla` starts with a prepared successor and fenced inherited
frontends. Commit authorizes the successor to wait for the previous owner's
exit evidence. Only observed lifetime-pipe EOF releases the extra reservation
and permits worktree starts and frontend recovery. A failed handoff leaves
frontend admission closed. After EOF the HTTP gate can open, while general
admission waits for every inherited peer's resume attempt to settle. An
individual resume failure represents that peer closing.

`RestartHandoffLiveness` checks eventual completion or failure. It assumes a
commit decision, an EOF observation or failure before recovery, and weak
fairness of recovery, resume attempts and finalization. The safety mutations
refund capacity, start work, reopen HTTP or resume peers on commit alone. They
also test reopening admission after failure or before resume attempts settle.
Disabling resume progress violates liveness.

The model abstracts fork/exec, descriptor inheritance, deadline duration,
the distinction between timeout and pipe/cancellation errors, and individual
worktree startup outcomes. Native tests check the runtime's interpretation of
EOF and failure, and exercise actual descriptor transfer.

## Runtime conformance

Symbols identify where Go enforces each obligation. Regression tests exercise
those boundaries. The checked refinements relate models to each other.

| Obligation | Implementation symbols | Inspected regression evidence and scope |
| --- | --- | --- |
| Unique generation/worktree; shared capacity | `supervisor.ensureRecord`, `startGeneration`, `generations` under `supervisor.mu` | `TestSupervisorTwoBridgesAndHTTPShareAndReconnectAfterIdle` exercises two bridges plus HTTP sharing/replacement; `TestSupervisorGlobalAdmissionDoesNotMultiplyWithRouters` checks global operation admission |
| Queued admission competes atomically with drain | `router.admit`, `bindWorktree`, `beginOperation`; `supervisor.admit`, `bind`, `begin`, `protectedLocked`, `drain` share owner mutex | `TestSupervisorQueuedAndActiveWorkSurviveIdle`; queued reservations protect their resolved worktree, active operations remain protected after two hours |
| Pure idle boundary; passive attachments do not pin | `ownedUpstream.available`, `supervisor.idleSweep`, `drain` recheck completion/protection under mutex | `TestSupervisorIdleBoundaryAndQuietConnections`; `TestSupervisorIdleCandidateRechecksLastCompletion` covers completion after candidate selection |
| Same-boot abandoned retirement preserves credits | `router.removeLocked`, `abandonOperationLocked`, `clearRequests`; `supervisor.abandon`, `protectedLocked`, `drain` | `TestSupervisorAbandonedOperationsRetireOnlyAfterGrace` checks full grace, retained credit through drain, then exit release. `AbandonedRetirement` models this policy separately from pure idle |
| Definitive result releases exact operation | `operationKey{conn,id}`, `callState`, `router.ownerLocked`, `finish`, `supervisor.completeOperationLocked`; `ownedUpstream.Read`, `supervisor.answered` | `TestDeliveryCompletionCannotTouchReusedID`, `TestAbandonedIDCannotBeReusedOnItsConnection`, `TestCancelledQueueEntryCannotClaimReusedID`, `TestDelayedCancellationCannotCancelReusedID` and `TestResponseReadCannotReleaseIDBeforeWaiterClaim` cover local-generation/connection reuse and atomic response claiming |
| Advisory cancellation preserves exact request identity | `router.cancelRequest`, `queueCancellation`, `lane.sendCancellation`, `supervisor.staleCancellation` | The queue carries the parsed ID, connection and admission identity. `TestCancellationCannotOvertakeItsCall` checks the original notification parameters and write order. `TestCancellationWriteFencesIDReuse` checks the write gate |
| Cancellation/timeout/disconnect do not prove completion | `router.startExecution`, `removeLocked`, `clearRequests`; `ownedUpstream.Close`, `supervisor.abandon` | `TestAbandonedOperationsBoundFurtherDelivery`, `TestDisconnectedOperationsDoNotReleaseCredits`, `TestExecutionTimeoutQueuesAdvisoryCancellation` preserve unresolved credits. Local abandonment starts retirement grace; it does not release credit |
| Uncertain delivery is not replayed as a new execution | `router.beginOperation`, transport delivery certainty, `supervisor.rejectedBeforeDelivery` | `TestSupervisorUncertainPostDoesNotReplayExecutedTool` executes once, loses POST acknowledgement, then observes abandoned ownership |
| Signals require owned identity; TERM is not exit | `startOwnedGuardian`, `guardianChild.Signal`, `guardianController`, `runGuardian`, `supervisor.terminate`, `fenceUncertain` | Guardian OS tests and `TestGuardianCleanupProofDoesNotReleaseBeforeGuardianExit` distinguish child/group proof from guardian Wait. Unexpected guardian EOF/death is uncertainty |
| No slot/FD refund while startup ownership uncertain | `guardianStartError.Released`, stable port lifetime lease, `startOwnedGuardian`, `supervisor.maintenanceSweep`, `confirmExited` | `TestGuardianPartialStartReturnsRetainedCapacityContract`, `TestGuardianPreForkCrashReconcilesOnlyAfterLeaseRelease`, `TestGuardianInheritedLifetimeLeaseSurvivesParentCopyClosing` |
| Boot/session fencing and conservative restart | `ownerNonce`, `startGeneration`, `connection`, `available`; private versioned Unix owner endpoint; `withSupervisorSingleton`, `guardianRecovery`, `validateOwnerRegistry` | `TestSupervisorProtocolAndConfigMismatchFailBeforeAllocation`, `TestGuardianRecoveryKeepsPositivePIDAndLegacyUncertainty`; a complete two-owner stale-session/crash-window runtime matrix remains a separate integration obligation |
| Storage errors cannot erase reservations | `supervisor.terminate` persists matching boot/generation before signal; `confirmExited` retains generation/FD quota until registry removal succeeds | `TestSupervisorCorruptRegistryNeverLosesReservations` covers malformed storage. Native injected failures at every registry/manifest fsync/rename boundary are not established by TLC stuttering |
| Finite FD allocation | `supervisorFDAllowance`, `reserveGenerationFDs`, `countWatchDescriptors`, child RLIMIT and guardian allowance | Runtime budget/OS pressure tests must establish the concrete bounds; `ReservationCore` proves only abstract retained-slot containment |
| Payload and ID ownership independent of operation credits | `validOwnerID`, `reserveMessageBytes`, `retainMessage`, `releaseMessage`, `releaseRetainedMessages`, `writeToClient` | `FuzzRetainedPayloadAccounting` compares actual queue credits and bytes with an ownership ledger. `TestSupervisorCancellationDoesNotRefundQueuedPayload`, `TestSupervisorBlockedWriterKeepsPayloadChargedAcrossChurn` and `TestSupervisorOpaqueIDBoundaryAndControls` check production boundaries |
| HTTP cold-queue teardown releases retained payloads | `httpBackend.start`, `httpBackend.close` | `TestHTTPBackendShutdownReleasesColdQueue` checks cancel and discard of active and queued cold calls before backend close returns |
| Hot restart exit proof and admission fence | `restoreRestartSessions`, `awaitPreviousOwner`, `resumeAfterRestart`, `frontendAdmission`, `restartHTTPBlocked` | `TestRestartPreviousOwnerReservationRequiresEOF` and `TestRestartHandoffFailsWithoutPreviousOwnerExit` check EOF and fail-closed handoff. `TestRestartReopensAdmissionAfterPeerResumeSettles` checks resume order and `TestRestartNativeForkExecPreservesAttachmentAndLock` checks native transfer |
| Server requests/handshakes share protection/accounting | `supervisor.serverControl`, `ownedUpstream.Write`, `Read` | `TestSupervisorOpaqueIDBoundaryAndControls`, `TestSupervisorUnknownOrFailedControlCannotRefreshIdle`; finite accounting model omits these message classes |
| One bounded shared filesystem worker | `ownerFilesystemCall`, `supervisor.filesystem`, `router.lookupWorktrees`, `router.planRoots`, `router.answerRoots`, `ownedUpstream.Write` | `TestSupervisorSharedFilesystemWorkerSurvivesSessionChurnBoundedly` and `TestRoutingBudgetDoesNotMultiplyOrGrowWorkers` simulate a worker held by an uncancellable syscall |

### Composed-core implementation mapping

The owner mutex serializes state transitions. Network, process and storage
effects run outside it.

| Core action/property | Runtime boundary and regression |
| --- | --- |
| Reserve / Start / Publish | `ensureRecord` reserves the worktree under mu; `prepareGeneration` reserves FD/port capacity and performs start/publication/readiness effects; `startGeneration` rechecks shutdown under mu before Ready. `TestSupervisorConcurrentStartsReserveDistinctPorts`, `TestSupervisorShutdownDuringStartCannotPublishReady` |
| Fence / UncertaintyRetains | `fenceUncertain` removes readiness while retaining remote credits and generation/FD capacity. `TestSupervisorGuardianLossRetainsCapacityThroughStorageFailure`, `TestSupervisorPartialStartKeepsLeaseUntilReconciliation` |
| ObserveExit / RemoveRecord / Forget | Guardian confirmation or an unstarted released lease authorizes `confirmExited`; its cleaning flag serializes durable cleanup, and only successful cleanup removes the generation/refunds FDs. `TestSupervisorExitCleanupHasOneOwner`, `TestSupervisorStartupFailureCleanup` |
| ExactFinish | Operations include exact connection plus wire ID and are bound to the generation. Caller/control namespaces remain separate. `TestSupervisorLateResponseCannotCompleteReplacement`, `TestSupervisorCallerAndControlNamespacesRemainDistinct` |
| Hold / Abandon / Expire / IdleDrain | Queued work, writes, active operations and unexpired abandonment grace feed `protectedLocked`; `drain` checks protection under mu. `TestSupervisorQueuedAndActiveWorkSurviveIdle`, `TestSupervisorAbandonedOperationsRetireOnlyAfterGrace` |

Applying these obligations to Go requires exact operation tokens, fresh
generation IDs, truthful exit and durable-cleanup evidence, full grace
intervals and critical sections that implement the abstract actions.

### Refinements and limits

- `SupervisorLifecycle` combines reservation with spawn and confirmed exit
  with durable cleanup. `LifecycleCore` separates these steps. In Go, exit can
  release operation credits while a failed registry update retains slot and FD
  capacity.
- `SupervisorLifecycle.Recover` is atomic. Go requires the singleton lock,
  guardian recovery and registry validation before starting a new boot.
- Queue reservations bind to a worktree before exact generation delivery.
  Admission after an old generation entered drain reserves future replacement
  work and waits for its retained generation to exit. This is more detailed
  than the lifecycle model's single Admit transition.
- The operation models omit notifications, private handshakes and server
  controls. Go tests cover their queue, write and credit lifetimes.
- Guardian identity depends on private inherited pipes, direct-child Wait,
  bounded group observation and inherited flock lifetime. Detected escapes
  retain uncertainty. The models assume these OS primitives and do not cover
  arbitrary escaped descendants or telemetry sidecar suppression.
- Finite generations never recycle and model clocks saturate. Runtime evidence
  must cover resource arithmetic, decoded amplification, blocked workers and
  long-run map, log, inode, heap and goroutine retention.

Runtime release checks still need native FD/process pressure, crash injection
at every durable boundary, and a default-idle run beyond 70 minutes including
suspend/resume. The named regressions cover active-work and grace boundaries.
Ignored TERM with early slot reuse, delayed responses through replacement,
and crash/storage interleavings need broader runtime coverage. Conformance is
reviewed against code and tests. The runner does not replay runtime traces
against the models.

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
