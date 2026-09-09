# Cross-client leases before eviction

Status: supervisor design only. Recorded-spawn admission and router-local
operation credits are implemented; automatic eviction and leases are not.

## Decision

Keep shared gopls processes; refuse new spawns at
`GOPLS_MANAGER_MAX_SERVERS` (default 64 recorded servers). This policy is checked
under the same lock as spawn/reservation and preserves existing endpoint reuse.
It is a configurable admission choice, not a measured RAM recommendation. It
requires cooperating managers with the same configuration and intact records.
`status` reports registered server count, identity-checked RSS in KiB,
and termination state. `ActiveClients: null` means unknown, not zero.
`GOPLS_MANAGER_METRICS=1` reports per-operation registry lock wait/hold durations
and manager operation timings to stderr. Session snapshots every 30 s and at
exit include request counts, rejection reasons, stage timings and retention
counts. Request counts describe the bridge, not all clients of a shared server.
Lane and memo limits bound each bridge; they do not authorize process eviction.

Delivered requests acquire separate operation credits before Write. A terminal
upstream response releases a credit; local cancellation, timeout and uncertain
connection failure do not. Counts are bounded by the configured session and
per-worktree outstanding ceilings. A retry on another connection acquires another
credit. Lost-response credits remain until router shutdown, which only forgets
local accounting and is not evidence of server completion. No cross-client or
cross-restart operation guarantee is claimed.

Local lane turnover was evaluated and remains disabled. Retirement would need
to drain delivery/control queues, account for private handshakes, join readers,
and preserve unresolved-operation identity. Closing an apparently idle lane
would not reclaim the shared gopls index. The bounded retained-lane policy is
kept until that added lifecycle complexity has a measured benefit; at capacity,
new worktrees fail explicitly and existing lanes remain usable.

A per-bridge idle timer is insufficient: another bridge may still be executing
a call against the same gopls. A timeout or terminal local cancellation does not
prove that the upstream operation stopped. RSS snapshots do not prove inactivity.

## Proposed ownership model

Use a local supervisor as the sole process owner if measurements justify
eviction. It retains process handles and exit notifications, replacing the
cross-process PID-only cleanup authority. Bridges keep their current routing
lanes and obtain an attachment lease before dialing a server generation.

An attachment identifies `(server generation, bridge session nonce)`; neither
PID nor worktree path alone is a generation. The supervisor stores server state
and leases durably in one transaction domain. A lease includes bridge identity,
renewal sequence, and expiry. Generation and session identifiers are random,
not timestamps. A new supervisor boot fences all previous attachment tokens.

Track attached sessions separately from outstanding operations. Bridges acquire
an operation token before delivery and release it only on a definitive upstream
result or confirmed server teardown. Closing a connection alone is insufficient.
Locally abandoned calls retain operation
ownership until the server confirms completion or the session is fenced.

## Safe eviction sequence

1. Select a candidate only when no attachment lease and no operation token is
   live. A disconnected or suspended bridge alone is not proof of idle work.
2. Atomically transition that generation from ready to draining, refusing new
   leases. A concurrent attachment either wins before draining or retries after
   replacement; it cannot silently attach to the retiring generation.
3. Revalidate the generation and owned process handle, terminate outside the
   metadata transaction, wait for confirmed exit, then remove its record.
4. Retain a terminating record on any signal/wait error. Never count it as free
   memory or hand out its endpoint as ready.
5. Create a new generation on the next attachment, keeping reservation and spawn
   exclusive so concurrent clients start exactly one server.

Lease expiry is a failure detector, not proof of safety. Before reclaiming an
expired session's operations, fence its connection so it cannot continue issuing
work; if work cannot be proven complete, retire the entire server generation.
This disrupts callers, so make that policy explicit rather than hiding it behind
an idle timeout. Choose renewal/expiry intervals from observed suspend and
reconnect behavior, not arbitrary constants in this implementation.

## Compatibility and validation

Introduce a versioned supervisor protocol and refuse unsupported lease versions.
During migration, legacy clients prevent automatic eviction: they cannot report
attachment or operation ownership. Do not infer their absence from lease files.
All managers sharing today's registry must be upgraded before relying on the
new `Terminating` field; older binaries ignore it.

Before enabling eviction, test two bridges sharing one server, simultaneous
acquire/drain, cancelled-but-still-running work, supervisor restart, stale lease
renewal, clock changes and laptop suspend, PID reuse, failed SIGTERM/SIGKILL,
and registry write failures. Measure cold-index cost, RSS recovery, attachment
counts and request latency under realistic worktree churn. Set the memory or
eviction policy only after that evidence is available. The implemented spawn
ceiling does not enable eviction.

Current choice: operator-managed lifetime with explicit spawn admission. It
avoids supervisor recovery and protocol migration but leaves reclamation manual.
