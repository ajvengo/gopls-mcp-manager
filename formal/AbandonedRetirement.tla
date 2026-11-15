------------------------ MODULE AbandonedRetirement -------------------------
EXTENDS Naturals, FiniteSets

CONSTANTS Generations, Clients, Capacity, OperationCapacity, IdleTicks,
          CooperativeExit, Mutation
ASSUME /\ Generations # {} /\ Clients # {}
       /\ Capacity \in Nat \ {0} /\ OperationCapacity \in Nat \ {0}
       /\ IdleTicks \in Nat \ {0} /\ CooperativeExit \in BOOLEAN
       /\ Mutation \in {"none", "active-retire", "early-abandon",
                        "retire-releases", "early-slot", "never-retire"}

\* One worktree, one unchanged owner boot. Generation identities are never
\* reused. A credit is admitted before delivery; local completion does not release it.
\* Cancellation/timeout/disconnect start grace but preserve the held credit.
Keys == Generations \X Clients
States == {"Absent", "Ready", "Draining", "Terminating", "Exited"}
VARIABLES phase, operations, abandoned, grace, idle, accepting,
          unsafeRetire, unsafeRelease
vars == <<phase, operations, abandoned, grace, idle, accepting,
          unsafeRetire, unsafeRelease>>

Live == {g \in Generations : phase[g] \notin {"Absent", "Exited"}}
Charged == IF Mutation = "early-slot"
           THEN {g \in Live : phase[g] = "Ready"} ELSE Live
Ops(g) == {k \in operations : k[1] = g}
Protected(g) == {k \in Ops(g) : k \notin abandoned \/ grace[k] < IdleTicks}

Init == /\ phase = [g \in Generations |-> "Absent"]
        /\ operations = {} /\ abandoned = {}
        /\ grace = [k \in Keys |-> 0]
        /\ idle = [g \in Generations |-> 0]
        /\ accepting = TRUE /\ unsafeRetire = FALSE /\ unsafeRelease = FALSE

Spawn(g) ==
    /\ accepting /\ phase[g] = "Absent" /\ Charged = {}
    /\ Cardinality(Charged) < Capacity
    /\ phase' = [phase EXCEPT ![g] = "Ready"]
    /\ UNCHANGED <<operations, abandoned, grace, idle, accepting,
                   unsafeRetire, unsafeRelease>>

Admit(g,c) ==
    /\ accepting /\ phase[g] = "Ready" /\ <<g,c>> \notin operations
    /\ Cardinality(operations) < OperationCapacity
    /\ operations' = operations \cup {<<g,c>>}
    /\ grace' = [grace EXCEPT ![<<g,c>>] = 0]
    /\ UNCHANGED <<phase, abandoned, idle, accepting, unsafeRetire, unsafeRelease>>

Abandon(g,c) ==
    /\ <<g,c>> \in operations \ abandoned
    /\ abandoned' = abandoned \cup {<<g,c>>}
    /\ grace' = [grace EXCEPT ![<<g,c>>] = 0]
    /\ UNCHANGED <<phase, operations, idle, accepting, unsafeRetire, unsafeRelease>>

\* A definitive result releases exactly its own generation/connection credit.
Finish(g,c) ==
    /\ <<g,c>> \in operations
    /\ operations' = operations \ {<<g,c>>}
    /\ abandoned' = abandoned \ {<<g,c>>}
    /\ idle' = [idle EXCEPT ![g] = 0]
    /\ UNCHANGED <<phase, grace, accepting, unsafeRetire, unsafeRelease>>

Tick ==
    /\ idle' = [g \in Generations |->
          IF phase[g] = "Ready" /\ idle[g] < IdleTicks
          THEN idle[g] + 1 ELSE idle[g]]
    /\ grace' = [k \in Keys |->
          IF k \in abandoned /\ grace[k] < IdleTicks
          THEN grace[k] + 1 ELSE grace[k]]
    /\ UNCHANGED <<phase, operations, abandoned, accepting, unsafeRetire, unsafeRelease>>

\* This is controlled retirement of abandoned work, not proof it completed.
\* Passive connections are omitted: they never appear in Protected(g).
Retire(g) ==
    /\ Mutation # "never-retire" /\ phase[g] = "Ready" /\ idle[g] = IdleTicks
    /\ IF Mutation = "active-retire" THEN TRUE
       ELSE IF Mutation = "early-abandon" THEN Ops(g) \subseteq abandoned
       ELSE Protected(g) = {}
    /\ phase' = [phase EXCEPT ![g] = "Draining"]
    /\ unsafeRetire' = (unsafeRetire \/ Protected(g) # {})
    /\ operations' = IF Mutation = "retire-releases"
                      THEN operations \ Ops(g) ELSE operations
    /\ abandoned' = IF Mutation = "retire-releases"
                     THEN abandoned \ Ops(g) ELSE abandoned
    /\ unsafeRelease' = (unsafeRelease \/
                           (Mutation = "retire-releases" /\ Ops(g) # {}))
    /\ UNCHANGED <<grace, idle, accepting>>

Terminate(g) ==
    /\ phase[g] = "Draining"
    /\ phase' = [phase EXCEPT ![g] = "Terminating"]
    /\ UNCHANGED <<operations, abandoned, grace, idle, accepting,
                   unsafeRetire, unsafeRelease>>

\* This observation assumes actual child+owned-group exit proof, not TERM/EOF.
ObserveExit(g) ==
    /\ phase[g] = "Terminating" /\ CooperativeExit
    /\ phase' = [phase EXCEPT ![g] = "Exited"]
    /\ operations' = operations \ Ops(g)
    /\ abandoned' = abandoned \ Ops(g)
    /\ UNCHANGED <<grace, idle, accepting, unsafeRetire, unsafeRelease>>

Quiesce == /\ accepting /\ accepting' = FALSE
           /\ UNCHANGED <<phase, operations, abandoned, grace, idle,
                          unsafeRetire, unsafeRelease>>

Next == \/ (\E g \in Generations : Spawn(g) \/ Retire(g) \/ Terminate(g) \/ ObserveExit(g))
        \/ (\E g \in Generations, c \in Clients : Admit(g,c) \/ Abandon(g,c) \/ Finish(g,c))
        \/ Tick \/ Quiesce

TypeOK == /\ phase \in [Generations -> States]
          /\ operations \subseteq Keys /\ abandoned \subseteq operations
          /\ grace \in [Keys -> 0..IdleTicks]
          /\ idle \in [Generations -> 0..IdleTicks]
          /\ accepting \in BOOLEAN /\ unsafeRetire \in BOOLEAN /\ unsafeRelease \in BOOLEAN
Safety == /\ Cardinality(Live) <= Capacity /\ Cardinality(Live) <= 1
          /\ Cardinality(operations) <= OperationCapacity
          /\ (\A k \in operations : k[1] \in Live)
          /\ (\A g \in Live : phase[g] # "Ready" => Ops(g) \subseteq abandoned)
          /\ ~unsafeRetire /\ ~unsafeRelease

Spec == Init /\ [][Next]_vars
FairSpec == Spec /\ WF_vars(Tick)
            /\ (\A g \in Generations : WF_vars(Retire(g)) /\ WF_vars(Terminate(g))
                                     /\ WF_vars(ObserveExit(g)))
\* No fairness of Finish or Abandon is assumed. Already abandoned or empty
\* work is eventually reclaimed; a live unresponsive operation remains held.
AbandonedReclaims == (~accepting /\ operations \subseteq abandoned) ~> Live = {}
=============================================================================
