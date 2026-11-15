------------------------- MODULE SupervisorLifecycle -------------------------
EXTENDS Naturals, FiniteSets

CONSTANTS Trees, Clients, Generations, Capacity, OperationCapacity, IdleTicks,
          MaxBoot, CooperativeExit, Mutation
ASSUME /\ Trees # {} /\ Clients # {} /\ Generations # {}
       /\ Capacity \in Nat \ {0} /\ OperationCapacity \in Nat \ {0}
       /\ IdleTicks \in Nat \ {0} /\ MaxBoot \in Nat \ {0}
       /\ CooperativeExit \in BOOLEAN
       /\ Mutation \in {"none", "busy-drain", "early-slot", "stale-token",
                        "unowned-signal", "early-idle", "never-drain"}

Instances == Trees \X Generations
States == {"Absent", "Starting", "Ready", "Draining", "Terminating", "Exited"}
VARIABLES phase, operations, attachments, born, age, owned, boot, online,
          accepting, unsafeDrain, unsafeSignal, unsafeToken, unsafeIdle
vars == <<phase, operations, attachments, born, age, owned, boot, online,
          accepting, unsafeDrain, unsafeSignal, unsafeToken, unsafeIdle>>

Live == {i \in Instances : phase[i] \notin {"Absent", "Exited"}}
Charged == IF Mutation = "early-slot"
           THEN {i \in Live : phase[i] \notin {"Draining", "Terminating"}}
           ELSE Live
Ops(i) == {p \in operations : p[1] = i}
Tickets == Instances \X Clients \X (1..MaxBoot)

Init == /\ phase = [i \in Instances |-> "Absent"]
        /\ operations = {} /\ attachments = {}
        /\ born = [i \in Instances |-> 0]
        /\ age = [i \in Instances |-> 0]
        /\ owned = [i \in Instances |-> FALSE]
        /\ boot = 1 /\ online = TRUE /\ accepting = TRUE
        /\ unsafeDrain = FALSE /\ unsafeSignal = FALSE
        /\ unsafeToken = FALSE /\ unsafeIdle = FALSE

Spawn(i) ==
    /\ online /\ accepting /\ phase[i] = "Absent"
    /\ Cardinality(Charged) < Capacity
    /\ ~\E j \in Charged : j[1] = i[1]
    /\ phase' = [phase EXCEPT ![i] = "Starting"]
    /\ born' = [born EXCEPT ![i] = boot]
    /\ owned' = [owned EXCEPT ![i] = TRUE]
    /\ UNCHANGED <<operations, attachments, age, boot, online, accepting,
                   unsafeDrain, unsafeSignal, unsafeToken, unsafeIdle>>

Ready(i) ==
    /\ online /\ phase[i] = "Starting" /\ born[i] = boot
    /\ phase' = [phase EXCEPT ![i] = "Ready"]
    /\ age' = [age EXCEPT ![i] = 0]
    /\ UNCHANGED <<operations, attachments, born, owned, boot, online, accepting,
                   unsafeDrain, unsafeSignal, unsafeToken, unsafeIdle>>

Attach(i, c) ==
    /\ online /\ accepting /\ phase[i] = "Ready" /\ born[i] = boot
    /\ attachments' = attachments \cup {<<i, c, boot>>}
    /\ UNCHANGED <<phase, operations, born, age, owned, boot, online, accepting,
                   unsafeDrain, unsafeSignal, unsafeToken, unsafeIdle>>

Admit(i, c, b) ==
    /\ online /\ accepting /\ phase[i] = "Ready" /\ born[i] = boot
    /\ IF Mutation = "stale-token"
       THEN \E j \in Instances : <<j, c, b>> \in attachments
       ELSE /\ <<i, c, b>> \in attachments /\ b = boot
    /\ <<i, c>> \notin operations
    /\ Cardinality(operations) < OperationCapacity
    /\ operations' = operations \cup {<<i, c>>}
    /\ unsafeToken' = (unsafeToken \/ b # boot \/ <<i, c, b>> \notin attachments)
    /\ UNCHANGED <<phase, attachments, born, age, owned, boot, online, accepting,
                   unsafeDrain, unsafeSignal, unsafeIdle>>

Finish(i, c) ==
    /\ <<i, c>> \in operations
    /\ operations' = operations \ {<<i, c>>}
    /\ age' = [age EXCEPT ![i] = 0]
    /\ UNCHANGED <<phase, attachments, born, owned, boot, online, accepting,
                   unsafeDrain, unsafeSignal, unsafeToken, unsafeIdle>>

Tick == /\ online
        /\ age' = [i \in Instances |->
              IF phase[i] = "Ready" THEN IF age[i] < IdleTicks
                                           THEN age[i] + 1 ELSE age[i]
              ELSE age[i]]
        /\ UNCHANGED <<phase, operations, attachments, born, owned, boot, online,
                       accepting, unsafeDrain, unsafeSignal, unsafeToken, unsafeIdle>>

IdleDrain(i) ==
    /\ online /\ phase[i] = "Ready"
    /\ Mutation # "never-drain"
    /\ (Mutation = "busy-drain" \/ Ops(i) = {})
    /\ (Mutation = "early-idle" \/ age[i] = IdleTicks)
    /\ phase' = [phase EXCEPT ![i] = "Draining"]
    /\ unsafeDrain' = (unsafeDrain \/ Ops(i) # {})
    /\ unsafeIdle' = (unsafeIdle \/ age[i] < IdleTicks)
    /\ UNCHANGED <<operations, attachments, born, age, owned, boot, online,
                   accepting, unsafeSignal, unsafeToken>>

Terminate(i) ==
    /\ online /\ phase[i] = "Draining"
    /\ (Mutation = "unowned-signal" \/ owned[i])
    /\ phase' = [phase EXCEPT ![i] = "Terminating"]
    /\ unsafeSignal' = (unsafeSignal \/ ~owned[i])
    /\ UNCHANGED <<operations, attachments, born, age, owned, boot, online,
                   accepting, unsafeDrain, unsafeToken, unsafeIdle>>

ObserveExit(i) ==
    /\ online /\ i \in Live
    /\ ((phase[i] = "Terminating" /\ CooperativeExit) \/ ~owned[i])
    /\ phase' = [phase EXCEPT ![i] = "Exited"]
    /\ owned' = [owned EXCEPT ![i] = FALSE]
    /\ operations' = {p \in operations : p[1] # i}
    /\ UNCHANGED <<attachments, born, age, boot, online, accepting,
                   unsafeDrain, unsafeSignal, unsafeToken, unsafeIdle>>

\* The child exits and its PID may be reused before the registry notices.
LoseIdentity(i) ==
    /\ i \in Live /\ owned[i]
    /\ owned' = [owned EXCEPT ![i] = FALSE]
    /\ UNCHANGED <<phase, operations, attachments, born, age, boot, online,
                   accepting, unsafeDrain, unsafeSignal, unsafeToken, unsafeIdle>>

Crash == /\ online /\ boot < MaxBoot
         /\ online' = FALSE
         /\ UNCHANGED <<phase, operations, attachments, born, age, owned, boot,
                        accepting, unsafeDrain, unsafeSignal, unsafeToken, unsafeIdle>>

\* Recovery closes/fences old delivery transports before making the new boot live.
\* Old operations survive until definitive completion or observed server exit.
Recover ==
    /\ ~online /\ boot < MaxBoot
    /\ online' = TRUE /\ boot' = boot + 1
    /\ phase' = [i \in Instances |-> IF i \in Live THEN "Draining" ELSE phase[i]]
    /\ UNCHANGED <<operations, attachments, born, age, owned, accepting,
                   unsafeDrain, unsafeSignal, unsafeToken, unsafeIdle>>

\* Renewing a session can issue a current token; never changes idle age.
Renew(i, c) == Attach(i, c)
Quiesce == /\ accepting /\ accepting' = FALSE
           /\ UNCHANGED <<phase, operations, attachments, born, age, owned, boot,
                          online, unsafeDrain, unsafeSignal, unsafeToken, unsafeIdle>>

Next == \/ (\E i \in Instances : Spawn(i) \/ Ready(i) \/ IdleDrain(i)
                                  \/ Terminate(i) \/ ObserveExit(i) \/ LoseIdentity(i))
        \/ (\E i \in Instances, c \in Clients : Attach(i,c) \/ Finish(i,c))
        \/ (\E i \in Instances, c \in Clients, b \in 1..MaxBoot : Admit(i,c,b))
        \/ Tick \/ Crash \/ Recover \/ Quiesce

TypeOK == /\ phase \in [Instances -> States]
          /\ operations \subseteq Instances \X Clients
          /\ attachments \subseteq Tickets
          /\ born \in [Instances -> 0..MaxBoot]
          /\ age \in [Instances -> 0..IdleTicks]
          /\ owned \in [Instances -> BOOLEAN]
          /\ boot \in 1..MaxBoot /\ online \in BOOLEAN /\ accepting \in BOOLEAN
          /\ unsafeDrain \in BOOLEAN /\ unsafeSignal \in BOOLEAN
          /\ unsafeToken \in BOOLEAN /\ unsafeIdle \in BOOLEAN
Safety == /\ Cardinality(Live) <= Capacity
          /\ Cardinality(operations) <= OperationCapacity
          /\ (\A t \in Trees : Cardinality({i \in Live : i[1] = t}) <= 1)
          /\ (\A p \in operations : p[1] \in Live)
          /\ ~unsafeDrain /\ ~unsafeSignal /\ ~unsafeToken /\ ~unsafeIdle

Spec == Init /\ [][Next]_vars
FairSpec == Spec /\ WF_vars(Tick) /\ WF_vars(Recover)
            /\ (\A i \in Instances : WF_vars(Ready(i)) /\ WF_vars(IdleDrain(i))
                                     /\ WF_vars(Terminate(i)) /\ WF_vars(ObserveExit(i)))
ResponseFairSpec == FairSpec /\ (\A i \in Instances, c \in Clients : WF_vars(Finish(i,c)))
QuiescenceReclaims == ~accepting ~> Live = {}
PureIdleReclaims == \A i \in Instances :
    (~accepting /\ phase[i] = "Ready" /\ Ops(i) = {}) ~> phase[i] = "Exited"
=============================================================================
