------------------------- MODULE OperationAccounting -------------------------
EXTENDS Naturals, FiniteSets
CONSTANTS Clients, Connections, MaxRequest, Credits, Mutation
ASSUME /\ Clients # {} /\ Connections # {}
       /\ MaxRequest \in Nat \ {0} /\ Credits \in Nat \ {0}
       /\ Mutation \in {"none", "cancel-releases", "disconnect-releases",
                        "wrong-completion", "id-reuse", "wrong-credit"}
Requests == Clients \X (1..MaxRequest)
OperationKeys == Requests \X Connections
VARIABLES generation, local, bound, replies, executing, tracked, active,
          serverAlive, unsafeCompletion, unsafeReuse
vars == <<generation, local, bound, replies, executing, tracked, active,
          serverAlive, unsafeCompletion, unsafeReuse>>

Init == /\ generation = [c \in Clients |-> 0]
        /\ local = [c \in Clients |-> "idle"]
        /\ bound = [c \in Clients |-> "none"]
        /\ replies = [r \in Requests |-> 0]
        /\ executing = {} /\ tracked = {} /\ active = Connections
        /\ serverAlive = TRUE /\ unsafeCompletion = FALSE /\ unsafeReuse = FALSE

Admit(c) ==
    /\ serverAlive /\ generation[c] < MaxRequest /\ local[c] # "pending"
    /\ generation' = [generation EXCEPT ![c] = @ + 1]
    /\ local' = [local EXCEPT ![c] = "pending"]
    /\ bound' = [bound EXCEPT ![c] = "none"]
    /\ UNCHANGED <<replies, executing, tracked, active, serverAlive,
                   unsafeCompletion, unsafeReuse>>

\* One client wire ID per connection; new local generations may reuse it.
SameWire(c,k) == {p \in executing : p[1][1] = c /\ p[2] = k}
Reserve(c,k) ==
    LET r == <<c,generation[c]>> IN
    /\ serverAlive /\ local[c] = "pending" /\ k \in active
    /\ <<r,k>> \notin tracked /\ Cardinality(tracked) < Credits
    /\ (Mutation = "id-reuse" \/ SameWire(c,k) = {})
    /\ executing' = executing \cup {<<r,k>>}
    /\ tracked' = tracked \cup {<<r,k>>}
    /\ bound' = [bound EXCEPT ![c] = k]
    /\ unsafeReuse' = (unsafeReuse \/ SameWire(c,k) # {})
    /\ UNCHANGED <<generation, local, replies, active, serverAlive,
                   unsafeCompletion>>

Cancel(c) ==
    LET r == <<c,generation[c]>> IN
    /\ local[c] = "pending"
    /\ local' = [local EXCEPT ![c] = "cancelled"]
    /\ replies' = [replies EXCEPT ![r] = @ + 1]
    /\ tracked' = IF Mutation = "cancel-releases"
                   THEN {p \in tracked : p[1] # r} ELSE tracked
    /\ UNCHANGED <<generation, bound, executing, active, serverAlive,
                   unsafeCompletion, unsafeReuse>>

Disconnect(k) ==
    /\ k \in active /\ active' = active \ {k}
    /\ tracked' = IF Mutation = "disconnect-releases"
                   THEN {p \in tracked : p[2] # k} ELSE tracked
    /\ UNCHANGED <<generation, local, bound, replies, executing, serverAlive,
                   unsafeCompletion, unsafeReuse>>

Response(r,k) ==
    LET c == r[1]
        matches == r[2] = generation[c] /\ bound[c] = k
        complete == local[c] = "pending" /\
                    (Mutation = "wrong-completion" \/ matches)
        current == <<c,generation[c]>>
    IN /\ <<r,k>> \in executing
       /\ executing' = executing \ {<<r,k>>}
       /\ tracked' = IF Mutation = "wrong-credit"
                      THEN {p \in tracked : p[1][1] # c}
                      ELSE tracked \ {<<r,k>>}
       /\ local' = IF complete THEN [local EXCEPT ![c] = "done"] ELSE local
       /\ replies' = IF complete THEN [replies EXCEPT ![current] = @ + 1]
                      ELSE replies
       /\ unsafeCompletion' = (unsafeCompletion \/ (complete /\ ~matches))
       /\ UNCHANGED <<generation, bound, active, serverAlive, unsafeReuse>>

ObserveServerExit ==
    /\ serverAlive /\ serverAlive' = FALSE
    /\ executing' = {} /\ tracked' = {} /\ active' = {}
    /\ UNCHANGED <<generation, local, bound, replies, unsafeCompletion, unsafeReuse>>

Next == \/ (\E c \in Clients : Admit(c) \/ Cancel(c))
        \/ (\E c \in Clients, k \in Connections : Reserve(c,k))
        \/ (\E r \in Requests, k \in Connections : Response(r,k))
        \/ (\E k \in Connections : Disconnect(k))
        \/ ObserveServerExit
Spec == Init /\ [][Next]_vars
TypeOK == /\ generation \in [Clients -> 0..MaxRequest]
          /\ local \in [Clients -> {"idle", "pending", "done", "cancelled"}]
          /\ bound \in [Clients -> Connections \cup {"none"}]
          /\ replies \in [Requests -> 0..1]
          /\ executing \subseteq OperationKeys /\ tracked \subseteq OperationKeys
          /\ active \subseteq Connections /\ serverAlive \in BOOLEAN
          /\ unsafeCompletion \in BOOLEAN /\ unsafeReuse \in BOOLEAN
Safety == /\ executing = tracked /\ Cardinality(executing) <= Credits
          /\ ~unsafeCompletion /\ ~unsafeReuse
=============================================================================
