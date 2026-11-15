---------------------------- MODULE LifecycleCore ----------------------------
EXTENDS TLAPS
CONSTANTS Slots, Trees, Generations, Operations
Reservations == Slots \X Trees \X Generations
Keys == Operations \X Reservations
VARIABLES reserved, physical, exited, removed, ready, draining,
          held, protected, abandoned, retired
vars == <<reserved, physical, exited, removed, ready, draining,
          held, protected, abandoned, retired>>

\* Exit and retirement histories are ghost state; identities never recycle.
\* Physical means a process may remain, until trusted guardian/lease proof.
Init == /\ reserved = {} /\ physical = {} /\ exited = {} /\ removed = {}
        /\ ready = {} /\ draining = {} /\ held = {} /\ protected = {}
        /\ abandoned = {} /\ retired = {}

Reserve(r) ==
    /\ r \in Reservations \ exited
    /\ ~\E p \in reserved : p[1] = r[1] \/ p[2] = r[2]
    /\ reserved' = reserved \cup {r}
    /\ UNCHANGED <<physical, exited, removed, ready, draining,
                   held, protected, abandoned, retired>>
Start(r) ==
    /\ r \in reserved \ exited
    /\ physical' = physical \cup {r}
    /\ UNCHANGED <<reserved, exited, removed, ready, draining,
                   held, protected, abandoned, retired>>
Publish(r) ==
    /\ r \in physical \ draining
    /\ ready' = ready \cup {r}
    /\ UNCHANGED <<reserved, physical, exited, removed, draining,
                   held, protected, abandoned, retired>>
Hold(o,r) ==
    /\ o \in Operations /\ r \in ready /\ <<o,r>> \notin held
    /\ held' = held \cup {<<o,r>>}
    /\ protected' = protected \cup {<<o,r>>}
    /\ UNCHANGED <<reserved, physical, exited, removed, ready, draining,
                   abandoned, retired>>
Abandon(o,r) ==
    /\ <<o,r>> \in held
    /\ abandoned' = abandoned \cup {<<o,r>>}
    /\ UNCHANGED <<reserved, physical, exited, removed, ready, draining,
                   held, protected, retired>>
\* Expiry assumes the runtime has observed the full abandonment grace.
Expire(o,r) ==
    /\ <<o,r>> \in abandoned
    /\ protected' = protected \ {<<o,r>>}
    /\ UNCHANGED <<reserved, physical, exited, removed, ready, draining,
                   held, abandoned, retired>>
Finish(o,r) ==
    /\ <<o,r>> \in held
    /\ held' = held \ {<<o,r>>}
    /\ protected' = protected \ {<<o,r>>}
    /\ abandoned' = abandoned \ {<<o,r>>}
    /\ UNCHANGED <<reserved, physical, exited, removed, ready, draining, retired>>
IdleDrain(r) ==
    /\ r \in ready
    /\ ~\E o \in Operations : <<o,r>> \in protected
    /\ draining' = draining \cup {r}
    /\ retired' = retired \cup {r}
    /\ ready' = ready \ {r}
    /\ UNCHANGED <<reserved, physical, exited, removed, held, protected, abandoned>>
\* Guardian uncertainty fences delivery but proves neither exit nor completion.
Fence(r) ==
    /\ r \in reserved \ exited
    /\ draining' = draining \cup {r}
    /\ ready' = ready \ {r}
    /\ UNCHANGED <<reserved, physical, exited, removed,
                   held, protected, abandoned, retired>>
ObserveExit(r) ==
    /\ r \in reserved
    /\ exited' = exited \cup {r}
    /\ physical' = physical \ {r}
    /\ ready' = ready \ {r}
    /\ held' = {k \in held : k[2] # r}
    /\ protected' = {k \in protected : k[2] # r}
    /\ abandoned' = {k \in abandoned : k[2] # r}
    /\ UNCHANGED <<reserved, removed, draining, retired>>
RemoveRecord(r) ==
    /\ r \in exited
    /\ removed' = removed \cup {r}
    /\ UNCHANGED <<reserved, physical, exited, ready, draining,
                   held, protected, abandoned, retired>>
Forget(r) ==
    /\ r \in exited \cap removed
    /\ reserved' = reserved \ {r}
    /\ draining' = draining \ {r}
    /\ UNCHANGED <<physical, exited, removed, ready,
                   held, protected, abandoned, retired>>
Next == \/ (\E r \in Reservations : Reserve(r) \/ Start(r) \/ Publish(r)
             \/ IdleDrain(r) \/ Fence(r) \/ ObserveExit(r) \/ RemoveRecord(r) \/ Forget(r))
        \/ (\E o \in Operations, r \in Reservations :
               Hold(o,r) \/ Abandon(o,r) \/ Expire(o,r) \/ Finish(o,r))
Spec == Init /\ [][Next]_vars

TypeOK == /\ reserved \subseteq Reservations
          /\ exited \subseteq Reservations /\ removed \subseteq exited
          /\ exited \ reserved \subseteq removed
          /\ physical \subseteq reserved \ exited
          /\ ready \subseteq physical \ (draining \cup retired)
          /\ draining \subseteq reserved /\ retired \subseteq draining \cup exited
          /\ held \subseteq Operations \X (reserved \ exited)
          /\ protected \subseteq held /\ abandoned \subseteq held
Exclusive == \A p,q \in reserved :
               (p[1] = q[1] \/ p[2] = q[2]) => p = q
NoProtectedIdleDrain == \A k \in protected : k[2] \notin retired
Inv == TypeOK /\ Exclusive /\ NoProtectedIdleDrain

THEOREM Initial == Init => Inv
BY SMT DEF Init, Inv, TypeOK, Exclusive, NoProtectedIdleDrain

THEOREM Preserve == Inv /\ [Next]_vars => Inv'
<1>1. ASSUME NEW r \in Reservations, Inv, Reserve(r) PROVE Inv'
      BY SMT, Inv, Reserve(r) DEF Inv, TypeOK, Exclusive, NoProtectedIdleDrain, Reserve
<1>2. ASSUME NEW r \in Reservations, Inv, Start(r) PROVE Inv'
      BY SMT, Inv, Start(r) DEF Inv, TypeOK, Exclusive, NoProtectedIdleDrain, Start
<1>3. ASSUME NEW r \in Reservations, Inv, Publish(r) PROVE Inv'
      BY SMT, Inv, Publish(r) DEF Inv, TypeOK, Exclusive, NoProtectedIdleDrain, Publish
<1>4. ASSUME NEW o \in Operations, NEW r \in Reservations, Inv, Hold(o,r) PROVE Inv'
      BY SMT, Inv, Hold(o,r) DEF Inv, TypeOK, Exclusive, NoProtectedIdleDrain, Hold
<1>5. ASSUME NEW o \in Operations, NEW r \in Reservations, Inv, Abandon(o,r) PROVE Inv'
      BY SMT, Inv, Abandon(o,r) DEF Inv, TypeOK, Exclusive, NoProtectedIdleDrain, Abandon
<1>6. ASSUME NEW o \in Operations, NEW r \in Reservations, Inv, Expire(o,r) PROVE Inv'
      BY SMT, Inv, Expire(o,r) DEF Inv, TypeOK, Exclusive, NoProtectedIdleDrain, Expire
<1>7. ASSUME NEW o \in Operations, NEW r \in Reservations, Inv, Finish(o,r) PROVE Inv'
      BY SMT, Inv, Finish(o,r) DEF Inv, TypeOK, Exclusive, NoProtectedIdleDrain, Finish
<1>8. ASSUME NEW r \in Reservations, Inv, IdleDrain(r) PROVE Inv'
      BY SMT, Inv, IdleDrain(r) DEF Inv, TypeOK, Exclusive, NoProtectedIdleDrain, IdleDrain
<1>9. ASSUME NEW r \in Reservations, Inv, Fence(r) PROVE Inv'
      BY SMT, Inv, Fence(r) DEF Inv, TypeOK, Exclusive, NoProtectedIdleDrain, Fence
<1>10. ASSUME NEW r \in Reservations, Inv, ObserveExit(r) PROVE Inv'
       BY SMT, Inv, ObserveExit(r) DEF Inv, TypeOK, Exclusive, NoProtectedIdleDrain, ObserveExit
<1>11. ASSUME NEW r \in Reservations, Inv, RemoveRecord(r) PROVE Inv'
       BY SMT, Inv, RemoveRecord(r) DEF Inv, TypeOK, Exclusive, NoProtectedIdleDrain, RemoveRecord
<1>12. ASSUME NEW r \in Reservations, Inv, Forget(r) PROVE Inv'
       BY SMT, Inv, Forget(r) DEF Inv, TypeOK, Exclusive, NoProtectedIdleDrain, Forget
<1>13. Inv /\ UNCHANGED vars => Inv'
       BY SMT DEF vars, Inv, TypeOK, Exclusive, NoProtectedIdleDrain
<1> QED BY <1>1, <1>2, <1>3, <1>4, <1>5, <1>6, <1>7, <1>8, <1>9,
           <1>10, <1>11, <1>12, <1>13 DEF Next

THEOREM Safety == Spec => []Inv
<1>1. Init => Inv BY Initial
<1>2. Inv /\ [Next]_vars => Inv' BY Preserve
<1> QED BY <1>1, <1>2, PTL DEF Spec

\* Completion changes only the exact connection/generation/request token.
THEOREM ExactFinish ==
    \A o \in Operations, r \in Reservations :
      Finish(o,r) => \A k \in Keys \ {<<o,r>>} :
        /\ (k \in held' <=> k \in held)
        /\ (k \in protected' <=> k \in protected)
BY SMT DEF Finish, Keys

THEOREM UncertaintyRetains ==
    \A r \in Reservations : Fence(r) => UNCHANGED <<reserved, physical, held>>
BY SMT DEF Fence

\* A prefork reservation conservatively represents an abstract physical process.
Reservation == INSTANCE ReservationCore WITH
    Slots <- Slots, Trees <- Trees, Generations <- Generations,
    reserved <- reserved, physical <- reserved \ exited, terminating <- draining

THEOREM ReservationStep == Inv /\ [Next]_vars => [Reservation!Next]_Reservation!vars
BY SMT DEF Inv, TypeOK, Exclusive, NoProtectedIdleDrain, Next, vars,
    Reserve, Start, Publish, Hold, Abandon, Expire, Finish, IdleDrain, Fence,
    ObserveExit, RemoveRecord, Forget, Reservations,
    Reservation!Next, Reservation!Spawn, Reservation!MarkTerminating,
    Reservation!ChildExited, Reservation!ForgetExited, Reservation!vars,
    Reservation!Reservations

THEOREM RefinesReservations == Spec => Reservation!Spec
<1>1. Spec => []Inv BY Safety
<1>2. Init => Reservation!Init BY SMT DEF Init, Reservation!Init
<1>3. Inv /\ [Next]_vars => [Reservation!Next]_Reservation!vars BY ReservationStep
<1> QED BY <1>1, <1>2, <1>3, PTL DEF Spec, Reservation!Spec
Admission == INSTANCE AdmissionCore WITH
    Operations <- Operations, Generations <- Reservations,
    held <- protected, draining <- retired

THEOREM AdmissionStep == Inv /\ [Next]_vars => [Admission!Next]_Admission!vars
BY SMT DEF Inv, TypeOK, Exclusive, NoProtectedIdleDrain, Next, vars,
    Reserve, Start, Publish, Hold, Abandon, Expire, Finish, IdleDrain, Fence,
    ObserveExit, RemoveRecord, Forget, Reservations,
    Admission!Next, Admission!Acquire, Admission!Complete, Admission!Drain,
    Admission!ObserveExit, Admission!vars

THEOREM RefinesAdmission == Spec => Admission!Spec
<1>1. Spec => []Inv BY Safety
<1>2. Init => Admission!Init BY SMT DEF Init, Admission!Init
<1>3. Inv /\ [Next]_vars => [Admission!Next]_Admission!vars BY AdmissionStep
<1> QED BY <1>1, <1>2, <1>3, PTL DEF Spec, Admission!Spec
=============================================================================
