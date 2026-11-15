--------------------------- MODULE ReservationCore ---------------------------
EXTENDS TLAPS
CONSTANTS Slots, Trees, Generations
Reservations == Slots \X Trees \X Generations
VARIABLES reserved, physical, terminating
vars == <<reserved, physical, terminating>>

Init == /\ reserved = {} /\ physical = {} /\ terminating = {}
Spawn(s,t,g) ==
    LET r == <<s,t,g>> IN
    /\ r \in Reservations
    /\ ~\E p \in reserved : p[1] = s \/ p[2] = t
    /\ reserved' = reserved \cup {r}
    /\ physical' = physical \cup {r}
    /\ UNCHANGED terminating
MarkTerminating(r) ==
    /\ r \in reserved
    /\ terminating' = terminating \cup {r}
    /\ UNCHANGED <<reserved, physical>>
ChildExited(r) ==
    /\ physical' = physical \ {r}
    /\ UNCHANGED <<reserved, terminating>>
ForgetExited(r) ==
    /\ r \notin physical
    /\ reserved' = reserved \ {r}
    /\ terminating' = terminating \ {r}
    /\ UNCHANGED physical
Next == \/ (\E s \in Slots, t \in Trees, g \in Generations : Spawn(s,t,g))
        \/ (\E r \in Reservations :
              MarkTerminating(r) \/ ChildExited(r) \/ ForgetExited(r))

TypeOK == /\ reserved \subseteq Reservations
          /\ physical \subseteq reserved
          /\ terminating \subseteq reserved
Exclusive == \A p,q \in reserved :
               (p[1] = q[1] \/ p[2] = q[2]) => p = q
Inv == TypeOK /\ Exclusive
Spec == Init /\ [][Next]_vars

THEOREM Initial == Init => Inv
BY SMT DEF Init, Inv, TypeOK, Exclusive, Reservations

THEOREM Preserve == Inv /\ [Next]_vars => Inv'
<1>1. ASSUME NEW s \in Slots, NEW t \in Trees, NEW g \in Generations,
             Inv, Spawn(s,t,g)
      PROVE Inv'
      BY SMT, Inv, Spawn(s,t,g)
         DEF Inv, TypeOK, Exclusive, Reservations, Spawn
<1>2. ASSUME NEW r \in Reservations, Inv, MarkTerminating(r)
      PROVE Inv'
      BY SMT, Inv, MarkTerminating(r)
         DEF Inv, TypeOK, Exclusive, Reservations, MarkTerminating
<1>3. ASSUME NEW r \in Reservations, Inv, ChildExited(r)
      PROVE Inv'
      BY SMT, Inv, ChildExited(r)
         DEF Inv, TypeOK, Exclusive, Reservations, ChildExited
<1>4. ASSUME NEW r \in Reservations, Inv, ForgetExited(r)
      PROVE Inv'
      BY SMT, Inv, ForgetExited(r)
         DEF Inv, TypeOK, Exclusive, Reservations, ForgetExited
<1>5. Inv /\ UNCHANGED vars => Inv'
      BY SMT DEF Inv, TypeOK, Exclusive, vars
<1> QED BY <1>1, <1>2, <1>3, <1>4, <1>5 DEF Next

THEOREM Safety == Spec => []Inv
<1>1. Init => Inv BY Initial
<1>2. Inv /\ [Next]_vars => Inv' BY Preserve
<1> QED BY <1>1, <1>2, PTL DEF Spec
=============================================================================
