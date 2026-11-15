---------------------------- MODULE AdmissionCore ----------------------------
EXTENDS Naturals, TLAPS

\* Arbitrary operation/generation sets, not finite TLC constants.
CONSTANTS Operations, Generations
VARIABLES held, draining
vars == <<held, draining>>

Init == /\ held = {} /\ draining = {}
Acquire(o,g) == /\ o \in Operations /\ g \in Generations
                /\ g \notin draining
                /\ held' = held \cup {<<o,g>>}
                /\ UNCHANGED draining
Complete(o,g) == /\ held' = held \ {<<o,g>>}
                 /\ UNCHANGED draining
Drain(g) == /\ g \in Generations
            /\ ~\E o \in Operations : <<o,g>> \in held
            /\ draining' = draining \cup {g}
            /\ UNCHANGED held
ObserveExit(g) == /\ held' = {p \in held : p[2] # g}
                  /\ UNCHANGED draining
Next == \/ (\E o \in Operations, g \in Generations : Acquire(o,g) \/ Complete(o,g))
        \/ (\E g \in Generations : Drain(g) \/ ObserveExit(g))
TypeOK == /\ held \subseteq Operations \X Generations
          /\ draining \subseteq Generations
NoBusyDrain == \A o \in Operations, g \in Generations :
                 g \in draining => <<o,g>> \notin held
Inv == TypeOK /\ NoBusyDrain
Spec == Init /\ [][Next]_vars

THEOREM Initial == Init => Inv
BY SMT DEF Init, Inv, TypeOK, NoBusyDrain

THEOREM Preserve == Inv /\ [Next]_vars => Inv'
<1>1. ASSUME NEW o \in Operations, NEW g \in Generations,
             Inv, Acquire(o,g)
      PROVE Inv'
      BY SMT, Inv, Acquire(o,g) DEF Inv, TypeOK, NoBusyDrain, Acquire
<1>2. ASSUME NEW o \in Operations, NEW g \in Generations,
             Inv, Complete(o,g)
      PROVE Inv'
      BY SMT, Inv, Complete(o,g) DEF Inv, TypeOK, NoBusyDrain, Complete
<1>3. ASSUME NEW g \in Generations, Inv, Drain(g)
      PROVE Inv'
      BY SMT, Inv, Drain(g) DEF Inv, TypeOK, NoBusyDrain, Drain
<1>4. ASSUME NEW g \in Generations, Inv, ObserveExit(g)
      PROVE Inv'
      BY SMT, Inv, ObserveExit(g) DEF Inv, TypeOK, NoBusyDrain, ObserveExit
<1>5. Inv /\ UNCHANGED vars => Inv'
      BY SMT DEF vars, Inv, TypeOK, NoBusyDrain
<1> QED BY <1>1, <1>2, <1>3, <1>4, <1>5 DEF Next

THEOREM Safety == Spec => []Inv
<1>1. Init => Inv BY Initial
<1>2. Inv /\ [Next]_vars => Inv' BY Preserve
<1> QED BY <1>1, <1>2, PTL DEF Spec
=============================================================================
