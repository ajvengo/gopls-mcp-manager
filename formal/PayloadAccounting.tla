------------------------- MODULE PayloadAccounting --------------------------
EXTENDS FiniteSets, Naturals

CONSTANTS Calls, Capacity, Mutation
VARIABLES ingress, queued, output, writing, charged

vars == <<ingress, queued, output, writing, charged>>
In(c) == <<"ingress", c>>
Out(c) == <<"reply", c>>
Charges == {"ingress", "reply"} \X Calls

Init ==
    /\ ingress = {}
    /\ queued = {}
    /\ output = {}
    /\ writing = {}
    /\ charged = {}

Admit(c) ==
    /\ c \notin ingress \cup output \cup writing
    /\ Cardinality(charged) < Capacity \/ Mutation = "overbook"
    /\ ingress' = ingress \cup {c}
    /\ queued' = queued \cup {c}
    /\ charged' = charged \cup {In(c)}
    /\ UNCHANGED <<output, writing>>

Cancel(c) ==
    /\ c \in queued
    /\ queued' = queued \ {c}
    /\ LET room == Cardinality(charged) < Capacity
       IN /\ output' = IF room THEN output \cup {c} ELSE output
          /\ charged' = (IF room THEN charged \cup {Out(c)} ELSE charged)
                        \ (IF Mutation = "refund-cancel" THEN {In(c)} ELSE {})
    /\ UNCHANGED <<ingress, writing>>

Discard(c) ==
    /\ c \in ingress \ queued
    /\ ingress' = ingress \ {c}
    /\ charged' = charged \ {In(c)}
    /\ UNCHANGED <<queued, output, writing>>

StartWrite(c) ==
    /\ c \in output
    /\ output' = output \ {c}
    /\ writing' = writing \cup {c}
    /\ charged' = IF Mutation = "refund-dequeue" THEN charged \ {Out(c)} ELSE charged
    /\ UNCHANGED <<ingress, queued>>

FinishWrite(c) ==
    /\ c \in writing
    /\ writing' = writing \ {c}
    /\ charged' = charged \ {Out(c)}
    /\ UNCHANGED <<ingress, queued, output>>

DiscardOutput ==
    /\ LET discarded == IF Mutation = "refund-writer" THEN output \cup writing ELSE output
       IN charged' = charged \ {Out(c) : c \in discarded}
    /\ output' = {}
    /\ UNCHANGED <<ingress, queued, writing>>

Next ==
    \/ \E c \in Calls : Admit(c) \/ Cancel(c) \/ Discard(c) \/ StartWrite(c) \/ FinishWrite(c)
    \/ DiscardOutput

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ ingress \cup queued \cup output \cup writing \subseteq Calls
    /\ charged \subseteq Charges

Safety ==
    /\ queued \subseteq ingress
    /\ output \cap writing = {}
    /\ charged = {In(c) : c \in ingress} \cup {Out(c) : c \in output \cup writing}
    /\ Cardinality(charged) <= Capacity
=============================================================================
