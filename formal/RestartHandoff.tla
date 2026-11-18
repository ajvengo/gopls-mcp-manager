--------------------------- MODULE RestartHandoff ---------------------------
EXTENDS TLC

CONSTANTS Frontends, Mutation
VARIABLES phase, committed, exitProof, charged, started, httpOpen, settled,
          resumed, admissionOpen

vars == <<phase, committed, exitProof, charged, started, httpOpen, settled,
          resumed, admissionOpen>>

Init ==
    /\ phase = "prepared"
    /\ committed = FALSE
    /\ exitProof = FALSE
    /\ charged = TRUE
    /\ started = FALSE
    /\ httpOpen = FALSE
    /\ settled = {}
    /\ resumed = {}
    /\ admissionOpen = FALSE

Commit ==
    /\ phase = "prepared"
    /\ phase' = "waiting"
    /\ committed' = TRUE
    /\ charged' = (Mutation # "commit-refunds")
    /\ started' = (Mutation = "commit-starts")
    /\ httpOpen' = (Mutation = "commit-opens-http")
    /\ resumed' = IF Mutation = "commit-resumes" THEN Frontends ELSE resumed
    /\ UNCHANGED <<exitProof, settled, admissionOpen>>

ObserveEOF ==
    /\ phase = "waiting"
    /\ phase' = "recovering"
    /\ exitProof' = TRUE
    /\ charged' = FALSE
    /\ UNCHANGED <<committed, started, httpOpen, settled, resumed, admissionOpen>>

Fail ==
    /\ phase \in {"prepared", "waiting"}
    /\ phase' = "failed"
    /\ admissionOpen' = (Mutation = "failure-opens-admission")
    /\ UNCHANGED <<committed, exitProof, charged, started, httpOpen, settled, resumed>>

StartWorktree ==
    /\ phase = "recovering"
    /\ ~started
    /\ started' = TRUE
    /\ UNCHANGED <<phase, committed, exitProof, charged, httpOpen, settled, resumed,
                   admissionOpen>>

FinishRecovery ==
    /\ phase = "recovering"
    /\ phase' = "resuming"
    /\ httpOpen' = TRUE
    /\ UNCHANGED <<committed, exitProof, charged, started, settled, resumed, admissionOpen>>

SettleFrontend(f, ok) ==
    /\ phase = "resuming"
    /\ Mutation # "never-settles"
    /\ f \in Frontends \ settled
    /\ settled' = settled \cup {f}
    /\ resumed' = IF ok THEN resumed \cup {f} ELSE resumed
    /\ UNCHANGED <<phase, committed, exitProof, charged, started, httpOpen, admissionOpen>>

FinishHandoff ==
    /\ phase = "resuming"
    /\ settled = Frontends \/ Mutation = "early-admission"
    /\ phase' = "done"
    /\ admissionOpen' = TRUE
    /\ UNCHANGED <<committed, exitProof, charged, started, httpOpen, settled, resumed>>

Settle == \E f \in Frontends, ok \in BOOLEAN : SettleFrontend(f, ok)
Next == Commit \/ ObserveEOF \/ Fail \/ StartWorktree \/ FinishRecovery
        \/ Settle \/ FinishHandoff

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ phase \in {"prepared", "waiting", "recovering", "resuming", "done", "failed"}
    /\ {committed, exitProof, charged, started, httpOpen, admissionOpen} \subseteq BOOLEAN
    /\ settled \subseteq Frontends
    /\ resumed \subseteq Frontends

Safety ==
    /\ (~charged \/ started \/ httpOpen \/ resumed # {} \/ admissionOpen)
       => (committed /\ exitProof)
    /\ resumed \subseteq settled
    /\ admissionOpen => settled = Frontends
    /\ phase = "failed" => ~(started \/ httpOpen \/ resumed # {} \/ admissionOpen)

FairSpec == Spec
    /\ WF_vars(Commit \/ Fail)
    /\ WF_vars(ObserveEOF \/ Fail)
    /\ WF_vars(FinishRecovery)
    /\ WF_vars(Settle)
    /\ WF_vars(FinishHandoff)

Settles == <>(phase \in {"done", "failed"})
=============================================================================
