# ADR-0023 — Native Jev in the live Telegram intake pipeline

Status: accepted by the owner request to connect Jev, 2026-10-10.

The J0–J5 components previously did not consume live Telegram intake. The owner
explicitly requests production wiring now. This is an operator-authorized rollout,
not a claim that the seed corpus satisfies the statistical gate in ADR-0022.
J6 is outside scope. Existing authority, confirmation and apply boundaries stay
in force.

`JevDecisionsInterpreter` consumes the existing `interpretation` and
`discussion_classify` queues through `vuzol-interpreter`. It uses only
`typesafe/jev-1.13` at the native Decisions endpoint. A single batched request
classifies effect, relation, target, task type and operation. Unknown labels,
missing answers, model substitution and an unresolved reference fail closed.
The J3 strict `decision.v3` reader validates the runtime-bound receipt. Prompt
and input hashes are recorded with `jev.intake_decided`; attempts, tokens and
failures use the existing usage ledger. Jev confidence does not grant authority.

`jev_enabled_kinds`, the cohort and per-kind kill switch gate calls. The adapter
checks admission again after the provider returns; a disabled in-flight answer
cannot enter the pipeline. Existing explicit discussion commands bypass Jev.
Task intake additionally checks its source Task version before applying a result;
discussion intake already checks the session version.

Jev is a classifier and cannot generate names or detailed plan items. An optional
`interpretation.structure_profile_id` chooses an existing generative planner only
after Jev routes to a plan. Its result must match the selected route; execution
and approval stay with deterministic policy and card controls. With no planner,
plan intake returns a visible configuration explanation. Manual project creation
is an explicit local setting, default off; this deployment enables it at the
owner's request. It asks for a manually created topic without invoking provisioning.

The tracked profile stores `file:jev-openrouter.key` only. The credential is a
local file with mode 0600 mounted read-only into the interpretation service under
its configured secret root. It is not placed in registry documents, image layers,
command arguments, provider response logs or coordination artifacts. Other live
services do not receive this credential mount.

The first implementation uses existing bounded Task/discussion context ports;
it does not claim full historical target retrieval. Missing targets abstain;
broader retrieval is a follow-up. Transport failures use existing bounded retries.
