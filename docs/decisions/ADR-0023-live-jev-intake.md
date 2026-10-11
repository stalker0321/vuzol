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

## Compound turns — T089, 2026-10-11

The additive `mixed` effect classifies the whole turn when no single existing
route can preserve every requested outcome and restriction. Multiple steps
serving one coherent goal still use `execute_request` (the existing wire name
for execute), including "check, change only if needed, then report". Conditions,
negations and "do not start yet" must not be stripped to select a route.
For `mixed` only, `decision.v3` permits an explicit null relation; existing
effects retain their non-null relation contract. The native choice API represents
this as the supplied `null` choice, normalized to JSON null in the receipt.
There are still exactly five questions in one Jev call, no intent list or new
planner. The native prompt revision is `jev-intake-native-v2`.

Existing Task/discussion routes cannot execute a mixed turn in its entirety:
both paths now return visible clarification without applying any part. A mixed
receipt also fences legacy keyword policy and never invokes the optional
structurer or converts a continuation/control override into partial execution.
This is safe dispatch support, not a claim that mixed commands execute in TG.

Provider requests continue to carry the full raw `original_input` from the Task,
independently of the bounded goal/title. CLI/API policies explicitly preserve
conditions, negation and scope; these instructions are not a condition AST or a
new authority mechanism. Existing continuation refs now resolve the full current
carrier turn (with target/hash validation), not just the old Task text; the old
request stays in supporting context. Voice tasks forward their full transcript.
A non-execution effect cannot resume work merely through relation=continue.
The bounded discussion task-request payload has no separate raw-turn field:
requests exceeding its 4,000-character goal limit clarify rather than truncating
a trailing condition. Legacy Task intake still retains the full raw source
independently of its bounded goal, including restrictions after character 4,000.
A conditional edit may finish without changes when
the condition does not hold. Its optional `WorkerEditReport.no_change_reason`
must explain the condition and observed evidence, with claimed_complete=true
and no failure. The finalizer still measures Git, runs all required gates,
revokes access and verifies the manifest, retaining result_commit=base_commit
without manufacturing a commit. Reports contradicting measured edits, failed
gates and unexplained empty diffs remain failures. The reason is retained in the
result manifest for inspection; it is a provider claim, not independent proof
that an arbitrary natural-language condition was false. Review/approval remain
unchanged. Applying an approved no-op validates the clean target/base and does
not advance or reset Git refs (including root-commit bases).

These changes are implemented/tested on an isolated branch, not a rollout.
