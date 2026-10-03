# ADR-0020 — INTAKE/TARGET_RESOLUTION shadow harness and versioned prompts

Status: accepted (J3, base fd81e39). Implements `IMPLEMENTATION_PLAN.md` §J3.
Builds on ADR-0018 (DecisionBinding/chain) and ADR-0019 (context assembler).

## 1. Versioned prompt loader

`interpretation/prompt_loader.py` registers prompt templates by
``kind:version`` with a status and a content hash. Draft prompts stay draft:
``require_active`` refuses to run one on a production path, while the shadow
harness composes ``base + rubric`` explicitly. The composed prompt hash is
recorded on every advisory event so a decision can be traced to the exact
prompt that produced it. No live I/O; the loader is deterministic.

## 2. New decision schema, old readers untouched

INTAKE is ``decision.v3`` / kind ``intake``. ``decision.v1``
(``repair_triage``) and ``decision.v2`` (``target_selection``) keep their
readers and parsers; J3 adds a new namespace rather than extending them.

## 3. One INTAKE, at most one TARGET_RESOLUTION

`experiments/intake_shadow.py` runs exactly one INTAKE classification for a
semantically unresolved turn. TARGET_RESOLUTION runs at most once, and only
when the effect is execute/control, no target was selected, and retrieval
supplied new target facts (candidates present or partial coverage). The result
never changes the effect silently and never creates work.

## 4. Explicit commands skip the classifier

An explicit task command is detected before any provider call; the harness
returns with zero provider calls and records an ``explicit_command`` advisory
event. The deterministic ``/task`` fast path is not routed through a model.

## 5. Zero production transitions

Every run writes only an ``Event`` (``jev.shadow_recorded``,
``advisory=true``, ``production_transition=false``). The harness never touches
Task, Run, WorkPackage or Approval state; the integration test asserts those
tables stay empty after a run.

## 6. Visible outcomes and preserved provenance

Source change, detected prompt injection, out-of-domain snapshots and schema
errors produce explicit reason codes and an advisory event instead of a silent
no-op. On a provider/schema failure the fallback abstain decision keeps the
prompt hash and input fingerprint, so provenance survives fallback.

## 7. Full call accounting

A run reports ``provider_calls`` covering INTAKE plus the optional
TARGET_RESOLUTION, not just the Jev price. Offline/shadow spending goes through
the existing step-less budget owner (`execute_decision_step`), so real cost is
recorded in the shared ledger with purpose ``intake``.

## Consequences

- J4/J5 can compare routes by full call count and ledger cost.
- The harness is advisory-only; wiring any effect into production requires a
  later package and an explicit decision, not this shadow path.
