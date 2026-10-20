# Project lead: stage 2 review handoff

Work date: 2026-10-20. This is an implementation/test handoff, not approval for
production or stages 3–5.

## Starting point and isolation

- Actual branch: `agent/project-lead-v1`.
- Actual initial HEAD: `d5f5e155efdbc5fbf62780fa04b3cc49c2025146`, clean worktree.
- Original review HEAD: `84716b9`; pre-overlay baseline: `55f6615`.
- Read the implementation plan, `GROK_START.md`, the external review and all
  five external regression probes before changing the implementation.
- `d5f5e15` already addressed parts of R1–R7 and moved probes into the suite.
  Its 45 leadership tests passed. Its word-matching acceptance checker was
  still capable of accepting an OAuth claim in a README.
- A detached checkout of that exact HEAD and a second disposable database,
  `vuzol_stage2_baseline`, reproduced five remaining defects with failing
  assertions: TODO-as-OAuth acceptance, resume leaving a package paused,
  accepting an expired inference, model error undoing owner pause, and accepting
  a wait for a nonexistent package. These are additional residual-defect probes,
  not a claim that all five original review probes still failed at this HEAD.
- A fresh local PostgreSQL container was created from the already installed
  `postgres:16.13-bookworm` image: `vuzol-stage2-jzwfpd`, host port `32769`,
  database `vuzol_stage2_test`, user `vuzol_stage2`. Its `public` schema was
  verified empty before migrating. No existing container/database was reused.
- Dependencies were installed from `uv.lock` into
  `/tmp/vuzol-stage2.jzwfpD/venv`. No production venv or host packages changed.
- No live model calls, Telegram API calls, external deployment, push or
  production service changes were performed.
- After verification, the owned PostgreSQL container and its anonymous volume
  (both disposable databases) were removed, as was the clean detached baseline
  worktree. No test DB backup was retained; tests recreate those fixtures.
  The isolated venv and residual-defect probe file remain under
  `/tmp/vuzol-stage2.jzwfpD`; production environments were not changed.

## Contracts changed

### R2: independent acceptance

The applier captures bounded regular files from the actual Git commit using the
existing `LocalGit.source_archive`; dirty/untracked files and symlink targets
cannot enter that snapshot. Capture writes pending evidence with `satisfied=false`.
It cannot accept a criterion.

A separately selected reviewer profile receives the original request, complete
criteria, scope revision and captured commit. It returns a typed per-criterion
verdict with explanations and references to captured files. Unknown or missing
evidence, fabricated paths, incomplete criterion sets and mismatched commits fail
closed. The reviewer has no mutation commands. Its response is checked against
the active turn lease, goal fence, scope and current completed package head.

Acceptance reads only `goal_review_v2` evidence for the current package/head.
Old `goal_head_v1` keyword verdicts are never accepted. Starting another package,
changing the description/scope or advancing the result invalidates prior coverage.
Acceptance also checks the live integration ref in the source repository recorded
by the applier from server configuration. Isolated task repositories are not the
source of that ref; a moved/deleted ref or missing repository fails closed.

### R3: stop and execution admission

Pause/cancel/revoke propagate to child work. Resume restores the paused package
and tasks; it cannot restore a revoked or expired grant. Unknown effects still
block resumption. The normal task controls remain responsible for active runs.

Pending workflow dispatch checks the managed grant before creating a run.
The applier rechecks the grant before recording an effect intent, using the same
goal-before-child lock order as owner controls. Feature off blocks these new
managed admissions. A workflow outcome arriving after cancellation/takeover
cannot replace the authoritative cancelled state or kill the worker loop.

An already dispatched effect still requires the existing reconciliation path;
these changes do not promise to undo effects.

### R4/R5: turn fencing and command replay

Every turn carries a server-issued claim containing the goal, outbox item,
worker, lease epoch and outbox generation. Both leases are renewed during
inference. Saving a decision and applying each command check the claim;
the command check is inside the mutation transaction.

Commands keep their original ordinal, key, payload and expected version.
Replay consults receipts and continues the unapplied suffix. It does not rewrite
preconditions to match current state. Owner changes invalidate old decisions.
Tests include replacement workers still inside inference, including identical
worker names and late errors, and crashes after the first command commit and before
ack. A late ack cannot release a successor's lease with the same worker name.
Human/agent command replay is serialized even before a goal exists. Managed and
legacy package admission share a project lock; enabling cannot take over live
legacy work.

### R6: reachable waits

Resume has its own generation-bound wakeup. Completed/blocked/failed/stopped
package outcomes are reconciled. Package waits must target the goal's current
package; a ready package is immediately visible. Repeated waiting on the same
unchanged ready result asks the owner instead of generating an inference loop.

Model/configuration/budget failures park with a durable owner question.
Answers wake a new turn. `request_revision` remains explicitly unsupported;
normal review/repair inside a package is unchanged.

### R1: runtime provider and accounting

The CLI composes the lead through its actual adapter registry. Supported lead
and goal-review profiles are API `openai-compatible` profiles with the appropriate
roles. Goal review requires a separately selected profile.

Both roles use existing step-less budget reservations and the common usage
ledger, including daily limits, profile concurrency and unknown usage on errors.
Calls are bounded; response schemas validate command-specific payloads.
Turn invocation IDs are stable across recovery. A prior invocation without a
durable decision requires reconciliation/owner action instead of blind resending.

The entrypoint test runs actual CLI composition, the real registry and HTTP
adapter with `httpx.MockTransport`, and checks the resulting reservation/usage.

### R7: Telegram

Durable notices include the goal, lifecycle, blocker, package, next action and
result. Completion is delivered without requiring a later `/lead_status`.
Status also finds the last terminal goal.

An owner question includes a copyable command:

```text
/lead_answer <goal-uuid> <question-version> <answer>
```

The explicit goal and question version prevent a delayed answer from approving
another question. An answer records information; it never grants a waiver,
host capability, deployment permission or broader scope.

`/lead_status`, `/lead_pause`, `/lead_cancel`, `/lead_revoke` and answers remain
available for existing goals with the feature disabled.

## Tests and commands

Only use a disposable database. The integration fixture terminates other
connections to that database and truncates its public tables.

Commands for the test environment used here:

```sh
cd /home/vodkolyan/vuzol-afterword-stabilization
export UV_PROJECT_ENVIRONMENT=/tmp/vuzol-stage2.jzwfpD/venv
uv sync --frozen --group dev
export VUZOL_TEST_DATABASE_DSN='postgresql://vuzol_stage2:stage2-disposable-only@127.0.0.1:32769/vuzol_stage2_test'
export PYTHONPATH=src:.
export PYTHONDONTWRITEBYTECODE=1
VUZOL_DATABASE_DSN_REFERENCE=env:VUZOL_STAGE2_DSN \
  VUZOL_STAGE2_DSN='postgresql+psycopg://vuzol_stage2:stage2-disposable-only@127.0.0.1:32769/vuzol_stage2_test' \
  /tmp/vuzol-stage2.jzwfpD/venv/bin/alembic upgrade head
uv run --frozen pytest tests/unit --no-cov -p no:cacheprovider -q --tb=short
uv run --frozen pytest tests/integration -m 'not docker' --no-cov -p no:cacheprovider -q --tb=short
uv run --frozen pytest tests/unit/leadership tests/integration/leadership --no-cov -p no:cacheprovider -q --tb=short
uv run --frozen pytest tests/integration/leadership/test_managed_goal_flow.py::test_managed_goal_reviews_repairs_and_resumes_after_restart --no-cov -p no:cacheprovider -q -s
uv run --frozen mypy src/vuzol/leadership --follow-imports=silent --show-error-codes
uv run --frozen ruff check src/vuzol/leadership src/vuzol/cli/applier.py \
  src/vuzol/config/settings.py src/vuzol/discussion/application.py \
  src/vuzol/execution/result_apply.py src/vuzol/telegram/layout.py \
  src/vuzol/workflows/dispatch.py src/vuzol/workflows/worker.py \
  tests/unit/leadership tests/integration/leadership tests/integration/test_d2_acceptance_pg.py
git diff --check
```

The DSNs above identify only this run's disposable database, not a permanent
test service. To reproduce after cleanup, create a fresh explicitly named container
from an already available PostgreSQL image, verify its published port/database,
and update both DSNs. For example, with no image pull:

```sh
docker run -d --pull=never --name vuzol-stage2-review \
  -e POSTGRES_USER=vuzol_stage2 -e POSTGRES_PASSWORD=stage2-disposable-only \
  -e POSTGRES_DB=vuzol_stage2_test -p 127.0.0.1::5432 postgres:16.13-bookworm
docker port vuzol-stage2-review 5432/tcp
docker exec vuzol-stage2-review pg_isready -U vuzol_stage2 -d vuzol_stage2_test
docker exec vuzol-stage2-review psql -U vuzol_stage2 -d vuzol_stage2_test \
  -c "SELECT current_database(); SELECT tablename FROM pg_tables WHERE schemaname='public';"
```

Do not run integration commands until the identity is correct and the new public
schema is empty, then apply migrations. Never run two pytest integration processes
against the same database.

At initial HEAD: leadership **45 passed**; unit **2873 passed, 1 failed,
1 skipped**. The failure was a pre-existing Git timeout/child-exit race.
Commit `aea0c55` fixes that race; its Git guardrail tests: **36 passed**.

Additional baseline probes were executed from the detached `d5f5e15` checkout,
not from the modified source (result: **5 failed on assertions**):

```sh
cd /tmp/vuzol-stage2.jzwfpD/baseline
VUZOL_TEST_DATABASE_DSN='postgresql://vuzol_stage2:stage2-disposable-only@127.0.0.1:32769/vuzol_stage2_baseline' \
  PYTHONPATH=src:. PYTHONDONTWRITEBYTECODE=1 \
  /tmp/vuzol-stage2.jzwfpD/venv/bin/pytest -c pyproject.toml \
  /tmp/vuzol-stage2.jzwfpD/baseline_probes.py \
  --no-cov -p no:cacheprovider -q --tb=short
```

This is the historical command: the detached checkout and database were removed
after use. Equivalent regressions are now in the normal leadership suite.

After the changes: unit **2872 passed, 1 skipped, 3 warnings in 35.63s**;
leadership mypy **20 source files, no errors**. The skipped unit test explicitly excludes remote execution
(`tests/unit/test_remote_out_of_scope.py`). The keyword-checker tests were replaced
by committed-snapshot and independent-evidence tests, not weakened to accept claims.

The first broad integration run returned **807 passed, 4 failed, 1 skipped,
15 deselected**. Investigation:

- The concurrent legacy button test also failed at initial HEAD. The ownership
  fence pre-read cached an unlocked stale package in the ORM. Admission now takes
  the project lock and reads the package under its normal row lock.
- Both D2 apply failures reproduced at initial HEAD. Their Git mocks omitted the
  existing async `require_apply_access` method; commit `bcba097` supplies AsyncMock.
  Assertions and production apply checks were not relaxed.
- The secret scanner subprocess could not find `detect-secrets-hook` when pytest
  was invoked by absolute path without the venv on PATH. `uv run --frozen` supplies
  the correct test environment. No scanner bypass or allowlisting was added.

The combined follow-up command below returned **93 passed in 80.76s** (before the
final three lease/ack cases were added):

```sh
uv run --frozen pytest tests/unit/leadership tests/integration/leadership \
  tests/integration/storage/test_discussion_application.py \
  tests/integration/test_d2_acceptance_pg.py tests/integration/test_secret_scan.py \
  --no-cov -p no:cacheprovider -q --tb=short
```

Final non-Docker integration on runtime commit `a2cfeca`:
**816 passed, 1 skipped, 15 deselected,
13 warnings in 524.66s**. The skipped test is
`test_completed_plan_enqueues_system_trace`: the existing per-task planner is
temporarily disabled. Warnings include Telegram deprecations and one SQLAlchemy
unchecked-in-connection cleanup warning reported at
`test_terminal_projections_sequence_work_package_observation_once`. That warning's
cause was not resolved in this work; it is not counted as a test failure or hidden.

Focused leadership on `a2cfeca`: **66 passed in 65.09s**.
Afterward, test-only commit `3fbc166` strengthened the restart boundary from a
fresh worker object to a separate Python process, with a fresh composition and
database engine. Its standalone trace run passed: **1 passed in 13.20s**.
Final focused result on that test-only commit: **66 passed in 65.28s**.
Ruff on the listed source/test paths and `git diff --check`: passed.
Docker-marked integration tests, live providers, live Telegram, deployment and
the global coverage floor are not included in these commands and are not claimed
as passing.

## Test enable/disable

The integration tests above assemble enabled and disabled runtime configurations
and use fake transports; they are the reproducible, no-charge pilot.

For a separate manually configured test installation only, keep its own DSN,
repository/worktree/artifact roots and registry. Select an enabled API profile
with role `lead`, and a different enabled API profile with role `reviewer`:

```sh
export VUZOL_PROJECT_LEAD__ENABLED=true
export VUZOL_PROJECT_LEAD__PROFILE_ID='<lead-profile-id>'
export VUZOL_PROJECT_LEAD__REVIEWER_PROFILE_ID='<review-profile-id>'
```

Do not run this against a real provider or Telegram account without separate
authorization. The test transport is injected by the test suite; those settings
alone do not make a real provider free or fake.

Within an existing test project topic: `/lead <goal and observable criteria>`,
then `/lead_status`, `/lead_pause`, `/lead_resume`, `/lead_cancel` or
`/lead_revoke`. Pause the test goal before shutdown if work is active. To turn off
new managed activity, set
`VUZOL_PROJECT_LEAD__ENABLED=false` in all isolated test processes and restart
those processes. Owner stop/status controls remain accessible. Disabling is
not a schema downgrade or automatic transfer to legacy ownership.

## Limits for independent review

- This is one goal in an existing project, with sequential packages. No inbox,
  new project provisioning, parallel execution or stage 3–5 functionality added.
- The goal snapshot is limited to 120,000 bytes of regular committed files.
  Non-text files, unsafe paths, oversize snapshots or insufficient model context
  cannot produce a positive verdict. The reviewer is read-only; it does not run
  new tests. Goals requiring unprovided runtime/device/deployment observations
  must remain unproven.
- Independence means a separate server-selected reviewer invocation/profile and
  evidence boundary. A model verdict is not a mathematical guarantee of correctness.
- No new schema migration is required beyond `f3a8c2d14e70`. Old lexical verdicts
  are ignored; this handoff does not repair existing production data.
  The existing additive lead chain is `b1a7e0d91c20` (commands/delegation) →
  `c2b8f1e02d31` (execution links) → `f3a8c2d14e70` (criterion evidence).
- Unknown external invocation outcomes remain unknown. Reconciliation/owner
  recovery is required; the agent cannot accept cost uncertainty for the owner.
- Revoked grants cannot be resumed. Regrant/ownership transfer is not introduced
  in this stage; cancel the stopped goal before creating a new authorized goal.

## Trace

The positive scenario uses `/lead ship the marker` → package A → review requests
a repair → existing repair workflow → approved apply into the managed branch →
new Python process → package B → approved apply → independent evidence on B's
commit → goal completed → Telegram result. The default branch remains at its
original commit. The negative scenario keeps identical marker work but requests
OAuth, including a README repeating OAuth claims: it must remain incomplete.

Concrete trace from the standalone run above:

```text
goal: 9c2f03d6-7776-4c2f-ad6c-c4b1268a18c9
branch: vuzol-managed/vuzol/1fedd334ab28
A: 041d23cb-78f3-40a4-97ac-f5bade731e23
  review_changes_requested -> repair -> approved apply
  head: cba8a0f4991d9b95ddf62f9c599b68f3c70569eb
restart: separate Python process; fresh composition and DB engine
B: 2abd97a5-d55a-49cf-92c7-1cd2ccc572c7
  approved apply
  head: e7c1b142e8fc5852cb7fc455de042152052dd2c4
goal_review_v2 -> propose_completion -> completed -> Telegram result
main before/after: 69849f0fda0f04b8f3d4962ac67c2ee7c337ee94
```

Assertions check exactly two completed packages, the repair step, approval,
the actual committed marker contents, unchanged `main`, and delivered completion
content. The restarted worker does not create a third package on another poll.

## Commits

- `aea0c55` — preserve Git timeout errors when the child has already exited.
- `bcba097` — configure async Git access checks in the two baseline D2 fixtures.
- `a2cfeca` — R1–R7 runtime contracts, independent evidence and regression coverage.
- `3fbc166` — exercise the A/B restart boundary in a separate Python process.
- This report is a separate documentation commit on the same feature branch.

The feature remains off by default. No push/deploy was performed. Implementation
stops at the stage-2 review boundary; this report does not approve rollout.

## Required regression coverage

| Boundary | Tests |
| --- | --- |
| A → review/repair → restart → B → proven completion | `test_managed_goal_reviews_repairs_and_resumes_after_restart` |
| No OAuth by marker files/README claims | `test_goal_description_requires_actual_evidence`, `test_copying_oauth_words_into_committed_readme_is_not_evidence` |
| Before inference / after response / after one command / before ack | `test_crash_before_model_then_applies_once`, `test_crash_after_response_applies_on_resume`, `test_crash_after_first_command_resumes_from_receipts`, `test_ack_retry_does_not_repeat_mutations` |
| Owner controls during inference; stale response after resume | `test_pause_during_inference_discards_the_response`, `test_revoke_during_inference_discards_the_response`, `test_saved_response_invalidated_by_pause_resume`, `test_model_error_after_pause_does_not_unpause_the_goal` |
| Lease expiry, renewal, takeover, same-owner ack | `test_expired_response_is_discarded_and_reclaimed`, `test_slow_inference_renews_both_leases`, `test_old_response_cannot_overwrite_a_replacement_still_in_inference`, `test_old_ack_does_not_release_a_successor_lease_with_the_same_worker_name` |
| Revoke/cancel after actual approval, before effect | `test_owner_control_after_real_approval_prevents_the_git_effect` |
| Blocked/failed wakeup and waiting exits | `test_blocked_package_wakes_the_lead`, `test_failed_package_wakes_the_lead`, `test_resume_wakes_parked_goal`, `test_owner_question_and_answer_bind_the_goal_revision` |
| Telegram content and feature-off legacy | `test_feature_off_still_pauses_and_shows_the_goal_card`, `test_owner_question_and_answer_bind_the_goal_revision`, positive flow final result assertion, `test_flag_off_leaves_lead_text_on_the_legacy_path` |
| Actual CLI/provider boundary | `test_cli_composes_actual_http_provider_and_settles_its_reservation`, budget refusal and unknown-usage tests in the same file |

## General-boundary follow-up, 2026-10-20

This follow-up starts from clean `9de02867402f8d95266ea1cd56fbe85e1f3ebd1e`
on `agent/project-lead-v1`. It addresses the two later findings about production
authority and poison Telegram updates; it is not approval for rollout.

### Changes and commits

- `c10b78c`: commit raw Telegram admission separately from bounded business
  processing, using the existing inbox/outbox and fenced leases. Offset
  acknowledgement cannot precede the raw transaction. Authorized raw JSON stays
  escaped so PostgreSQL-incompatible text cannot poison admission itself.
  Unauthorized user/chat identities never persist a body or replayable job.
  Processing gets three attempts, a cooperative 30-second timeout and a
  60-second lease; exhausted jobs have durable failure receipts and can use the
  existing operator replay CLI. That CLI's nested-transaction bug is fixed.
- `67d5611`: an integration grant cannot enter the production effect boundary.
  Managed packages skip automatic production and continue on integration/preview.
  Ordinary publication requires a separate immutable, artifact/commit/target-
  bound approval from the task owner, checked again immediately before activation.
  Telegram renders the exact production URL, commit and full artifact hash.
  Exact-step replay is permitted after consumption; another artifact is not.
- `a3d9408`: keep the existing allowlisted-operator contract for non-production
  approvals. The initial broad run found that applying the new owner-only rule
  to legacy REDO was an unintended change. The rule is now production-specific;
  the old REDO assertion and the new wrong-owner production assertion both pass.
- `3e45703`: recheck the lease clock/cancellation after the awaited approval
  lookup, immediately before activation; a lock wait cannot outlive authority.

See `TELEGRAM.md` for quarantine/replay and data handling, and
`PUBLICATION_TOPOLOGY.md` for the separate publication authority contract.

### Isolation and exact verification commands

Only the owned container `vuzol-boundary-fix-20261020` was created, using the
already present `postgres:16.13-bookworm` image (`--pull=never`), host port
`127.0.0.1:32772`, user `boundary_test`. Its disposable databases were
`vuzol_boundary_test` and `vuzol_boundary_delta`. Name/user were checked with
`SELECT current_database(), current_user`; both were migrated to `f3a8c2d14e70`.
Concurrent test processes used different databases, never the same TRUNCATE target.
The existing disposable venv was reused; no production venv, host install,
live provider, real Telegram mutation, push, deployment or running service changed.
After all runs completed, that container and its anonymous volume (both test
databases) were removed. No database backup was retained; committed fixtures
recreate the test data. The disposable venv remains available for independent review.

```sh
cd /home/vodkolyan/vuzol-afterword-stabilization
export UV_PROJECT_ENVIRONMENT=/tmp/vuzol-stage2.jzwfpD/venv
export PYTHONPATH=src:.
export PYTHONDONTWRITEBYTECODE=1
export VUZOL_TEST_DATABASE_DSN='postgresql://boundary_test:boundary-local-only@127.0.0.1:32772/vuzol_boundary_test'
uv run --frozen pytest --no-cov -q -rs tests/unit
uv run --frozen pytest --no-cov -q -rs tests/integration -m 'not docker'
uv run --frozen mypy src/vuzol
git diff --name-only 9de0286..3e45703 -- '*.py' | xargs uv run --frozen ruff check
git diff --name-only 9de0286..3e45703 -- '*.py' | xargs uv run --frozen ruff format --check
git diff --check

export VUZOL_TEST_DATABASE_DSN='postgresql://boundary_test:boundary-local-only@127.0.0.1:32772/vuzol_boundary_test'
uv run --frozen pytest --no-cov -q -rs \
  tests/integration/test_d1_identity_pg.py \
  tests/integration/telegram/test_durable_ingress.py \
  tests/integration/telegram/test_polling_commit.py \
  tests/integration/workflows/test_production_consent.py \
  tests/integration/workflows/test_publication_boundary.py \
  tests/integration/ops/test_outbox_replay.py
export VUZOL_TEST_DATABASE_DSN='postgresql://boundary_test:boundary-local-only@127.0.0.1:32772/vuzol_boundary_delta'
uv run --frozen pytest --no-cov -q -s \
  tests/integration/leadership/test_managed_goal_flow.py::test_managed_goal_reviews_repairs_and_resumes_after_restart
```

These are historical disposable DSNs, not a permanent test service. Recreate and
verify a fresh isolated database before reusing the commands; never substitute a
production/shared/default DSN. Migration used `VUZOL_DATABASE_DSN_REFERENCE=env:VUZOL_BOUNDARY_DSN`
with `VUZOL_BOUNDARY_DSN` pointing to the corresponding `postgresql+psycopg://`
URL, then `uv run --frozen alembic upgrade head`.

Observed results:

- Final unit: **2893 passed, 1 skipped, 3 warnings in 53.98s**.
- Final non-Docker integration on `3e45703`: **867 passed, 1 skipped,
  15 deselected, 13 warnings in 689.46s**.
- Final focused boundary/legacy integration: **44 passed in 39.81s**.
- Final standalone A/B restart trace: **1 passed in 18.75s**.
- Mypy: **290 source files**, no errors. Scoped Ruff/format: **16 files**, clean.
- Initial broad integration: **864 passed, 1 failed, 1 skipped, 15 deselected**;
  the failure was the legacy REDO regression described above, not a passed check.

The unit skip is the explicitly out-of-scope remote execution test. The
integration planner trace skip remains the existing disabled per-task planner.
Docker-marked tests, live model/Telegram calls, deployments and the global
coverage floor (`--no-cov`) are not claimed as passing. The lint result is scoped
to changed Python files, not a claim that unrelated baseline lint is clean.
Broad runs also emitted the known Telegram deprecation notices and the existing
SQLAlchemy unchecked-in-connection cleanup warning at
`test_terminal_projections_sequence_work_package_observation_once`; this work
does not claim to fix that unrelated warning.

### Traces and limits

The actual polling test admits updates 101/102 durably and advances offset to
103 while 101's business handler repeatedly fails. Update 102 is delivered;
101 is quarantined after three attempts. The separate worker test asserts
attempt order `[101, 102, 101, 101]`, then exercises real CLI dry-run/apply replay.
Restart after admission and crash after business commit before ack both retain
exactly one task; a late worker cannot acknowledge a successor's lease.

The real publisher assembly uses a fake HTTP probe and temporary site root:
first call → pending production card/no `current` → owner decision → fresh lease
→ activation/consumed approval → replay with `changed=false`.
Changed artifact/target, expiry, revoked approval, wrong owner, pause and takeover
fail closed. Revocation/pause/takeover are also injected after the preliminary
approval check to prove the final effect fence is enforced. A separate clock
injection expires the lease during approval lookup and still prevents activation.

Fresh goal trace on the final runtime:

```text
goal=034cc245-798e-4058-957b-7b2a29dd16f6
branch=vuzol-managed/vuzol/fc01a6bdb777
package_a=6e7aef4b-ef4e-4f3a-a13e-0673ef88da03
head_a=a340c7386ad3ddd3c9ae5869bf6a6c54849d30a7
restart=separate Python process; fresh composition and DB engine
package_b=fd99b522-5889-4fb7-a851-3cc0cd7c92a4
head_b=6dbf378e407821007034bcc8f2d2970bdf11b298
lifecycle=completed
main_before_and_after=cecf507eb43b0c88d4ffab9107bcc3a7aa5f83f6
```

There is no new migration or safety-bypass flag. The no-charge test pilot is the
fake-transport suite above; manual project-lead enable/disable remains as documented
earlier in this file, default off. Disabling the lead never grants production.
Managed-goal promotion remains unsupported rather than implicitly authorized.
Expired/changed publication envelopes need a new reviewed workflow. Telegram
timeouts are cooperative; retries do not promise strict per-chat ordering across
failures. Raw authorized bodies remain private outbox data; this change adds no
automatic body-retention/erasure policy. Independent stage-2 review is still required.
