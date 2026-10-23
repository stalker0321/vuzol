# Architecture Invariants

These rules are non-negotiable unless explicitly changed through an ADR.

## State and workflow

1. PostgreSQL is the canonical source of task, run, step, approval, and usage state.
2. Telegram messages and topics are projections of state and must be reconstructable.
3. Every meaningful external side effect is represented as a persisted step.
4. A workflow must not rely on an in-memory process surviving.
5. Retrying a step is allowed only when it is known to be safe, idempotent, or isolated.
6. Unknown side effects must produce a blocked or validation-required state, not a blind retry.
7. Cancellation does not imply rollback.
8. External input is deduplicated through a persisted inbox before it changes business state.
9. Durable external delivery is driven by a transactional outbox and is safe under at-least-once execution.
10. Step leases use a monotonically increasing fencing token; an expired worker cannot commit a result for an older lease generation.

## Language-model boundaries

11. Original user text or transcript is always preserved.
12. Structured interpretation supplements the original request; it never replaces it.
13. Natural-language interpretation is model-based, not keyword-based.
14. A model may recommend risk or workflow but may not grant permissions.
15. Security policy may increase model-suggested risk and may never be weakened by model output.
16. The semantic interpreter chooses capabilities, not credentials or concrete accounts.
17. Provider and model implementations remain replaceable behind adapters.
18. Natural-language text and model output cannot approve a privileged or destructive action.

## Telegram boundaries

19. Topic mapping uses `chat_id + message_thread_id`, never topic names.
20. A reply to a task-linked message is stronger affinity evidence than recent-topic context.
21. Ambiguous continuation must ask for clarification rather than silently attaching to the wrong task.
22. Only allowlisted users and chats may create executable tasks.
23. Approval callbacks are single-use and tied to one persisted step.
24. Unknown projects may be created only through the dedicated allowlisted project-intake topic;
    ordinary natural-language tasks cannot provision projects.

## Execution safety

25. Coding tasks use per-task Git worktrees.
26. Default execution is non-root.
27. Sandboxes must not receive the Docker socket.
28. Credentials are scoped to the worker or provider profile that needs them.
29. Review execution is read-only and receives no production secrets.
30. Host-privileged operations require explicit action-specific approval.
31. Actual commands, diffs, and deployment targets are re-evaluated for risk at runtime.
32. Repository content, attachments, retrieved content, and model output are untrusted input.
33. Sandbox network access is deny-by-default and enabled only by explicit policy.
34. Host safety cannot depend on parsing or classifying arbitrary shell syntax correctly.
35. Approval covers an immutable action envelope; any material change invalidates it.
36. Plan approval authorizes the declared stack, not downloading or installing missing bytes;
    every toolchain or dependency installation requires its own source- and hash-bound approval.

## Context

37. Full Telegram topic history is never sent by default.
38. Large instruction or memory files are not repeatedly injected as a single prompt blob.
39. Context assembly is budgeted, source-linked, and task-scoped.
40. Unchanged summaries and indexes are reused by content hash.
41. Git and the filesystem remain the source of truth for repository content.
42. Every run records the workflow, policy, configuration, prompt, and repository revisions needed to explain its behavior.

## Scope and operations

43. The MVP runs as a modular monolith plus workers and PostgreSQL.
44. Logical component boundaries do not require separate services.
45. One heavy execution slot is the default on the current VPS.
46. Vector search, graph storage, formal durable runtimes, and web dashboards require measured justification.
47. An implementation step must not silently implement later roadmap items.
48. Cost, token, attempt, duration, storage, and concurrency limits are enforceable policy, not advisory metadata.

## Managed plan revisions (PR5)

- The executable plan is the work package's head revision and its immutable items.
  `managed_goals.proposed_plan` is retained only as legacy data. A proposal persists
  a draft before any task can start. Its hash includes the server's scope revision
  and repository commit; `start_task` binds the revision ID, hash, scope and ordinal.
- Item contracts require stable local IDs, goals, concrete scope, expected results,
  acceptance checks and explicit dependencies. New revisions store a stable
  topological order. Materialization checks completed dependencies, including an
  unchanged completed prefix from an earlier revision.
- Multi-item, large, elevated-risk, explicitly gated and environment-changing plans
  require the goal owner's exact revision/hash/scope approval. Delegation can approve
  only a single low-risk item. Plan approval never grants deployment, installation,
  protected-ref or host permissions (invariants 14–18, 30, 35–36 still apply).
  A managed approval queues one durable lead wakeup; only the lead dispatches tasks.
- Rolling revisions require dispatch stopped and the current task settled. All
  materialized contracts, including the cursor item, remain immutable. Reapproval
  preserves the managed integration branch, base and accumulated head. Owner pause
  remains a pause through revision and approval.
- Legacy conversion runs under the goal lock on its next lead wakeup. It creates
  a draft only for an unmaterialized proposal, assigns deterministic `legacy-N`
  IDs where absent, and commits the revision pointer and an idempotency marker
  together. Existing materialized history and terminal goals are not rewritten;
  the old JSON is retained. Paused goals remain paused, and no approval is inherited.
  An unavailable legacy repository is represented by a null source commit; new
  proposals require a server-resolved commit. Malformed legacy proposals are marked
  invalid and cannot execute. There is no schema or Alembic change.

## Managed strong planning (PR6)

- `request_plan` and `revise_plan` queue a planning turn on `project_lead`.
  The turn pins the project's `planner_strong` assignment and uses inference phase
  `planner`, with the shared budget reservation, frozen request and durable receipt.
  A stored receipt is replayed; an uncertain send is never paid again automatically.
  `plan_ready` / `plan_failed` is delivered to the lead on a subsequent turn.
- Planner output creates a PR5 draft, never an approval or a permission. The server
  prepends the immutable materialized prefix and rechecks scope, revision generation
  and repository head before publishing. Dispatch cannot start while planning is pending.
  Failure preserves the goal's lifecycle, produces a reasoned lead event and topic notice,
  and permits a later request. Existing work ceilings still require the owner's decision.
- CLI planning reads a regular-file-only export of the frozen commit, without `.git`,
  mounted read-only at `/workspace`. The repository itself is not mounted. Exports reject
  path escapes, symlinks and submodules and are capped at 64 MiB / 20,000 archive entries.
  Files/directories are also host read-only. API planning receives bounded `read_repo`
  context from the same commit, including a manifest and explicit truncation markers.
- Snapshot names depend only on the durable invocation UUID under `/tmp`. Process intent
  commits before the export is created. Normal cleanup removes the export only after
  runtime cleanup; startup reconciliation uses the saved process envelope to retry cleanup.
  A failed cleanup retains both its process ownership record and snapshot.
- Alembic `a6c9e2107b32` extends only the inference phase CHECK. Existing plans, receipts,
  approvals and grants are not rewritten. Historical `propose_batch` / `propose_plan`
  grants resolve to `request_plan`; their supplied plan is ignored. New grants use the
  new names. `request_revision` remains only an input alias of `amend_goal`.

## Fixed executor (PR7)

- New task dispatch materializes `fixed_executor.v1` with
  `execution-contract.v2`. Complexity, topic workflow defaults and legacy planning
  flags cannot select stages or create a `plan` step. The worker receives one task
  contract and an explicit instruction to execute it without planning or expanding
  scope. Legacy planning requests persist a deduplicated task event and wake the
  project's live lead, including while waiting or paused; without a live goal the
  task event remains durable. This signal does not call a planner or amend the task.
- The shared executor stages are technical preflight/context, isolated worktree,
  sandbox CLI execution, dependency preflight, validation and review. Change results
  also prepare delivery artifacts before the existing immutable approval boundary.
  Only the lead's `accept_result` authorizes managed delivery. Applier and static
  publisher remain the consumers after approval. New chains do not publish previews
  or invoke the legacy horizon acceptance handler.
- Read-only/research work uses the same stages with `execute_agent` and a
  `task-report.v1` output contract. Worktree mounts remain read-only; technical
  installers skip report tasks. Validation requires unchanged Git state and a
  nonempty report with findings, sources and limitations. Independent review sees
  the complete hash-checked report and task outcomes. References supplied by a
  worker are not server-verified retrieval. Approval binds the report through both
  validation and review hashes and the lead's `accept_spec.report_hash`; accepting
  it consumes the approval without Git apply or publication. Source-backed research
  contracts retain their old meaning. Repository-only task contracts do not acquire
  a web-research capability requirement.
- Repair uses the existing fingerprinted scheduler. Dispatch freezes the recovery
  policy in `Run.selected_route`; every subsequent outcome uses that policy.
  Repairs across all runs of a task count toward its saved cap, including failed
  or cancelled attempts. Changing settings or budget epochs cannot add attempts.
  A failed v2 task cannot be rematerialized by the sequencer; new work requires a
  lead decision on the plan. Existing safe operator recovery and fencing remain.
- There is no DDL or rewrite of stored graphs. Stored coding v4, discussion-agent
  and adaptive-worker-trial runs keep their steps, handlers, approvals and recovery
  semantics. The task-level cap also counts historical repairs on the same task.
  Old planner handlers, handoff, definitions and horizon machinery remain for PR8.
  PR8 can remove the old registry/templates, per-task planning/tier producers and
  handlers, planner handoff and horizon paths after its upgrade preflight. Preserve
  the fixed factory (move it out of `compiler.py` if that module is removed), shared
  execution constraints, acceptance evidence needed by apply, and historical data.
