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

## Context

36. Full Telegram topic history is never sent by default.
37. Large instruction or memory files are not repeatedly injected as a single prompt blob.
38. Context assembly is budgeted, source-linked, and task-scoped.
39. Unchanged summaries and indexes are reused by content hash.
40. Git and the filesystem remain the source of truth for repository content.
41. Every run records the workflow, policy, configuration, prompt, and repository revisions needed to explain its behavior.

## Scope and operations

42. The MVP runs as a modular monolith plus workers and PostgreSQL.
43. Logical component boundaries do not require separate services.
44. One heavy execution slot is the default on the current VPS.
45. Vector search, graph storage, formal durable runtimes, and web dashboards require measured justification.
46. An implementation step must not silently implement later roadmap items.
47. Cost, token, attempt, duration, storage, and concurrency limits are enforceable policy, not advisory metadata.
