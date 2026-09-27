# Transport-neutral controls (WP09)

Identical user actions over Telegram and CLI/API; visibility without
micromanagement.

## 1. Application commands

`src/vuzol/workflows/application.py:TaskControlService` — the single
application boundary for task commands `start/pause/cancel/resume/inspect`:

- Every command runs the same domain operations (`workflows/controls.py`)
  with an explicit `Principal(user_id, ingress_source)`; transports differ
  only in how the principal is established (Telegram allowlist vs operator
  `--user-id`), never in the transition applied (parity-tested).
- Mutating commands take `expected_task_version`: a stale control never
  applies (`ValueError: stale task version` → existing dead-letter path).
  `None` preserves the legacy unchecked path.
- Retrying a consumed command is fail-closed: the version moved, so the
  retry is stale — exactly one transition per command (parity-tested via
  `task.pause_effective` event count).
- `inspect` is strictly read-only: no row locks, no writes, no outbox rows
  (row-count tested).

## 2. Principal mapping / backfill

- `tasks.source_chat_id` is nullable; `tasks.ingress_source`
  (`telegram/cli/api/legacy`) records the explicit origin (migration
  `9feb4e9d4de3`, parent `c4e8f1a92b70`, downgrade verified).
- Pre-contract rows are backfilled `ingress_source='legacy'` and keep their
  chat IDs; new CLI/API tasks carry `NULL` chat + their ingress label.
- No sentinel: `chat_id=0` never means "no chat". Telegram surfaces skip
  explicitly on `None`/`0` (`projections.py`, documented at the guard);
  orchestration traces without a chat raise `PermanentDeliveryError`
  instead of addressing chat 0.
- `Principal(user_id=0)` is rejected (`principal_invalid`); callers supply
  real identities, nothing is defaulted.

## 3. Operator CLI

`vuzol-task start|pause|cancel|resume|inspect --task-id --user-id
[--expected-version] [--json]` (`src/vuzol/cli/task.py`, registered in
`pyproject.toml`): exit 0 applied/inspected, 2 usage/validation, 3 domain
rejection. Same service, same CAS, same idempotency as Telegram.

## 4. Notification policy

`src/vuzol/telegram/attention.py:should_notify` — notify only on meaningful
transitions (task/package completion or failure, approval-needed, flagged
attention); transient mechanics (scheduled retry, backoff defer, queue
shuffle, unchanged state, duplicate delivery) stay silent. Each rule,
including every no-notification rule, has a unit test. The policy decision
is pure; delivery-side NOOP/idempotency mechanics are unchanged.
