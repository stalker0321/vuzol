# Telegram dogfood notebook

This runbook covers the small set of checks that must be performed through the real Telegram UI.
It complements hermetic tests; it does not replace them and is never part of CI.

## Test workspace

Create one private project topic named `Vuzol Test` backed by a disposable repository. Register it
as an ordinary project topic so it follows the production ingress, outbox, delivery, workflow, and
projection paths. Do not point it at a real project repository.

Before a session:

1. record the deployed Git SHA and configuration revision;
2. make sure the topic has no active package;
3. select a worker with available quota;
4. write the session date and tester at the top of the notebook;
5. use unique task text such as `[TG-2026-08-10-01]` so database records and logs are searchable.

For every failure preserve the tag, Telegram message link, approximate UTC time, visible card text,
button pressed, and expected result. Add a screenshot only for a rendering problem. Never copy
credentials, raw provider payloads, or environment files into the notebook.

## Release smoke test

Run these cases in order after a staging/dogfood deployment. Stop at the first failed invariant.

| ID | Action | Expected result |
|---|---|---|
| T01 | Send a four-item implementation request and approve its plan. | One plan card exists; execution starts at `1/4`; the compact first line fits mobile width and names the selected worker. |
| T02 | Let item 1 finish. | Item 2 starts without a per-item approval; progress becomes `2/4`; cumulative token usage does not reset. |
| T03 | Press `Остановить` while item 2 is active. | The active task is cancelled once, the package becomes stopped, stale action buttons disappear, and `Возобновить` is shown. |
| T04 | Press `Возобновить`. | A fresh attempt for item 2 starts; item 1 is not materialized or committed again. |
| T05 | Press `Перепланировать`, then send a clearly different instruction for the remaining work. | Telegram explicitly asks what to change; no revision is created before the message; the resulting plan reflects the new instruction and preserves the completed prefix. |
| T06 | Cause a transient provider failure or use the controlled fault fixture. | The package pauses with a useful reason; `Повторить` appears only when retry is safe. |
| T07 | Restore the provider and press `Повторить`. | Exactly one additional bounded attempt starts from the failed item; progress and earlier token totals remain intact. |
| T08 | Let all items finish. | Only one approval is requested, after the final item; the approval card is formatted and contains a concise result rather than an execution transcript. |
| T09 | Approve the final result. | The approval card is edited to the decided state, buttons disappear, the project card becomes completed, and history receives one terminal entry. |
| T10 | Open `/model`, choose a provider/account, then a model/effort. | The same chooser message is edited between stages (or the previous stage is deleted); the final preference is visible and no stale chooser remains. |
| T11 | Press `Обсудить`, then send a question. | Telegram tells the user to send the next message; that message is interpreted as discussion and does not replay an earlier plan. |
| T12 | Restart Telegram ingress/delivery between a button press and projection delivery. | The callback is not applied twice; delivery resumes from PostgreSQL and leaves one current card. |

## Controlled faults

Do not wait for a real provider outage. Add a test-only, default-off fault injector that is accepted
only for the disposable test project and an allowlisted user. It should support one-shot failures
at these boundaries:

- provider timeout before effects;
- provider quota exhaustion before effects;
- Telegram delivery failure before acknowledgement;
- process exit after callback persistence but before control consumption;
- process exit after state transition but before projection delivery.

The injector must be configuration-gated, auditable, consume each fault once, reject production
projects, and never accept arbitrary commands or exception text from Telegram.

## Automation layers

Keep three layers distinct:

1. **Hermetic state-machine tests** feed frozen Telegram updates into ingress/control services and
   assert PostgreSQL state, outbox records, fences, and rendered buttons.
2. **Bot API smoke tests** use the disposable topic to verify real send/edit/delete behavior and
   persist returned message IDs. They do not pretend to press buttons as a user.
3. **Human UI checks** cover actual button presses, mobile rendering, wording, and stale-message
   cleanup. T01–T12 are the notebook for this layer.

Automate the setup and evidence collection behind an operator command such as
`vuzol telegram-dogfood start --project vuzol-test`. The command should create a session ID, verify
the target allowlist, print the deployed SHA, and later export a redacted report from canonical
PostgreSQL state. It must not auto-approve results or operate on arbitrary topics.

## Pass criteria

A session passes only when all T01–T12 rows have an explicit `PASS`. `SKIP` requires a reason and
does not qualify a release that changed the skipped surface. Database state is authoritative when
the UI and logs disagree; that disagreement is itself a failed projection test.
