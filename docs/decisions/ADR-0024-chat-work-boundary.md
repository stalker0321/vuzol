# ADR-0024: chat replies are not user-visible work

Status: accepted by owner for implementation (T090); production rollout pending.
Base: T089 / ADR-0023. No migration, new queue, planner, executor or intent array.

## Decision

The opt-in `chat.enabled` boundary admits free project-topic text as existing
discussion intake, without immediately creating a numbered Task or intake card.
Jev remains first: one native intake of the full raw turn, using bounded existing
task references (including a scoped reply target). No second Jev/structuring
call, keyword-only router or generative classification fallback is added.

- respond / plan / status / capture: reuse discussion memory and the hidden
  discussion_agent Task/Run. This starts automatically ONLY as a read-only
  answer. Planning here means a conversational sketch, not a WorkPackage
  mutation or authorization to implement it. Plain replies are delivered into
  the project topic. Full raw current input is retained separately from memory
  packing and display bounds; bounded existing task/result snapshots are data.
- execute_request: tighten with the existing interpretation policy, promote to
  an ordinary numbered Task and dispatch the existing workflow. Original raw
  text, source turn and Jev receipt remain bound. `chat_work` admissions require
  manual Start even if legacy automatic execution is enabled. Work execution,
  review, exact-result approval and apply are otherwise unchanged.
- mixed / control / ambiguous effect: plain clarification; no partial Task,
  agent invocation, control or plan mutation. Continuing existing work also
  directs the user to existing controls rather than implicitly resuming it.
- T103: conversational continuation/review/amendment is independent of managed
  Task bindings. Confirmed read-only effects may use the existing responder even
  with a missing reference, asking a focused question if evidence is insufficient.
  Unresolved **operational** targets remain closed; no ambiguous act is converted
  into a read-only fallback merely because canonical abstain effect is respond.
  Original native choices/typed uncertainty remain in the receipt. Full detail
  and separate document/context bounds: `docs/CHAT_INTENT_CONTEXT.md`.

## Independent responder

`chat.profile_id` is one administrator-configured account/profile, independent
of project `/model`, planner/reviewer choices and automatic executor routing.
The requested deployment selects Sol via an existing Codex CLI subscription
profile, not a model API key. Enabled chat requires a known enabled sandboxed
CLI executor capable of repository-read / architecture. The profile remains
provider-neutral in code; no fixed model name or credential is tracked here.

Each new reply run snapshots a code-owned exact profile ID plus its non-secret
configuration revision in existing Run.selected_route. Claim-time routing
requires the original fingerprint, execute_agent and exactly repository_read;
project worker preferences and even declared cross-profile fallbacks are ignored.
Missing/disabled/reconfigured profiles block visibly rather than switching
models/accounts. Existing budgets, concurrency, timeout/cancellation, rootless
preflight and externally enforced read-only workspace/Git mounts remain intact.

## User surface and failure behavior

Hidden replies have no task numbers and no Start/approval/status/history cards.
Dashboard filtering is retained; status/history producers and delivery also
fence hidden tasks defensively. Completed replies remain ConversationTurns.
Permanent intake/agent failures and route/quota blocks produce bounded plain
chat notices, never task controls or raw exception text. A quota-blocked hidden
reply does not indefinitely serialize later turns behind itself.

## Compatibility / deployment boundaries

Default is off; the previous legacy and project-discussion paths remain readable
and usable. Existing explicit slash/package controls keep their contracts.
T090 changed plain project text, including replies to task cards. T100 extends
the same chat boundary to supported documents (see `docs/CHAT_DOCUMENTS.md`),
including grouped uploads and explicit whole-set failure. T100 is prepared, not
deployed. Voice and non-project topics retain the old intake path; no silent
activation of a transcription/vision API. Ordinary photos remain unsupported.
Already-created tasks are not replayed, auto-started, renumbered or cancelled.

Activation requires `VUZOL_CHAT__ENABLED=true` and an explicit
`VUZOL_CHAT__PROFILE_ID` consistently on ingress, interpreter and workers, plus
the existing supervised CLI executor ready to process read-only agent steps.
Turning on chat is NOT equivalent to turning on unrestricted code execution or
legacy automatic execution. Do not merge T088 or bypass missing validation,
seccomp, account access or rootless startup gates to activate a responder.
T090 implementation/tests do not claim live Sol or Telegram end-to-end success.
