# Chat intent and bounded context (T103)

Implementation only, not activated by this change. Extends T100/T102 without a
new subsystem, planner, service, queue, migration, intent array, second Jev call,
or autonomous work/control authority. Jev prompt v4, read-only reply prompt v5.

## Effect is independent of conversational relation

Runtime supplies `conversational_context=true` only on opt-in project chat.
Jev's existing target question gains `conversation_context`: the current topic's
conversation, delivered answers, proposals, critique or supplied reference files,
not a managed Task. Existing legacy intake defaults remain unchanged.

- respond/plan/status/capture may continue, amend, correct or review a conversation
  without a managed Task ID. These always use the existing hidden read-only Sol
  workflow, not a numbered Task, Start or approval card.
- A read-only effect with an unresolved reference may reach that same responder.
  Its prompt identifies the missing binding; it must use available evidence or
  ask a focused question, never invent an object/state or implement anything.
- Execution continuation/amendment and controls still require managed bindings;
  missing/no-target/conversation-only bindings cannot become new work silently.
  A **new** explicitly requested implementation may use conversation/documents
  as its specification, but still creates ordinary work requiring manual Start.
- Truly unknown effect, unsupported mixed requests and controls remain plain
  clarification/attention. No read-only fallback for ambiguous execute/control,
  no execution of a subset, no automatic resume or approval.

Classify the whole coherent goal, not each verb. Several supported steps of one
goal remain execute_request; mixed means a single existing route cannot preserve
the full request. All captions, conditions and negations remain raw downstream.
The canonical decision.v3 stays readable. Its receipt additionally retains
`selected_choices`, `reference_resolution` and `clarification_reason`, so an
abstain normalized to respond never hides the original requested effect.

## Explicit partitions, not repeated cap increases

- **50000** extracted file-text characters total per message/album. Existing
  per-file25MB, count, encoding, archive/XML/PDF-page and parser time/resource
  gates remain separate. No OCR, photo/vision or additional supported formats.
- **40000** other characters in the compiled reply prompt: conversation, current
  caption, instructions, document metadata, summary, decisions, recall and task
  snapshots. This is the existing application guard, not a model-window claim.
- The derived original_input maximum is **90000**. Runtime supplies the actual
  included document-text count to ProviderRequest; without documents the old
  40000 guard still applies. Token, output, cost and route/model guards are unchanged.
- The included reference texts across current turn and history share the50000
  partition. Multiple individually admissible uploads are not unlimited context.

Canonical turns and private prepared document snapshots remain intact, including
existing T100 JSON snapshots. Memory uses trusted snapshots to separate captions
from text; a user-typed marker cannot turn ordinary prose into a document budget.
New file text is rendered verbatim instead of escaped JSON, so newlines/quotes
do not waste the document budget. No private content_uri is rendered to models.

The opt-in reader fetches **all** uncovered turns (delivered assistant replies
only when memory is enabled), with a bounded source-size admission check before
fetching raw content. There is no last20 cutoff for uncovered turns. It retains
the existing six-ordinal summary overlap, removing whole **covered** turns only
when necessary. Uncovered critique/constraints/documents are never prefix-cut
or removed to fit another upload. The final prompt, not only raw history, is
checked; only covered overlap can be compacted for framing overhead.

True overflow gets an explicit plain notice (or focused retry callback alert),
no hidden/public partial task or provider reply. Stored history/intakes remain.
This does not promise unlimited context or automatic recovery of an arbitrarily
large backlog: narrow the request/reference set when necessary. Existing same-call
optional summaries advance only after complete confirmed answer delivery; no
extra summarizer/model call was added. A pack's uncovered-completeness marker
allows safe summary publication after covered-only compaction. AcceptedDecision
authority, recall provenance and retry source binding stay unchanged.

## Verification and rollout

Ordinary unit and real isolated TEST PostgreSQL regressions use fake Jev/CLI/TG
transports, actual parser children and existing workflow/routing/delivery. Cases
include proposal -> critique -> two files -> two followups, old unresolved target
choices, conditions, no cards, >20 uncovered turns, covered-only compaction,
50000 acceptance/50001 aggregate rejection, multiline Unicode documents, complete
large input through the CLI adapter, same-source retry and manual work controls.
No separate bench or live-model quality/E2E claim. Production needs its own
owner-authorized frozen common-image/executor rollout; no DB migration or
account/credential/security/daemon change. No automatic replay of old turns.
