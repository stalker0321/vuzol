# Discussion to implementation handoff (T105)

Implementation prepared; activation requires a separately approved rollout.
No schema migration, new planner, service, queue or extra model call.

Jev still classifies the whole turn once. An explicit `execute_request` with
`conversation_context` means new work based on discussion, including conversational
`continue`, `amend`, `correct` and `review_result` relations. A real managed Task
target retains the previous continuation/control checks. Read-only chat is still
hidden; mixed, uncertain and unresolved operational requests still clarify.

At promotion, the runtime freezes the existing memory pack (summary, whole retained
turns, accepted decisions, recall) and current canonical source. Only fully delivered
assistant replies become work evidence, also with memory disabled. Original prepared
reference documents are recovered from the same session even when older than the
rolling summary overlap. Captions, filenames, file hashes and complete extracted text
are retained; private local paths are not sent to the model. Latest user choices and
conditions remain verbatim. Rejected assistant proposals, recalled memories and file
instructions are context/data, never operational approval.

The runtime-owned `chat_work_context` lives inside `TaskSpecRevision.spec`, not the
model-authored TaskDraft. Its source IDs, ordinals, hashes, summary revision and
scope are validated before provider spend. The original raw Task input and its Jev
provenance are unchanged. Corrections carry the frozen evidence into the next spec
revision. A materialized run is pinned to its spec, including later repair steps;
silent mid-run specification changes fail closed. Later chat or summary publication
does not replace the snapshot.

The existing provider `ContextItem` contract transports lossless 20k chunks to
ordinary planner/executor/research/synthesis requests and independent review, both
single-partition and cross-partition. Discussion and diff chunk batches are verified
separately. Routing includes discussion size before reserving budget; review checks
the combined full context against its existing role bound before reserving/calling.
No role/token/cost/output allowance is raised.

Admission remains 40k ordinary text including framing, separately 50k extracted
document text, 90k aggregate. Missing context, source mismatch or overflow produces
clarification/attention without a partial work task or truncated references. The
worker still determines the agreed scope from that evidence, not a second generated
specification; it must clarify genuine ambiguity. Currently all historical reference
documents are retained as a whole set, not heuristically selected or discarded.

Only explicit implementation produces a numbered Task and manual Start card.
Existing workflow, validation, independent review and exact-result approval/apply
remain in charge. This change does not authorize deployment, replay old Telegram
turns, automatically resume work or implement any discussed website by itself.
Legacy tasks without a snapshot retain their prior provider inputs.
