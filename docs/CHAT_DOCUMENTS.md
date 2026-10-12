# Project chat documents (T100)

Implementation only; not deployed. Extends T090/T097, no new planner, model call,
queue, runtime service or SQL migration. Existing opt-in `chat.enabled` enables
document intake in registered project topics. The same Jev classification and
hidden read-only responder are reused; work still requires manual Start.

## Supported input

- TXT/Markdown/CSV/JSON: strict UTF-8 (optional BOM) or BOM-marked UTF-16,
  preserved verbatim. Known extensions may use Telegram's octet-stream MIME.
- PDF: extracted text, up to100 pages; encrypted, malformed, textless/scanned
  inputs fail explicitly. No OCR/vision, diagram/image/layout understanding.
- DOCX: main document paragraphs and table-cell text in document order,
  including tabs/breaks. Images, headers/footnotes/comments and layout are not
  extracted; no Office macros, external relationship resolution or ZIP extraction.
- DOC/RTF/XLSX/PPTX, arbitrary archives/binaries and images are not supported by
  this document reader. Ordinary Telegram photos remain outside this change.

Existing size/count/name/archive authorization guards remain. Scope-specific
document MIME handling does not relax legacy voice/non-project intake. A rejected
document in a project chat is persisted as failed intake metadata and a plain
notice; its caption is never processed by itself. No-caption files are context,
not execution authority, even if Jev emits execute_request.

## Whole turns and groups

One accepted message can contain several documents. Telegram `media_group_id`
members are persisted individually on existing discussion classify outboxes,
collected after a3-second quiet window, and handled once by the smallest message
ID. Scope includes chat/topic/user. Every distinct caption remains verbatim and
in order. Admission and sealing share the existing advisory topic lock. A member
arriving after sealing gets an explicit late-file notice, not silent inclusion
or another partial request. A failed member rejects the entire collected set.

The interpreter downloads and extracts all members before classification. One
immutable prepared snapshot is fenced to its existing outbox lease. Downloads
and all parsers together have a120-second maximum, shortened to half the
configured lease when appropriate. Individual PDF/DOCX parsers use fixed Python
module arguments, no shell, stripped credential environment, CPU10s/address
space256MiB/core0 and wall15s bounds, kill+reap on timeout/cancellation. XML is
bounded2MiB, DTD/entity declarations rejected; archives never touch the filesystem.

Original bytes are persisted privately0600 with hashes/metadata, and attached as
existing task-scoped Artifacts to the hidden reply or explicitly created work.
No file bytes or paths are sent to Jev; only text/name/MIME/hash data. Extracted
text is quoted data, not operational authority. The original caption and all
conditions/negations are passed intact. Canonical source turns contain caption
plus document text, so history/memory and same-turn retry retain the documents.
Retry does not reclassify/redownload/reparse or append another user turn.
Failed preparation removes only that lease generation's uncached downloaded
copies, never another concurrent generation's cached files. A valid snapshot
retains its source copies even if classification asks a clarification; this
preserves provenance, but no Task-bound Artifact exists until work/reply is
materialized. Long-term GC of those intake-only snapshots is a follow-up to the
existing outbox/file retention policy, not implemented as a new cleanup service.

Extraction and the complete current turn use the existing40k provider boundary,
not a new24k document cap. No prefix cutting: oversized text/combined turns or a
reply prompt that cannot fit with history receive a plain error before hidden
execution. Text extraction does not imply full fidelity of visual document content.

## Activation/verification boundary

Deploy the same source and frozen updated lock to common consumers and the host
executor; new dependency pypdf6.20.0 must be in BOTH runtime environments.
Existing Docker/sandbox/provider/proxy/security pins, credentials, memory/rich
flags, manual execution controls and DB schema stay unchanged. No new account,
transcription/vision API, startup migration, automatic replay or old task edits.
Host disk headroom is currently critically low; no image build attempted here.

Tests use real isolated TEST PostgreSQL plus fake Jev/downloads, actual bounded
local PDF/DOCX child parsing, normal memory/retry/manual work paths. They do not
claim real Telegram upload or live-model classification E2E. The two earlier
owner files never reached persisted intake; they must be resent after rollout.

Parser reference: https://pypdf.readthedocs.io/en/stable/user/extract-text.html.
