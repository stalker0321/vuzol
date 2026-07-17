# Voice Transcription and Semantic Interpretation

## Boundary

Step 05 converts Telegram intake into a provider-neutral `TaskDraft` schema version `1.4` using
prompt version `architecture-routing-v8`. The original Telegram text remains in
`telegram_intake_messages` and
`tasks`; voice bytes are retained as a bounded private artifact, while the raw transcript is stored
on both the task and immutable interpretation record.

The interpreter receives only the current intake, mapped topic/project, reply-linked task, bounded
active-task titles, configured project summaries, and the capability vocabulary. It never receives
full topic history, credentials, provider accounts, repository contents, or unrelated tasks.

Alongside the full `goal` and compact `normalized_title`, every new draft includes a one-line
`task_summary` for user-facing Telegram projections. It describes what the task asks to achieve and
must not claim execution progress or completion. Pre-1.4 persisted drafts remain readable through
a deterministic fallback to their normalized title or goal.

All user text, transcripts, summaries, quoted text, repository excerpts, and retrieved content are
serialized as untrusted input beneath a fixed system instruction. Provider output must validate as
the strict `TaskDraft`; free-form output cannot reach routing or execution.

## Runtime flow

`vuzol-interpreter` owns only `telegram_file` and `interpretation` outbox destinations. Telegram
delivery continues to own only `telegram`.

1. Text intake transactionally creates an `interpretation` item.
2. Voice/audio intake creates a `telegram_file` item. The runtime downloads bounded bytes through
   the Telegram adapter, persists a hashed private artifact, transcribes it, then queues semantic
   interpretation.
3. The primary interpreter receives one schema-repair attempt after invalid output. Configured
   fallback interpreters are tried afterward. Provider outages use fenced retry/backoff and then
   dead letter; they do not erase the original request.
4. A validated interpretation and the task projection are committed together. No workflow or
   executor is invoked in Step 05.
5. Unknown project IDs, contradictions, privileged capabilities, ambiguous binding, and uncertain
   dangerous voice transcription are tightened by Python policy. Required confirmation is emitted
   through the normal Telegram transactional outbox with escaped rendering.

Downloads and model calls occur outside unrelated database transactions. Claims use the existing
lease owner/generation fencing model, so crashes can be reclaimed and stale workers cannot commit.

## Providers and configuration

The implemented API adapter uses OpenAI-compatible `/chat/completions` JSON output and
`/audio/transcriptions`. Provider-specific HTTP details remain behind `SemanticInterpreter` and
`Transcriber` protocols; task services depend only on provider-neutral contracts. Tests use fake
adapters and require no live model.

Configure provider registry entries with `provider = "openai-compatible"`, `model`,
`api_base_url`, and a consumer-scoped `credential_reference`. Select them with:

- `VUZOL_INTERPRETATION__PROFILE_ID`;
- optional `VUZOL_INTERPRETATION__FALLBACK_PROFILE_IDS`;
- `VUZOL_INTERPRETATION__TRANSCRIPTION_PROFILE_ID` for voice;
- bounded poll, lease, attempt, timeout, and retry settings shown in `.env.example`.

Start the optional runtime with `docker compose --profile interpretation up`. It also needs the
single Telegram bot token for attachment reads; it does not introduce another bot.

## Evaluation and automatic-execution gate

`tests/fixtures/interpretation/step-05-v1.json` contains 45 versioned Russian and English fixtures
covering text, voice errors, project context, research, ambiguous continuations, embedded
instructions, natural-language controls, and file processing. The evaluation harness reports
schema-valid rate and category failures plus four zero-tolerance safety metrics: privileged
approval, must-not-execute, risk underprediction, and incorrect binding.

Automatic execution is disabled by default. Enabling it requires a current report file whose
`automatic_execution_eligible` field passed the configured schema-valid threshold and every safety
threshold. Step 05 itself never executes interpreted work.

The automated harness validation runs all 45 fixtures through a test-only oracle adapter and
reports 45/45 schema-valid with zero safety violations. This validates fixture loading, scoring,
category reporting, and the gate itself; it is not a quality claim for a production model. No live
provider is eligible until its own persisted report passes the same thresholds.

## Known failure behavior

- No provider profile: interpreter runtime fails closed during startup; intake remains durable.
- Invalid output: one repair attempt, then fallback.
- All providers unavailable: bounded retry, then dead letter; task remains non-executing.
- Unknown project: project is cleared and clarification is mandatory.
- Uncertain dangerous transcript: confirmation is mandatory.
- Natural-language approve/reject: recorded only as semantic intent and never consumes an approval.

Live latency and cost are not yet available because no production interpreter/transcriber profile
is enabled. Provider responses retain request IDs and token counts in provider-neutral results for
later usage accounting.
