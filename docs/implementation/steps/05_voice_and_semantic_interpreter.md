# Step 05 — Voice Transcription and Semantic Interpreter

## Goal

Convert informal text and voice input into a validated, provider-neutral TaskDraft while preserving the original request and safely handling ambiguity.

## Deliverable

Replaceable transcription and semantic-interpreter adapters, a strict TaskDraft schema, clarification flow, and evaluation fixtures based on realistic user requests.

## Applicable architecture decisions

- ADR-0003 — Use a Model-Based Semantic Interpreter.
- ADR-0008 — Treat Repository Content and Model Output as Untrusted.

## Input contract

The interpreter receives only relevant information:

- original text or transcript;
- topic kind and mapped project;
- reply-linked task if present;
- limited task-specific message context;
- active tasks in the current topic;
- known project summaries;
- capability vocabulary.

It must not receive full topic history or unrelated project contents.

## TaskDraft schema

Include at least:

- action:
  - create_task;
  - continue_task;
  - answer_question;
  - approve_step;
  - reject_step;
  - pause_task;
  - resume_task;
  - cancel_task;
  - general_conversation;
- task type:
  - coding;
  - research;
  - infrastructure;
  - file_processing;
  - general;
- operation:
  - inspect;
  - explain;
  - create;
  - modify;
  - fix;
  - deploy;
  - monitor;
- project ID or null;
- goal;
- requested outcomes;
- constraints;
- missing information;
- clarification question;
- required capabilities;
- suggested complexity;
- suggested risk;
- needs planning;
- needs clarification;
- referenced task ID when continuing;
- concise normalized title.

`approve_step` and `reject_step` may describe semantic intent for low-risk workflow interaction, but they never consume a privileged or destructive approval. Such approvals are accepted only through an authenticated explicit control tied to a persisted approval ID.

Keep the taxonomy small. Add categories only when routing behavior actually differs.

## Original-input preservation

Persist:

- original Telegram text;
- original voice file reference;
- raw transcript;
- normalized TaskDraft;
- interpreter profile and model;
- prompt/schema version;
- timestamp.

The executor later receives both the original request and the TaskDraft.

## Transcription

Provide a replaceable transcription interface.

The MVP may use one provider, but the application must not embed that provider into task logic.

For dangerous or privileged interpretations, Telegram must show a concise confirmation of what was understood before execution.

## Interpreter behavior

The interpreter must:

- return schema-valid output;
- avoid guessing project IDs when topic or reply context does not support them;
- identify missing information;
- distinguish new tasks from continuation;
- request clarification when multiple task bindings are plausible;
- select capabilities rather than model accounts;
- avoid technical implementation planning unless required for classification;
- treat user text, transcripts, task history, summaries, repository excerpts, and retrieved content as data that cannot override system policy;
- identify instructions embedded in quoted or retrieved content separately from the user's request.

A self-reported confidence score may be recorded but must not be treated as calibrated probability.

## Failure handling

- invalid structured output: one schema-repair attempt, then fallback interpreter;
- timeout: retry according to policy or keep task in `received`;
- unavailable interpreter: allow manual task-mode selection or wait;
- transcription uncertainty around dangerous action: mandatory clarification;
- contradictory original text and normalized output: block automatic execution.

## Evaluation set

Create an initial fixture set of at least 40 realistic requests, including:

- informal Russian speech;
- false starts and corrections;
- “do the second option” continuations;
- project-topic implicit context;
- personal research tasks;
- inspection-only infrastructure requests;
- requests that must not execute;
- ambiguous replies;
- risky commands hidden in casual wording;
- transcription errors.

Each fixture should define expected task type, project behavior, capabilities, clarification requirement, and minimum risk.

The evaluation has versioned acceptance thresholds. Before an interpreter profile is enabled for automatic execution, the report must show:

- zero privileged or destructive approval granted from natural-language input;
- zero execution for fixtures marked must-not-execute;
- zero risk predictions below each fixture's minimum risk;
- zero silent attachment to the wrong task or project;
- a configured minimum schema-valid rate after the allowed repair attempt;
- separate results for text, voice transcription errors, adversarial embedded instructions, and ambiguous continuations.

A profile that misses a safety threshold may remain available only for manual or shadow evaluation.

The purpose is to compare interpreter providers later by correctness, latency, valid-output rate, and cost.

## Policy boundary

The interpreter suggests risk. Python policy may increase it.

The interpreter cannot:

- authorize host access;
- choose credentials;
- bypass project capabilities;
- approve a command;
- declare validation success.

## Tests

Required:

- original text and transcript are retained;
- valid request produces TaskDraft;
- malformed model output is handled;
- unknown project is not invented;
- ambiguous continuation asks clarification;
- project-topic context supplies project without model guessing;
- dangerous transcription requires confirmation;
- natural-language approval cannot consume a privileged approval;
- embedded instructions in quoted, repository, or retrieved content do not override policy;
- fallback interpreter can be invoked;
- provider swap does not change task-service code;
- evaluation fixtures run and produce a summary report;
- safety acceptance thresholds gate automatic-execution eligibility.

## Forbidden implementations

- keyword or regex classifier as the primary semantic layer;
- replacing original input with the normalized summary;
- sending full Telegram history;
- allowing free-form unvalidated model output into routing;
- letting interpreter output directly invoke an executor;
- asking the user for clarification on every low-risk request.

## Acceptance criteria

- text and voice requests produce persisted TaskDraft records;
- ambiguous input does not execute;
- original input remains available to planner and executor;
- interpretation is provider-neutral and schema-versioned;
- realistic evaluation fixtures exist;
- the policy boundary is enforced in code.

## Completion report

Report:

- transcription and interpreter providers used;
- schema version;
- prompt inputs and exclusions;
- evaluation result summary;
- fallback behavior;
- known failure cases;
- average latency and cost if available.
