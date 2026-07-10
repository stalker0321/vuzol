# Step 02 — Configuration, Secrets, and Registries

## Goal

Define typed, replaceable configuration for projects, Telegram scope, model/provider profiles, execution limits, and secrets without coupling the application to one provider.

## Deliverable

Validated configuration and registry services that can answer:

- which users and chats are allowed;
- which Telegram topics map to which logical scopes;
- which projects exist and where their repositories are;
- which model and worker profiles exist;
- what capabilities and limits each profile has;
- where credentials are referenced without exposing them to unrelated modules.

## Applicable architecture decisions

- ADR-0004 — Start as a Modular Monolith.
- ADR-0008 — Treat Repository Content and Model Output as Untrusted.

## Scope

Implement typed models for:

### Application settings

- environment;
- database DSN placeholder;
- Telegram bot token reference;
- allowed user IDs and chat IDs;
- artifact and repository roots;
- concurrency classes;
- retention defaults;
- logging and redaction settings;
- hard limits for task cost, tokens, attempts, duration, artifact size, and external input size;
- configuration revision and reload policy.

### Project registry

Each project includes:

- stable project ID;
- display name;
- repository path;
- default branch;
- allowed capabilities;
- validation command definitions;
- sandbox profile;
- optional project summary path;
- active or disabled state;
- Git delivery policy: retain only, local apply, merge, push, and the approvals required for each;
- network egress policy and allowed destinations.

### Provider/model profile registry

Each profile includes:

- stable profile ID;
- provider;
- model or CLI identity;
- launch mode: API, CLI, supported tool;
- credential reference;
- capabilities;
- concurrency limit;
- context or output limits if known;
- cost or quota class;
- supported task types;
- fallback profile IDs;
- sandbox requirement;
- enabled or disabled state.

### Topic registry contract

Define the model now, even if persistence is added in Step 03:

- chat ID;
- message thread ID;
- topic kind;
- optional project ID;
- accepts new tasks;
- default workflow;
- enabled state.

## Secrets

The repository contains only secret references or example files.

Initial acceptable mechanisms:

- environment variables scoped per process;
- mounted secret files;
- later `sops` + `age`.

The application must not deserialize all provider credentials into one globally accessible object.

Secret values are never part of configuration hashes or persisted snapshots. Snapshots contain stable secret references only.

## Configuration revisions

Each validated configuration load has a stable revision derived from its non-secret normalized content. A run snapshots or references the exact revisions of:

- project policy and validation commands;
- workflow definition;
- routing policy;
- sandbox and egress profile;
- system prompt and output schema.

Restarted work uses those revisions when they remain permitted. Security revocations, disabled projects or profiles, reduced capabilities, and revoked secret references take effect immediately and may block an existing run. Other configuration changes do not silently alter an in-progress run.

## Validation rules

Fail startup on:

- duplicate stable IDs;
- unknown project references;
- fallback cycles;
- capability names outside the known vocabulary;
- nonexistent repository paths when a project is enabled;
- missing required secret references;
- invalid concurrency values;
- non-positive or internally inconsistent hard limits;
- an egress destination outside the configured policy;
- a topic mapped to an unknown project.

## Capability vocabulary

Start with a small explicit set:

```text
repository_read
filesystem_write
code_edit
git
project_shell
network
web_research
transcription
secrets
host_admin
telegram_send
```

Do not create dozens of speculative capabilities.

## Interfaces

Provide registry interfaces such as:

```python
ProjectRegistry.get(project_id)
ProfileRegistry.find_candidates(required_capabilities)
TopicRegistry.resolve(chat_id, message_thread_id)
SecretResolver.get(reference, consumer_scope)
```

Concrete configuration format is replaceable. Business logic must not directly parse YAML or environment variables.

## Out of scope

- live provider health;
- quota tracking;
- database persistence;
- routing scoring;
- actual credentials for production;
- remote workers.

## Tests

Required:

- valid configuration loads;
- every invalid condition above fails with a precise error;
- secrets are not included in string representations or logs;
- fallback cycle detection works;
- profile capability matching works;
- project paths are normalized and constrained to the configured repository root;
- unknown topic or profile lookups are handled explicitly;
- configuration revision is stable for identical normalized input and excludes secret values;
- a non-security configuration change does not mutate an existing run snapshot;
- capability or credential revocation blocks affected pending work.

## Forbidden implementations

- untyped dictionaries propagated across the application;
- hard-coded provider IDs in task logic;
- topic-name-based project selection;
- one global `.env` passed to all workers;
- printing configuration objects containing secrets;
- automatic creation of unknown projects from user text.

## Acceptance criteria

- configuration is fully typed and validated;
- registries are testable without external services;
- adding a new provider profile requires configuration plus a future adapter, not changes to task schemas;
- credential access is consumer-scoped;
- configuration changes have explicit temporal semantics and runs remain explainable after restart;
- no Telegram or task logic depends on topic display names;

## Completion report

Report:

- configuration format and why it was selected;
- registry interfaces;
- capability vocabulary;
- secret-handling boundaries;
- validation tests;
- any decisions that require an ADR.
