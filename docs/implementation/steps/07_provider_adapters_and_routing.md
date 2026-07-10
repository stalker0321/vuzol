# Step 07 — Provider Adapters and Routing Policy

## Goal

Implement replaceable model and CLI adapters plus capability-based routing across profiles without letting semantic interpretation or task code depend on one provider.

## Deliverable

A provider-neutral request and result envelope, initial adapters, health and quota state, and a deterministic policy router.

## Applicable architecture decisions

- ADR-0003 — Use a Model-Based Semantic Interpreter.
- ADR-0004 — Start as a Modular Monolith.
- ADR-0007 — Transactional Delivery and Fenced Leases.

## Adapter contract

Each model or executor adapter must expose behavior equivalent to:

```python
execute(request, profile, cancellation_context) -> result
health(profile) -> health_state
usage(result) -> normalized_usage
```

The request envelope should include:

- task and step IDs;
- original user request;
- structured TaskDraft;
- role: interpreter, planner, executor, reviewer, summarizer;
- system-policy version;
- context bundle;
- required output schema;
- timeout;
- budget;
- tool or sandbox reference when applicable.

The result envelope should include:

- normalized status;
- structured output or text;
- provider request/session ID;
- usage;
- finish reason;
- recoverable error category;
- raw-result reference where retention is permitted.

## Initial profiles

Support configuration for:

- Codex CLI account A;
- Codex CLI account B;
- one generic OpenAI-compatible API profile, such as MiMo;
- semantic-interpreter profile;
- optional reviewer profile.

The first end-to-end execution may use one Codex account, but the profile and adapter design must support both accounts without shared auth state.

## Codex isolation

Each Codex account uses:

- separate Unix user or dedicated container identity;
- separate `CODEX_HOME`;
- separate credential storage;
- separate concurrency lease;
- no copied shared `auth.json`;
- serialized execution per profile unless official limits safely allow more.

Authentication failure marks only that profile unhealthy.

## Routing inputs

The policy router receives:

- TaskDraft and original request metadata;
- required capabilities;
- project capability boundary;
- risk;
- workflow role;
- estimated context size;
- user budget mode;
- profile health;
- queue depth and active leases;
- quota or rate-limit state;
- configured fallbacks.

## Routing behavior

The semantic interpreter does not select a concrete profile.

Python:

1. filters profiles by capability and project policy;
2. removes unhealthy, disabled, or exhausted profiles;
3. applies concurrency limits;
4. selects by simple deterministic priority;
5. records the routing decision and alternatives;
6. falls back on categorized provider failure.

The MVP should not invent a complex weighted quality formula before usage data exists.

Suggested initial ordering:

- explicit user-selected profile;
- best configured profile for the role;
- least-busy compatible fallback;
- await user or quota state when no valid profile exists.

## Budget modes

Support simple modes:

- cheap;
- balanced;
- strong.

Budget mode may affect profile priority, reasoning configuration, context budget, and review policy. It must not weaken security rules.

Budget mode is separate from enforceable limits. Policy resolves hard caps for:

- input and output tokens per call and task;
- provider attempts and fallback depth;
- monetary or quota units per step, task, and rolling daily window;
- step and task wall-clock duration;
- tool calls and generated artifact size where applicable.

Before a provider call, reserve the best available cost or quota estimate atomically. Reconcile the reservation against normalized usage afterward. Unknown pricing or usage uses a conservative configured estimate and is never treated as free or unlimited. Crossing a hard cap prevents new calls and moves work to an explicit budget-exhausted or awaiting-user state without weakening validation or security.

## Health and quota

Persist or cache:

- last successful call;
- last failure category;
- unhealthy-until time;
- rate-limit-until time;
- known quota state;
- active lease count.

Do not pretend to know exact quota when the provider does not expose it. Use explicit `unknown`.

## Error taxonomy

Normalize at least:

- authentication;
- quota exhausted;
- rate limited;
- timeout;
- provider unavailable;
- invalid structured output;
- cancelled;
- context too large;
- unsupported capability;
- permanent request error;
- unknown.

## Tests

Required:

- adding a generic profile requires no task-schema change;
- capability filtering works;
- project policy removes forbidden profiles;
- unhealthy profile is skipped;
- Codex accounts do not share auth paths;
- concurrency limit is enforced;
- categorized failure selects configured fallback;
- no-compatible-profile produces a waiting or quota state;
- explicit user profile selection is honored only when permitted;
- usage is normalized where available;
- concurrent calls cannot reserve beyond a hard shared budget;
- provider attempts and fallback chains stop at configured caps;
- unknown usage or price uses conservative accounting;
- a budget-exhausted task retains state and can request an explicit increase;
- raw provider exceptions do not leak into business logic.

## Forbidden implementations

- `if provider == ...` scattered through task code;
- classifier choosing account credentials;
- one shared Codex home;
- undocumented account switching;
- treating unknown quota as unlimited;
- treating unknown cost as zero;
- implementing budget modes only as routing hints without hard enforcement;
- automatic fallback to a profile lacking required capabilities;
- security downgrade in cheap mode;
- speculative quality scoring with no data.

## Acceptance criteria

- provider-neutral model calls work through adapters;
- at least one Codex and one API profile can be configured;
- two Codex accounts are structurally isolated;
- routing is capability-, policy-, health-, and concurrency-aware;
- fallbacks are explicit and auditable;
- the router remains deterministic for identical state.

## Completion report

Report:

- adapter interfaces;
- supported profiles;
- Codex isolation mechanism;
- routing precedence;
- health and quota handling;
- normalized error mapping;
- integration tests and live smoke-test results.
