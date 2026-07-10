# Step 01 — Repository Foundation

## Goal

Create a small, typed, testable Python application that can evolve into the task system without premature microservices or framework lock-in.

## Deliverable

A runnable repository with:

- application package;
- worker entry point;
- configuration boundary;
- test suite;
- lint, formatting, and type-check commands;
- structured logging foundation;
- Docker development packaging;
- documentation update workflow.

## Applicable architecture decisions

- ADR-0004 — Start as a Modular Monolith.

## Scope

Implement:

- Python 3.12 project;
- dependency management using one modern tool chosen by the implementation agent and documented;
- modular package layout;
- application and worker CLI entry points;
- FastAPI health endpoint or equivalent internal health surface;
- pytest setup;
- static typing;
- lint and formatting;
- basic structured JSON logging;
- Dockerfile and local Compose skeleton;
- CI or a local `make check` equivalent;
- committed dependency lock file and reproducible clean-checkout install;
- secret scanning and dependency vulnerability audit commands suitable for CI.
- one-time import of this documentation pack into the repository root, preserving `PROJECT_OVERVIEW.md` and `docs/` paths;
- Git `origin` configured with the repository SSH URL and the initial branch aligned with the GitHub default branch.

## Documentation bootstrap

At repository initialization, copy the current documentation pack into the project repository and verify it against `MANIFEST.txt`. Do not copy local credentials, caches, logs, temporary extracts, worktrees, or other `vuzol-local` runtime directories.

After the first project commit, the documentation inside the project repository is canonical and is updated atomically with implementation changes. The bootstrap copy in `vuzol-local/specs` is not maintained as a second writable source of truth. Any later local export is generated from a committed project revision and labeled with that revision.

The repository remote contains only an SSH URL. Private keys, agent sockets, credential-helper state, and GitHub tokens remain outside the repository.

Suggested logical package layout:

```text
src/
  agent_system/
    app/
    telegram/
    tasks/
    routing/
    workflows/
    providers/
    execution/
    context/
    review/
    storage/
    security/
    observability/
```

Directories may start nearly empty, but package boundaries must be explicit.

## Out of scope

Do not implement:

- Telegram API integration;
- PostgreSQL schema;
- any real model call;
- task state machine;
- Docker sandbox execution;
- provider authentication;
- copying local runtime artifacts or SSH material into the repository;
- Redis, Temporal, DBOS, LangGraph, or MCP.

## Architecture requirements

- Use dependency injection through explicit constructors or small factories, not a global service locator.
- No provider-specific imports in task-management modules.
- No module should import a Telegram library outside the Telegram boundary.
- Configuration is loaded once and passed through typed settings.
- Business logic must remain testable without starting a web server.

## Required commands

The repository must expose simple commands equivalent to:

```text
run app
run worker
run tests
run lint
run format-check
run type-check
run all checks
```

The exact command names may vary but must be documented in `README.md`.

## Logging

Create a small logging contract containing at least:

- timestamp;
- severity;
- service or process;
- event name;
- correlation or task ID when available;
- human-readable message;
- optional structured fields.

Do not place secrets or full model prompts in default logs.

## Docker

Create:

- one application image;
- one development Compose file;
- no Docker socket mount;
- non-root runtime user where practical;
- health check for the app process.

PostgreSQL may appear as a placeholder service but its application integration belongs to Step 03.

## Tests

Required:

- package imports successfully;
- settings validation has a unit test;
- health endpoint or health function reports ready state;
- JSON logger adds expected structural fields;
- application and worker entry points fail clearly on invalid configuration;
- all quality commands pass from a clean checkout;
- lock file is enforced and dependency audit results are visible without silently changing dependencies;
- a fixture secret is rejected by the configured secret scan.

## Forbidden implementations

- one giant `main.py`;
- global mutable application state;
- secrets committed to the repository;
- shell scripts as the only source of runtime behavior;
- starting multiple physical microservices for logical modules;
- adding agent frameworks “for future use.”

## Acceptance criteria

- a fresh checkout can be installed and tested from documented commands;
- the committed documentation matches the imported manifest and all future step paths resolve inside the repository;
- `origin` uses the expected GitHub SSH URL without embedded credentials and the initial branch matches the remote default;
- app and worker processes start independently;
- test, lint, formatting, and type checks pass;
- no business logic depends on Telegram, PostgreSQL, or a model provider yet;
- package boundaries match the long-term architecture;
- container runs without privileged mode or Docker socket;
- CI and local checks use the committed dependency resolution and include secret scanning.

## Completion report

The agent must report:

- chosen dependency and quality tools with reasons;
- final repository tree;
- commands used;
- tests executed;
- unresolved risks;
- files changed.
