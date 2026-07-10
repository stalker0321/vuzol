# Implementation Agent Prompt Template

Implement only the requested MVP step.

## Required reading

Read these files before making changes:

- `PROJECT_OVERVIEW.md`
- `docs/ARCHITECTURE_INVARIANTS.md`
- `docs/CURRENT.md`
- `docs/implementation/00_MVP_PLAN.md`
- `<CURRENT_STEP_FILE>`
- `<REFERENCED_ADRS>`

Do not read unrelated future step files unless the current step explicitly references them.

## Repository safety

- Work only inside this project repository.
- Do not modify other projects or global VPS configuration.
- Do not access secrets that are not required by the current step.
- Do not introduce roadmap items marked out of scope.
- Do not silently replace an architecture decision.

## Before implementation

1. Inspect the current repository.
2. Compare existing code with the current step specification.
3. Identify missing prerequisites, contradictions, and migration risks.
4. Produce a concise implementation plan.
5. Stop and ask for a decision if the specification conflicts with an architecture invariant or existing accepted ADR.

For a planning-only pass, do not change files.

## During implementation

- Keep provider-specific behavior behind adapters.
- Add migrations for database changes.
- Add tests with behavior.
- Preserve original user input and audit state.
- Use explicit typed models rather than unstructured dictionaries.
- Keep the application runnable after each coherent change.
- Avoid unrelated refactors.

## Before completion

1. Run every test and check required by the step.
2. Verify every acceptance criterion explicitly.
3. Review the final diff for scope creep and security regressions.
4. Update `docs/CURRENT.md`.
5. Update `docs/CHANGELOG.md` only for completed behavior.
6. Do not mark the step complete when a required test or criterion fails.

## Completion response

Return:

- implementation summary;
- files changed;
- migrations added;
- commands and tests run;
- acceptance-criteria checklist;
- unresolved issues;
- security considerations;
- suggested next action.

Do not claim success without evidence from the required checks.
