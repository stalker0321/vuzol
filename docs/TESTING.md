# Testing policy

This document is the single source of truth for how Vuzol and projects created
through Vuzol are tested. It is intentionally small and risk-based.

## Goals

1. Protect **invariants** that keep the platform safe and restartable.
2. Keep the suite **maintainable** (readable modules, no megatons of filler).
3. Never force **coverage theatre**: numbers do not create safety by themselves.
4. Keep **new projects** green by default so empty scaffolds are not blocked.

## Two products, one philosophy

| Surface | What it is | Test bar |
|---|---|---|
| **Vuzol platform** (`this repository`) | Control plane that interprets tasks, runs sandboxes, applies Git, and spends budgets | High assurance on safety boundaries |
| **Managed projects** (repos provisioned via «Новый проект») | User/product code Vuzol operates on | Risk-based, minimal scaffold, no platform inheritance |

Both follow the same rule: **test behavior and invariants, not metrics.**

## Risk tiers (platform)

| Tier | Examples | Required tests | Gate |
|---|---|---|---|
| **P0 — safety / money / data integrity** | sandbox isolation, path containment, fenced leases, approval envelopes, budget reservation, secret redaction, CAS apply | Behavioral unit + integration where concurrency or I/O matters | Must pass on every change that touches the area |
| **P1 — product contracts** | TaskDraft interpretation fixtures, workflow legal transitions, provider error mapping, Telegram auth/affinity | Focused unit/integration with fixtures | Must pass |
| **P2 — composition / ops** | CLI wiring, systemd readiness helpers, image contracts | Sparse contract tests for fixed public surfaces | Prefer runtime checks / small contracts over string-scraping source |
| **P3 — glue / pure construction** | “object constructs”, “import succeeds”, private-field asserts | **Do not write** | — |

A change is done when the **relevant tier** is covered, not when a coverage percentage moves.

## What a good test looks like

- Names a **behavior or invariant** (`fails closed when …`, `exactly one commit when …`).
- Asserts an **observable outcome** (state, argv, exit code, persisted row, raised type).
- Uses the **shallowest** layer that can catch the bug (pure unit before full stack).
- Is **deterministic** (no live Telegram/provider/network unless explicitly marked).

## What we do not want

- Tests written only to raise line/branch coverage.
- Tests that restate configuration (“Postgres is configured”, “Makefile mentions X”) without behavior.
- Construction-only tests (`assert Instance is not None`).
- Multi-thousand-line grab-bag files; prefer modules by domain under `tests/unit/<area>/` and `tests/integration/<area>/`.
- Copying the platform quality bar into managed projects.

## Coverage (platform)

Coverage is a **report**, not a product requirement.

- `pytest` records coverage for visibility (`--cov=vuzol --cov-report=term-missing`).
- There is **no hard fail-under percentage** in the default suite. Percent targets force padding once real tests are written.
- CI still fails on **failing tests**, lint, types, and security scans.
- Reviewers may ask for tests when a **P0/P1** path changes without coverage of the new behavior; they must not ask to “get to N%”.

Mechanical review still flags **obvious quality sabotage** (forced success, swallowed exceptions, skipped tests, shell=True). Lowering or removing a coverage floor is not sabotage under this policy.

## Suite layout

```text
tests/
  unit/<domain>/          # fast, hermetic, by module boundary
    _*_helpers.py         # optional shared setup when several files share it
    test_<theme>.py       # one responsibility / risk boundary / adapter
  integration/<domain>/   # postgres / process boundaries when required
  fixtures/               # versioned shared fixtures (e.g. interpretation)
```

**Structure follows responsibility, not line counts.** Split a file only when
themes are genuinely mixed (different adapters, different lifecycle phases with
different fixtures, different product surfaces) and a reader benefits from a
clearer name and smaller blast radius. Do **not** split—or merge—just to hit a
size band. A long file that is one coherent boundary is fine; a short file that
is only half a story is not an improvement.

Markers:

- `postgresql` — real PostgreSQL (concurrency, locking, migrations)
- `docker` — real Docker daemon (rootless target)
- `asyncio` / anyio — async tests

Commands:

```bash
make test              # full hermetic suite (plus report coverage)
make test-postgres     # PostgreSQL-marked integration
make check             # lint, format, types, tests, security
```

## Managed projects (provisioned by Vuzol)

### Scaffold

Provisioning creates a minimal green repository:

- `README.md` — project title and description
- `Makefile` — `make test` target that **succeeds** when no project tests exist yet

`validation_commands` for new projects remains `make test` so the coding validation path has a trusted gate. That gate must not fail on an empty product.

### Project test policy

| Project stage | Expectation |
|---|---|
| Fresh scaffold / notes / no executable behavior | `make test` green with scaffold (no tests required) |
| Library, CLI, or service with real behavior | Add focused tests for the public behavior you ship |
| Security- or money-sensitive logic | Negative tests on the dangerous boundaries |

Rules for agents and humans working **inside** managed projects:

1. Do **not** impose Vuzol’s platform suite, mypy-strict-on-tests, or coverage floors.
2. Prefer one meaningful test over ten scaffolding asserts.
3. When you introduce non-trivial behavior, extend `make test` to run the real project suite (for example `pytest` once the project has a toolchain).
4. Do not fail validation solely because “tests folder is empty” on a brand-new project.

### What Vuzol validates on managed projects

Validation runs only **trusted fixed argv gates** configured for the project (today: `make test`, and optionally lint/format/type/security if configured). System Git facts and mechanical review remain platform-owned. Project gates are about **the project’s own quality command**, not about cloning Vuzol’s internal bar.

## When adding platform tests

1. Identify the tier (P0–P3). Skip P3.
2. Put the test next to its domain (`tests/unit/execution/…`, not a 3k-line dump file).
3. Prefer table-driven cases over copy-paste.
4. For concurrency and locking, use real PostgreSQL (`@pytest.mark.postgresql`).
5. Do not add a test whose only purpose is to execute a line for coverage.

## When deleting or merging tests

Delete or rewrite a test when:

- it only constructs an object / checks `is not None`;
- it only scrapes source text for strings that are already enforced by runtime checks;
- it duplicates a stronger integration or unit case;
- it exists solely to prop up a percentage.

Keep a test when removing it would let a P0/P1 regression ship silently.

## Summary

- **Platform:** risk-tiered behavioral tests; coverage is informational.
- **Managed projects:** green scaffold; grow tests with product risk, not with platform ceremony.
- **Never** block a brand-new empty project because “there are no tests yet.”
