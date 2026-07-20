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

## Coverage (platform vs managed projects)

| Surface | Coverage rule |
|---|---|
| **Vuzol platform** | Temporary **90%** fail-under floor (pytest + Makefile precision=6). Mechanical review still flags **coverage_weakening**. This is a short-term safeguard until automated **P0/P1** invariant checks exist. |
| **Managed projects** | **No** platform coverage percentage. Project gates are whatever `validation_commands` declare (usually `make test`). |

Coverage percentage is not a substitute for behavioral P0/P1 tests. Prefer invariant tests; do not pad to hit 90%.

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

Provisioning creates a minimal repository:

- `README.md` — project title and description
- `Makefile` — `make test` with machine marker `vuzol-scaffold-gate: true`

The scaffold `make test` target is **green only while the project is empty or docs-only**.
It is not a permanent free pass.

Scaffold status is detected from the **scaffold test recipe** (the no-op
`scaffold: no project tests yet (ok)` echo) and/or the **exact dedicated marker
line** `# vuzol-scaffold-gate: true`. Removing only the marker while leaving the
no-op recipe does **not** unlock product code. A second trusted gate such as
`make lint` does **not** bypass scaffold status either.

### Scaffold → code transition

When coding validation sees **executable product files** in the result
(source, package manifests, `requirements*.txt`, etc.) and the worktree still
uses the scaffold `make test` implementation, validation **fails closed** with
category `validation_scaffold_gate` and a clear reason listing sample paths.

To proceed after adding product code:

1. Replace the scaffold `make test` recipe with a real project command
   (for example `/usr/bin/pytest -q`);
2. Remove the dedicated `# vuzol-scaffold-gate: true` marker line from the
   Makefile (prose comments that merely mention the marker string are ignored).

Docs-only changes (`README.md`, markdown/text under `docs/**`, structured
OpenAPI/YAML samples under `docs/**`) may still use the scaffold gate. Pure data
files (for example `data/*.csv`) do not by themselves force a real gate.

### Project test policy

| Project stage | Expectation |
|---|---|
| Fresh scaffold / notes / docs-only | scaffold `make test` green |
| First executable product code | real test/build/smoke gate required; scaffold alone fails validation |
| Library, CLI, or service with real behavior | focused tests for shipped behavior |
| Security- or money-sensitive logic | negative tests on dangerous boundaries |

Rules for agents and humans working **inside** managed projects:

1. Do **not** impose Vuzol’s platform 90% coverage floor or mypy-strict-on-tests.
2. Prefer one meaningful test over ten scaffolding asserts.
3. When you introduce executable product code, configure a real gate and drop the scaffold marker.
4. Do not fail validation solely because a brand-new empty/docs project has no tests yet.

### What Vuzol validates on managed projects

Validation runs system Git facts, scaffold-gate policy, mechanical review, and
**trusted fixed argv gates** from project config. Project gates are about the
project’s own quality command, not about cloning Vuzol’s internal bar.

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

- **Platform:** risk-tiered behavioral tests; temporary 90% coverage floor until P0/P1 automation.
- **Managed projects:** scaffold green for empty/docs-only; real gate required once product code appears; no platform coverage percentage.
- **Never** block a brand-new empty/docs project solely for “no tests yet.”
- **Do** block scaffold-only validation once executable product files land.
