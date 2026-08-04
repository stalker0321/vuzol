# Contributing

Thanks for taking the time to improve Vuzol. The project is under active development, so opening
an issue before a large change is recommended. Small fixes, tests, and documentation improvements
can go directly to a pull request.

## Development setup

```bash
git clone https://github.com/stalker0321/vuzol.git
cd vuzol
uv sync --frozen
cp .env.example .env
make db-up
make db-migrate
```

Use a focused branch and keep commits scoped to one concern. Never commit provider credentials,
Telegram tokens, production registry data, database dumps, or agent state directories.

## Quality gate

Before submitting a change, run:

```bash
make check
git diff --check
```

Add behavioral tests for changes to runtime behavior. Architecture changes require an accepted ADR
or an explicit update to an existing ADR.

## Documentation policy

Documentation committed to this repository is limited to:

- user-facing product documentation;
- installation and operator documentation;
- stable architecture and security invariants;
- accepted architecture decision records;
- the public changelog.

Internal implementation step instructions, agent prompts, temporary project state, planning
documents, execution handoffs, and run reports belong in the local external specification package
at `~/vuzol-local/specs` and must not be committed.

The application, tests, builds, packages, and installation process must never depend on that
external directory. Any contract required for runtime behavior belongs in code or stable public
documentation in this repository.

## Testing

Follow [docs/TESTING.md](docs/TESTING.md):

- write behavioral tests for P0/P1 invariants, not coverage padding;
- structure tests by responsibility (not line-count targets);
- managed projects use a scaffold gate (empty/docs-only) and do not inherit the platform bar;
- Vuzol currently keeps a temporary 90% coverage floor until P0/P1 automation replaces it.
