# Contributing

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

Before submitting a change, run:

```bash
make check
git diff --check
```

Architecture changes require an accepted ADR or an explicit update to an existing ADR.
