# ADR-0008 — Treat Repository Content and Model Output as Untrusted

## Status

Accepted

## Decision

Treat repository content, attachments, retrieved web content, model output, and generated commands as untrusted input.

The default execution boundary is a constrained sandbox with explicit mounts, resource limits, and deny-by-default network access. Host protection must not depend on correctly understanding arbitrary shell syntax or on a model following prompt instructions.

Sensitive host operations use typed, narrowly implemented operations in a separate privileged executor. They require policy validation and an approval bound to the complete immutable action envelope.

## Reason

Source repositories and external content may contain malicious instructions, symlinks, scripts, dependency hooks, or data designed to cause credential disclosure and host modification. Language-model compliance and command string inspection are not security boundaries.

## Consequences

- egress is disabled or allowlisted by destination and purpose;
- credentials are exposed only to the process and operation that require them;
- path containment is enforced after symlink resolution;
- archives, attachments, hooks, build scripts, and dependency installation are risk inputs;
- shell command classification is defense in depth, not the primary isolation mechanism;
- security tests include prompt injection, path escape, metadata endpoint, and secret-exfiltration attempts.
