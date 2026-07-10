# Changelog

This file records completed implementation changes, not plans or speculative ideas.

## Unreleased

- added ADR-0007 for transactional inbox/outbox delivery and fenced leases;
- added ADR-0008 for untrusted repository, attachment, retrieved-content, and model-output boundaries;
- expanded MVP persistence, Telegram delivery, workflow recovery, sandbox, approvals, budgets, Git result lifecycle, backup, and acceptance contracts;
- added explicit ADR references to implementation steps and supply-chain checks to repository foundation.
- clarified one-time documentation bootstrap into the project repository and SSH remote setup without copying credentials.
- completed Step 01 repository foundation with Python 3.12, `uv`, typed settings, app and worker entry points, JSON logging, health endpoints, tests, security gates, and non-root Docker packaging.
- completed Step 02 configuration and registries with typed hard limits, TOML project/profile/topic models, immutable registries, scoped secret resolution, stable revisions, reload compatibility checks, and startup validation.

- Initial architecture and MVP implementation documentation created.
