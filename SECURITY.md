# Security policy

Vuzol executes AI-generated changes against local Git repositories. Security issues in sandboxing,
credential isolation, authorization, approval binding, artifact handling, or workflow recovery are
treated as high priority.

## Supported versions

Vuzol is in active development. Security fixes are applied to the latest revision of `main`; older
revisions are not maintained as separate release lines yet.

## Reporting a vulnerability

Please do not open a public issue for a suspected vulnerability. Use GitHub's private vulnerability
reporting for this repository, if available, or contact the repository owner privately through the
address listed on their GitHub profile.

Include the affected revision, impact, reproduction steps, and any suggested mitigation. Do not
include real provider credentials, Telegram tokens, private repository contents, or production
database records in the report.

You can expect an acknowledgement within seven days. A fix and disclosure timeline will depend on
severity and whether coordinated disclosure is required.

## Deployment responsibility

The project is not yet distributed as a hardened turnkey appliance. Operators are responsible for
reviewing the documented architecture invariants, using dedicated runtime identities, keeping
credentials outside the repository, pinning production images, and restricting network and
filesystem access for their deployment.

## Built-in execution controls

Provider agents run as an unprivileged user in rootless Docker with a read-only root filesystem,
no Linux capabilities, `no-new-privileges`, seccomp, bounded resources, and either no network or a
destination allowlisted HTTPS proxy. Validation runs in a separate offline image.

Task diffs are scanned with Gitleaks in the trusted validation image before Vuzol creates a result
commit. CI scans repository history with pinned Gitleaks and also runs `detect-secrets`. Common API
key, bearer token, cloud key, GitHub token, and private-key formats are rejected in task diffs and
redacted before runtime output is persisted as an artifact.

The provider image places Aikido Safe-Chain shims before supported package managers and enforces a
48-hour minimum package age. Package registries remain unavailable unless an operator explicitly
adds them to both project and provider egress policy; installing Safe-Chain does not widen network
access by itself. Locked installs and vulnerability auditing remain separate required controls.

Provider CLI state for the selected profile is mounted into its sandbox because the upstream CLI
requires it. Other profiles and Vuzol service credentials are not mounted. Operators should treat
the selected profile's session state as potentially readable by that provider agent and prefer
dedicated, least-privilege accounts.
