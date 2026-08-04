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
