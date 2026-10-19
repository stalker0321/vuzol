# Applier repository access

The trusted applier needs write access to the **managed repository root**, not
just one historical project. The registry confines projects to that root;
result application additionally requires an owner-approved, identity/hash-bound
envelope, a current lease, unchanged target ref and the existing promotion gate.
Neither model sandboxes nor the interpreter gain repository write access.

Keep systemd ProtectSystem=strict and the worktree/configuration paths read-only.
Do not mount the managed repository root read-only while allowing write only to
`afterword`: new registered projects would fail after approval when Git writes
FETCH_HEAD. The checked-in unit allows the managed repository root and the
artifact/toolchain/download directories needed by the existing trusted handlers.
Clear stale additive ReadOnlyPaths/ReadWritePaths entries in a deployment drop-in
before setting the intended lists; adding an exception without removing a parent
read-only mount is not a portable repair.

## Admission checks

- At startup, in the actual applier UID/mount namespace, require a writable
  managed root when registered projects support APPLY. Fail before effect
  reconciliation, worker claims or the ready event if that contract is broken.
- During ensure_capabilities, before returning ready or installing anything,
  check an APPLY-enabled project's repository and real Git metadata using the
  same service identity. This admits new projects before expensive model work;
  projects without APPLY are not unnecessarily required to be writable.
- Resolve both git-dir and common-dir with read-only typed Git operations so
  linked worktrees cannot hide read-only metadata elsewhere. Check mount flags,
  effective UID/ACL permissions, relevant directories and existing mutable files.
  Do not modify refs, write a test commit or request installation approval.
- Recheck after result approval and **before** persisting a Git effect intent,
  because permissions may have changed since early admission. Failure preserves
  the approval, result and Git refs. Admission is not a substitute for existing
  CAS/identity/clean-tree/effect-reconciliation guards or a guarantee against
  later filesystem races, locks or disk exhaustion.

Never grant general host write access, recursively chmod repositories, manually
advance a target branch, recreate the result approval or mark a failed step
completed. After deployment, reconcile any prior unsettled effect from real Git
state and resume through the ordinary owner/version-bound Retry operation.

A reconciled, proven-not-applied Git intent may be retried under the same exact
approval/envelope/context and a fresh lease. It must be durably marked dispatched
again before Git, with the former denial retained in events. Otherwise a retry
could apply successfully while leaving a failed receipt, or become invisible to
the reconciler if it crashes after CAS. Uncertain effects remain non-relaunchable;
identity/envelope drift cannot rearm a denied effect.
