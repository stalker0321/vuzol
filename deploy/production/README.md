# Production deployment

Everything that runs in production is described here and nowhere else:

- `compose.yaml` — containers: postgres, telegram-ingress, telegram-delivery, interpreter,
  memory-writer, worker, mcp.
- `systemd/` — host services: executor (rootless Docker sandboxes), applier, static publisher,
  project setup, OpenCode usage timer.
- `deploy.sh` — installs one commit and switches production to it.
- `vuzol-compose` — `docker compose` with the production files and env (installed to
  `/usr/local/bin`).

## Deploy and rollback

```console
sudo /srv/vuzol/repo/deploy/production/deploy.sh <commit>
```

The script builds `/opt/vuzol/releases/<sha12>` (code from `git archive`, its own frozen venv),
builds image `vuzol:<sha12>`, runs migrations, points `/opt/vuzol/current` and
`/opt/vuzol/current.env` at the release, installs the units, restarts host services and
recreates containers, then checks that everything runs. Rollback: run it with the previous
commit. Migrations are not downgraded, so they must stay compatible with the previous release.

## Rules

- Deploy only committed code. No `PYTHONPATH` overrides, no image patching.
- No systemd drop-ins and no extra compose overlays: change these files and redeploy.
  `deploy.sh` refuses to run if drop-ins exist.
- Host units read three files, later ones overriding earlier ones:
  `/etc/vuzol/vuzol.env` (shared settings), `/etc/vuzol/<service>.env` (`executor`, `applier`,
  `static-publisher`) and `/etc/vuzol/secrets.env` (`0600`, secrets only). Put a setting in
  `vuzol.env` unless one service needs a different value. Containers take theirs from
  `compose.yaml` and `/srv/vuzol/.env`. None of these files are committed: the repository is public.
- The database password is `VUZOL_POSTGRES_PASSWORD` and `VUZOL_DATABASE_DSN` in
  `/srv/vuzol/.env`; host units read the DSN from `secrets.env`, backups from
  `/etc/vuzol/backup/restore_dsn`. Compose refuses to start without them. To rotate, run
  `ALTER ROLE vuzol PASSWORD ...` in the container, update those three files and redeploy.
  Test databases (`vuzol_test*`) belong to a separate non-superuser role `vuzol_test`.
- Account profiles live in `/var/lib/vuzol/registry/account-profiles.toml`
  (`vodkolyan:vuzol-executor 0640`). Edit it in place and keep that group: host services
  cannot read it otherwise and crash-loop. Containers bind-mount it by inode, so after an
  editor replaces the file they keep the old copy until recreated. Validate before restarting:
  `sudo vuzol-compose run --rm --no-deps -T worker python -c "from vuzol.config import
  get_runtime_configuration as g; g(validate_profile_credentials=False)"`, then recreate the
  containers and restart the host services.
- Projects created from the assistant topic: `vuzol-project-setup` (root) appends the
  project and its topic to the account-profiles overlay, copied from
  `VUZOL_PROJECT_SETUP__TEMPLATE_PROJECT_ID`. It keeps the file's owner and mode. It creates
  the repository as the repository root's owner, maps the topic and copies the template's
  model choices, then restarts the services that cache the registry once work is idle.
  `VUZOL_PROJECT_SETUP__ENABLED=true` goes in `/etc/vuzol/vuzol.env` (the unit) and as
  `VUZOL_PROJECT_SETUP_ENABLED=true` in `/srv/vuzol/.env` (the assistant's button). Without
  both, the unit idles and the assistant does not offer to create projects.
- A Codex account for Vuzol gets its own state directory
  `/var/lib/vuzol-provider-state/<id>` (owner `110000`, mode `0710`, ACL
  `u:vuzol-executor:--x`, `auth.json` `110000:vuzol-executor 0640`) and a unique
  `runtime_identity`; a home directory such as `~/.codex-c` is not readable by the executor.

## OpenCode usage export

`vuzol-opencode-usage.timer` runs about once a minute as the owner's Unix account, which owns
the OpenCode OAuth store in `/var/lib/vuzol/opencode-shared/<account>` (`xdg_root` in
`/etc/vuzol/opencode-usage-c.json`; the owner's old profile path is a symlink to it). It writes a token-free snapshot to
`/var/lib/vuzol/usage-export/opencode-c/usage.json` (directory `2750`, file `0640`, group
`vuzol-executor`); the executor reads only that file and rejects it when stale (over 15 minutes),
malformed or bound to another account. The installed OpenCode CLI stays the only refresh owner:
`/usr/local/bin/opencode` is the dispatcher from `deploy/opencode-dispatch.py`, which serializes
interactive use and the exporter with one `flock`; the exporter checks its SHA-256
(`owner_launcher_sha256`), so update that after reinstalling it. Upgrading OpenCode requires rechecking the
dispatcher and the pinned store schema; until then the exporter stops with an explicit error.

## Backups and retention

`vuzol-backup.timer` runs `vuzol-backup-run.sh` every night at about 03:30. It takes an encrypted
`pg_dump` of the database (`vuzol-backup capture`), checks that the new package decrypts
(`restore --verify-crypto`), and copies every local package to the hosts in `BACKUP_TARGETS`
(`/etc/vuzol/backup.env`). Seven packages stay in `/srv/vuzol/backups/daily`. The copies land in
`~/vuzol-backups/daily` on `vuzol-main` and the NL server. The ssh key in
`/etc/vuzol/backup` is allowed only `rrsync -wo -no-del` there: it can add packages, not read or
delete them, so old copies are not pruned remotely. Only the database is backed up, not
repositories, artifacts or `/etc/vuzol`.

The key that decrypts the packages is `/etc/vuzol/backup/kek`, with copies in
`~/vuzol-backups/kek/kek` on both target hosts. Without it the packages cannot be restored. To
restore, put a package under the staging root and run, as root with the backup environment:
`vuzol-backup restore --run-id <id> --verify-crypto`, then into an empty `vuzol_restore` database
`VUZOL_BACKUP__RESTORE_CLI_PERMITTED=true vuzol-backup restore --run-id <id> --apply
--i-understand-partial-postgres-only`. The restore DSN is `/etc/vuzol/backup/restore_dsn`.

`vuzol-retention.timer` runs `vuzol-retention --apply` every night at about 04:30 as
`vuzol-executor`. It removes worktrees of finished tasks after their retention period, deletes
expired artifacts, and moves files with no artifact row into `/srv/vuzol/artifacts/.quarantine`
(never emptied automatically). Run `vuzol-retention --json` without `--apply` to see what it would
do.

