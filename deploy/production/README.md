# Production deployment

Everything that runs in production is described here and nowhere else:

- `compose.yaml` — containers: postgres, telegram-ingress, telegram-delivery, interpreter,
  memory-writer, worker, mcp.
- `systemd/` — host services: executor (rootless Docker sandboxes), applier, static publisher,
  OpenCode usage timer.
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
- Account profiles live in `/var/lib/vuzol/registry/account-profiles.toml`
  (`vodkolyan:vuzol-executor 0640`). Edit it in place and keep that group: host services
  cannot read it otherwise and crash-loop. Containers bind-mount it by inode, so after an
  editor replaces the file they keep the old copy until recreated. Validate before restarting:
  `sudo vuzol-compose run --rm --no-deps -T worker python -c "from vuzol.config import
  get_runtime_configuration as g; g(validate_profile_credentials=False)"`, then recreate the
  containers and restart the host services.
- A Codex account for Vuzol gets its own state directory
  `/var/lib/vuzol-provider-state/<id>` (owner `110000`, mode `0710`, ACL
  `u:vuzol-executor:--x`, `auth.json` `110000:vuzol-executor 0640`) and a unique
  `runtime_identity`; a home directory such as `~/.codex-c` is not readable by the executor.

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

## Not cleaned up yet

- Postgres uses the default password and services run with `VUZOL_ENVIRONMENT=development`.
