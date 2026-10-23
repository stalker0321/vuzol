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
- Secrets stay in `/srv/vuzol/.env` and `/etc/vuzol/*.env`; they are referenced, never committed.
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

## Not cleaned up yet

- Host services still read the historical env files listed in their units
  (`executor-t091.env`, `t1xx-*.env`); merge them into one file per service.
- Postgres uses the default password and services run with `VUZOL_ENVIRONMENT=development`.
