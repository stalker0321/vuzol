# Production deployment

Production releases are installed by one serialized, fail-closed command. Do not manually update
only the systemd checkout or only the interpreter image: that creates a split-brain release.

Run the command from a clean, reviewed checkout and pass the full commit SHA:

```console
sudo /opt/vuzol/.venv/bin/vuzol-production-deploy \
  --source /home/vodkolyan/projects/vuzol \
  --sha 0123456789abcdef0123456789abcdef01234567
```

The deployer:

1. takes a non-blocking host lock and rejects concurrent deploys;
2. rejects dirty operator and production checkouts or an unavailable commit;
3. checks out the exact SHA and performs a frozen runtime dependency sync;
4. applies forward-compatible migrations;
5. labels and rebuilds the interpreter image with the same Git SHA;
6. recreates the interpreter and restarts all long-running systemd services;
7. verifies the production checkout, container label, container state, and every service.

If installation or attestation fails after checkout, the deployer restores the previous code and
interpreter image, restarts the services, and attests the restored SHA before returning failure.
Database migrations are intentionally not downgraded during rollback. Reviewed migrations must
therefore remain backward compatible with the immediately preceding release; migration preflight
and the normal release gate enforce that contract.

Successful output is one JSON object suitable for an operator log. Environment-file values are
never printed. A failed deploy returns non-zero and a bounded error containing command diagnostics,
but not command environments.

## Independent verification

After a release, verify the two SHA authorities directly:

```console
sudo git -C /opt/vuzol rev-parse HEAD
sudo docker inspect vuzol-interpreter-1 \
  --format '{{index .Config.Labels "dev.hryshyn.vuzol.git-sha"}}'
```

The values must be identical. `systemctl is-active` must return `active` for every Vuzol service,
and the interpreter container state must be `running`.
