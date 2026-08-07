# One-time secret ingress

Vuzol can accept allowlisted deployment secrets without putting their values into Telegram,
PostgreSQL, the transactional outbox, or tracing. The Telegram command contains only the secret
identifier:

```text
/secret TOKENROUTER_API_KEY
```

The command is accepted only in the configured `system` topic. Vuzol deletes the command message,
sends a short-lived one-time HTTPS link, and provides an **Отменить** callback. PostgreSQL stores
only the SHA-256 hash of the random URL token and lifecycle metadata. The submitted value is
atomically installed as a mode `0600` file below `secret_ingress.storage_root`.

## Portable hosting

`secret_ingress.public_base_url` is installation-owned; no Vuzol domain is hard-coded. Put any TLS
reverse proxy in front of the loopback-only `vuzol-secret-ingress.service` and forward only
`/secret/` plus optional health endpoints. For example, an installation may use
`https://vuzol.example.com`, while the local application listens on `127.0.0.1:8088`.

Create a dedicated `vuzol-secret-ingress` system user and a root-owned deployment directory whose
write access is limited to that user. Consumers should receive read access only to the individual
files they need. Do not share the Telegram service environment with this process.

Copy `deploy/systemd/vuzol-secret-ingress.env.example`, set the public URL and explicit
`allowed_names`, install the unit, run database migrations, then enable it. DNS and a valid TLS
certificate must exist before exposing the route. Secret values are never accepted as command
arguments or query parameters.
