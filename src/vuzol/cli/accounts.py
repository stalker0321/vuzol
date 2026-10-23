"""Read-only inventory and explicit single-account usage. No auth refresh or models."""

from __future__ import annotations

import argparse
import json
import stat
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from vuzol.config.loader import ConfigurationLoadError, load_document, merge_documents
from vuzol.config.models import LaunchMode, ProviderProfileConfig
from vuzol.config.registries import ProfileRegistry
from vuzol.config.revision import content_revision
from vuzol.providers.account_auth import AccountAuthError, read_go_key
from vuzol.providers.accounts import effective_account_id
from vuzol.providers.opencode_oauth import read_go_oauth


def inventory(
    profiles: tuple[ProviderProfileConfig, ...], *, check_auth: bool = False
) -> list[dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for profile in sorted(profiles, key=lambda item: item.id):
        identity = (
            effective_account_id(profile)
            if profile.account_id or profile.launch_mode is LaunchMode.CLI
            else "unbound-api:" + profile.id
        )
        descriptor = profile.account
        row = result.setdefault(
            identity,
            {
                "account_id": identity,
                "descriptor_id": descriptor.id if descriptor else None,
                "normalization": "explicit" if descriptor else "legacy-inline",
                "service": descriptor.service if descriptor else "not-declared",
                "auth_method": descriptor.auth_method if descriptor else "not-declared",
                "auth_owner": descriptor.auth_owner if descriptor else "not-declared",
                "lifecycle": descriptor.lifecycle if descriptor else "not-declared",
                "concurrency_limit": profile.concurrency_limit,
                "control_reserved_slots": profile.control_reserved_slots,
                "quota_scope_id": descriptor.quota_scope_id if descriptor else None,
                "quota_health": "not-probed",
                "subscription": {"status": "unknown", "reason": "not-read"},
                "occupied_slots": "not-read; database audit required",
                "profiles": [],
            },
        )
        row["profiles"].append(
            {
                "id": profile.id,
                "model": profile.model,
                "runtime": profile.provider,
                "roles": sorted(profile.roles),
                "enabled": profile.enabled,
                "limits_visible": profile.subscription_limits_visible,
                "fallbacks": list(profile.fallback_profile_ids),
                "revision": content_revision(profile),
            }
        )
        if check_auth and "auth_health" not in row:
            row["auth_health"] = "not-verified"
            if profile.state_directory is not None:
                row["state_directory"] = str(profile.state_directory)
                try:
                    metadata = profile.state_directory.stat()
                    row["state_permissions"] = {
                        "uid": metadata.st_uid,
                        "gid": metadata.st_gid,
                        "mode": stat.filemode(metadata.st_mode),
                    }
                except OSError:
                    row["auth_health"] = "state_unreadable"
            if descriptor and descriptor.quota_collector == "opencode_go":
                oauth = descriptor.quota_oauth
                if oauth:
                    row["usage_auth"] = {
                        "method": "oauth",
                        "store": str(oauth.store_path),
                        "account_id": oauth.account_id,
                        "org_id": oauth.org_id,
                        "refresh_owner": oauth.refresh_owner,
                    }
                try:
                    if oauth:
                        credential = read_go_oauth(oauth, now=datetime.now(UTC))
                        row["usage_auth"]["expires_at"] = credential.expires_at.isoformat()
                        metadata = oauth.store_path.stat()
                        row["usage_auth"]["permissions"] = {
                            "uid": metadata.st_uid,
                            "gid": metadata.st_gid,
                            "mode": stat.filemode(metadata.st_mode),
                        }
                    else:
                        read_go_key(descriptor)
                    row["auth_health"] = (
                        "locally-readable; server identity/entitlement not verified"
                    )
                except AccountAuthError as error:
                    row["auth_health"] = str(error)
                if oauth:
                    row["usage_auth"]["health"] = row["auth_health"]
                    row["auth_health"] = "inference auth not checked; usage OAuth is separate"
    return [result[key] for key in sorted(result)]


def run(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("list", "check", "status", "usage"))
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--overlay", type=Path)
    parser.add_argument("--account", help="Exact descriptor ID; required for usage")
    args = parser.parse_args(argv)
    try:
        document = load_document(args.registry)
        if args.overlay:
            document = merge_documents(document, load_document(args.overlay))
        if len({a.id for a in document.accounts}) != len(document.accounts):
            raise ValueError("duplicate_account")
        profiles = ProfileRegistry(document.profiles).items()
        if args.operation == "usage":
            from vuzol.providers.subscription_limits import collect_profile_limits

            selected = sorted(
                (
                    p
                    for p in profiles
                    if p.enabled
                    and p.account is not None
                    and p.account.id == args.account
                    and p.account.quota_collector == "opencode_go"
                ),
                key=lambda p: p.id,
            )
            if not selected:
                print(json.dumps({"error": "explicit_enabled_go_account_required"}))
                return 1
            snap = collect_profile_limits(selected[0])
            print(
                json.dumps(
                    {
                        "account_id": args.account,
                        "ok": snap.ok,
                        "detail": snap.detail,
                        "source": snap.source,
                        "scope_id": snap.quota_scope_id,
                        "attempted_at": snap.attempted_at.isoformat()
                        if snap.attempted_at
                        else None,
                        "observed_at": snap.observed_at.isoformat() if snap.observed_at else None,
                        "windows": [w.model_dump(mode="json") for w in snap.windows],
                        "plan": snap.plan.model_dump(mode="json") if snap.plan else None,
                        "mode": "read-only; selected-account usage only; no login or auth refresh",
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return 0 if snap.ok else 2
        print(
            json.dumps(
                {
                    "accounts": inventory(profiles, check_auth=args.operation != "list"),
                    "mode": "read-only",
                    "network_requests": 0,
                    "limitations": [
                        "No live quota/slots/paused-run query in this command.",
                        "Import/reauth/retire require a reviewed migration; not implemented here.",
                    ],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0
    except (ConfigurationLoadError, ValueError, OSError):
        # Pydantic/config exceptions can include operator-supplied input.
        print(json.dumps({"error": "account_configuration_invalid"}))
        return 1


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
