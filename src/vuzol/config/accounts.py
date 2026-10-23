"""Single raw-config normalization boundary; no filesystem/auth discovery."""

from vuzol.config.models import AccountConfig

_BINDING_FIELDS = (
    "provider",
    "launch_mode",
    "state_directory",
    "runtime_identity",
    "credential_reference",
    "credential_required",
    "api_base_url",
    "runtime_network",
    "concurrency_limit",
    "control_reserved_slots",
    "subscription_limits_visible",
)


def normalize_accounts(raw: object) -> object:
    """Compile explicit account_ref into the existing profile contract.

    Run AFTER profile inheritance so inherited conflicts are also rejected.
    Legacy account_id alone is intentionally not an implicit descriptor lookup.
    """
    if not isinstance(raw, dict):
        return raw
    accounts: dict[str, AccountConfig] = {}
    scopes: set[tuple[str, str]] = set()
    for entry in raw.get("accounts", []):
        account = AccountConfig.model_validate(entry)
        if account.id in accounts:
            raise ValueError("duplicate account descriptor")
        if account.quota_scope_id is not None:
            scope = (account.service, account.quota_scope_id)
            if scope in scopes:
                raise ValueError(
                    "shared quota pools require server identity evidence; not supported"
                )
            scopes.add(scope)
        accounts[account.id] = account
    profiles = []
    for original in raw.get("profiles", []):
        profile = dict(original)
        if "account" in profile:
            raise ValueError("profile.account is compiled; use account_ref")
        reference = profile.pop("account_ref", None)
        if reference is not None:
            if reference not in accounts:
                raise ValueError("unknown account_ref")
            account = accounts[reference]
            binding = account.model_dump(mode="json")
            binding = {key: binding[key] for key in _BINDING_FIELDS}
            binding["account_id"] = account.id
            for key, value in binding.items():
                if key in profile and profile[key] != value:
                    raise ValueError(f"profile conflicts with account field: {key}")
                profile[key] = value
            if account.lifecycle != "enabled":
                profile["enabled"] = False
            profile["account"] = account
        profiles.append(profile)
    return {**raw, "profiles": profiles}
