"""Installed transport adapters, independent of project roles and model names."""

from collections.abc import Callable

from vuzol.config.models import LaunchMode, ProviderProfileConfig
from vuzol.providers.codex import CodexCliAdapter
from vuzol.providers.grok import GrokCliAdapter
from vuzol.providers.kimi import KimiCliAdapter
from vuzol.providers.pi import PiCliAdapter
from vuzol.providers.ports import CodexProcessTransport, ProviderAdapter

CLI_ADAPTERS: dict[str, Callable[[CodexProcessTransport], ProviderAdapter]] = {
    "codex": CodexCliAdapter,
    "grok": GrokCliAdapter,
    "kimi": KimiCliAdapter,
    "pi": PiCliAdapter,
}


def supported_profile(profile: ProviderProfileConfig) -> bool:
    return (profile.launch_mode is LaunchMode.CLI and profile.provider in CLI_ADAPTERS) or (
        profile.launch_mode is LaunchMode.API and profile.provider == "openai-compatible"
    )


def cli_adapter(
    profile: ProviderProfileConfig, transport: CodexProcessTransport
) -> ProviderAdapter:
    if profile.launch_mode is not LaunchMode.CLI or profile.provider not in CLI_ADAPTERS:
        raise ValueError(f"unsupported CLI adapter: {profile.id}")
    return CLI_ADAPTERS[profile.provider](transport)


def credential_access_policy(
    profiles: tuple[ProviderProfileConfig, ...],
) -> dict[str, frozenset[str]]:
    """One account credential can serve several explicitly configured profiles."""
    policy: dict[str, set[str]] = {}
    for profile in profiles:
        if profile.credential_reference is not None:
            policy.setdefault(profile.credential_reference, set()).add(f"profile:{profile.id}")
    return {reference: frozenset(consumers) for reference, consumers in policy.items()}
