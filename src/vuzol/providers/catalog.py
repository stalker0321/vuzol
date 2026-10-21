"""Installed transport adapters, independent of project roles and model names."""

from collections.abc import Callable

from vuzol.config.models import Capability, LaunchMode, ProviderProfileConfig, ProviderRole
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

CONVERSATION_TASK_TYPE = "discussion_agent_internal"


def supported_profile(profile: ProviderProfileConfig) -> bool:
    return (profile.launch_mode is LaunchMode.CLI and profile.provider in CLI_ADAPTERS) or (
        profile.launch_mode is LaunchMode.API and profile.provider == "openai-compatible"
    )


def conversation_compatible(profile: ProviderProfileConfig, role: ProviderRole) -> bool:
    """Read-only conversation is a role/transport capability, not an execution task type."""
    return (
        profile.enabled
        and supported_profile(profile)
        and profile.launch_mode is LaunchMode.CLI
        and profile.sandbox_required
        and role in {ProviderRole.EXECUTOR, ProviderRole.LEAD}
        and role in profile.roles
        and Capability.REPOSITORY_READ in profile.capabilities
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
