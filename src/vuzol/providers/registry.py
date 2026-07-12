"""Provider adapter construction and consumer-scoped credential resolution."""

from collections.abc import Mapping

from vuzol.config.models import LaunchMode, ProviderProfileConfig
from vuzol.config.registries import ProfileRegistry
from vuzol.config.secrets import ScopedSecretResolver
from vuzol.providers.openai import OpenAICompatibleAdapter
from vuzol.providers.ports import ProviderAdapter


class AdapterRegistry:
    def __init__(
        self,
        profiles: ProfileRegistry,
        resolver: ScopedSecretResolver,
        *,
        adapters: Mapping[str, ProviderAdapter] | None = None,
    ) -> None:
        self._profiles = profiles
        self._resolver = resolver
        self._adapters = dict(adapters or {})

    def get(self, profile_id: str) -> ProviderAdapter:
        if adapter := self._adapters.get(profile_id):
            return adapter
        profile = self._profiles.get(profile_id)
        adapter = self._build(profile)
        self._adapters[profile_id] = adapter
        return adapter

    def _build(self, profile: ProviderProfileConfig) -> ProviderAdapter:
        if profile.provider == "openai-compatible" and profile.launch_mode is LaunchMode.API:
            if profile.credential_reference is None:
                raise ValueError(f"API profile has no credential reference: {profile.id}")
            return OpenAICompatibleAdapter(
                credential=self._resolver.get(profile.credential_reference, f"profile:{profile.id}")
            )
        if profile.launch_mode is LaunchMode.CLI:
            raise ValueError(
                f"CLI profile {profile.id} requires the Step 08 sandbox process transport"
            )
        raise ValueError(f"unsupported provider profile: {profile.id}")
