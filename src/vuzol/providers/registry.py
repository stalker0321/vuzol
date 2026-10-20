"""Provider adapter construction and consumer-scoped credential resolution."""

from collections.abc import Mapping

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from vuzol.config.models import LaunchMode, ProviderProfileConfig
from vuzol.config.registries import ProfileRegistry
from vuzol.config.secrets import ScopedSecretResolver
from vuzol.providers.accounts import AccountBoundAdapter
from vuzol.providers.openai import OpenAICompatibleAdapter
from vuzol.providers.ports import ProviderAdapter


class AdapterRegistry:
    def __init__(
        self,
        profiles: ProfileRegistry,
        resolver: ScopedSecretResolver,
        *,
        adapters: Mapping[str, ProviderAdapter] | None = None,
        session_factory: async_sessionmaker[AsyncSession] | None = None,
    ) -> None:
        self._profiles = profiles
        self._resolver = resolver
        self._session_factory = session_factory
        if session_factory is None and any(
            profile.account_id is not None for profile in profiles.items()
        ):
            raise ValueError("explicit account_id profiles require a session_factory")
        self._provided = dict(adapters or {})
        self._adapters: dict[str, ProviderAdapter] = {}

    def get(self, profile_id: str) -> ProviderAdapter:
        if adapter := self._adapters.get(profile_id):
            return adapter
        profile = self._profiles.get(profile_id)
        adapter = self._provided.get(profile_id) or self._build(profile)
        if profile.account_id is not None or (
            profile.launch_mode is LaunchMode.CLI and self._session_factory is not None
        ):
            assert self._session_factory is not None
            adapter = AccountBoundAdapter(
                adapter,
                profile,
                self._session_factory,
                account_profile_ids=tuple(
                    member.id
                    for member in self._profiles.items()
                    if member.id == profile.id
                    or (profile.account_id is not None and member.account_id == profile.account_id)
                ),
            )
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
