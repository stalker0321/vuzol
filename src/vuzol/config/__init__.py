"""Typed configuration, registry, and secret-resolution boundary."""

from vuzol.config.loader import ConfigurationLoadError, build_bundle, load_document
from vuzol.config.models import (
    Capability,
    CommandDefinition,
    DeliveryMode,
    EgressDestination,
    GitDeliveryPolicy,
    LaunchMode,
    NetworkPolicy,
    ProjectConfig,
    ProviderProfileConfig,
    RegistryDocument,
    TopicConfig,
    TopicKind,
)
from vuzol.config.registries import (
    ConfigurationBundle,
    ProfileRegistry,
    ProjectRegistry,
    RegistryError,
    TopicRegistry,
)
from vuzol.config.revision import RunConfigurationSnapshot, SnapshotCompatibility
from vuzol.config.runtime import RuntimeConfiguration, get_runtime_configuration
from vuzol.config.secrets import ScopedSecretResolver, SecretResolutionError
from vuzol.config.settings import (
    ConcurrencyLimits,
    DatabaseSettings,
    HardLimits,
    InterpretationSettings,
    RetentionDefaults,
    Settings,
    TelegramSettings,
    get_settings,
)

__all__ = [
    "Capability",
    "CommandDefinition",
    "ConcurrencyLimits",
    "ConfigurationBundle",
    "ConfigurationLoadError",
    "DatabaseSettings",
    "DeliveryMode",
    "EgressDestination",
    "GitDeliveryPolicy",
    "HardLimits",
    "InterpretationSettings",
    "LaunchMode",
    "NetworkPolicy",
    "ProfileRegistry",
    "ProjectConfig",
    "ProjectRegistry",
    "ProviderProfileConfig",
    "RegistryDocument",
    "RegistryError",
    "RetentionDefaults",
    "RunConfigurationSnapshot",
    "RuntimeConfiguration",
    "ScopedSecretResolver",
    "SecretResolutionError",
    "Settings",
    "SnapshotCompatibility",
    "TelegramSettings",
    "TopicConfig",
    "TopicKind",
    "TopicRegistry",
    "build_bundle",
    "get_runtime_configuration",
    "get_settings",
    "load_document",
]
