"""Cached process-level configuration composition."""

from functools import lru_cache

from pydantic import BaseModel, ConfigDict

from vuzol.config.loader import build_bundle, load_document
from vuzol.config.models import RegistryDocument
from vuzol.config.registries import ConfigurationBundle
from vuzol.config.settings import Settings, get_settings


class RuntimeConfiguration(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    settings: Settings
    registries: ConfigurationBundle


@lru_cache(maxsize=1)
def get_runtime_configuration() -> RuntimeConfiguration:
    """Load and validate all startup configuration once per process."""

    settings = get_settings()
    document = (
        load_document(settings.registry_file)
        if settings.registry_file is not None
        else RegistryDocument()
    )
    return RuntimeConfiguration(settings=settings, registries=build_bundle(document, settings))
