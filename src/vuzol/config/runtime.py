"""Cached process-level configuration composition."""

from functools import lru_cache

from pydantic import BaseModel, ConfigDict

from vuzol.config.loader import build_bundle, load_document, merge_documents
from vuzol.config.models import RegistryDocument
from vuzol.config.registries import ConfigurationBundle
from vuzol.config.settings import Settings, get_settings


class RuntimeConfiguration(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    settings: Settings
    registries: ConfigurationBundle


@lru_cache(maxsize=2)
def get_runtime_configuration(*, validate_profile_credentials: bool = True) -> RuntimeConfiguration:
    """Load and validate all startup configuration once per process."""

    settings = get_settings()
    document = (
        load_document(settings.registry_file)
        if settings.registry_file is not None
        else RegistryDocument()
    )
    if settings.registry_overlay_file is not None and settings.registry_overlay_file.exists():
        document = merge_documents(document, load_document(settings.registry_overlay_file))
    return RuntimeConfiguration(
        settings=settings,
        registries=build_bundle(
            document,
            settings,
            validate_profile_credentials=validate_profile_credentials,
        ),
    )
