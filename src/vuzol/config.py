"""Typed application settings."""

from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Process settings loaded once at the composition boundary."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="VUZOL_",
        extra="forbid",
        frozen=True,
    )

    environment: Literal["development", "test", "production"] = "development"
    service_name: str = Field(default="vuzol", min_length=1)
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    host: str = "127.0.0.1"
    port: int = Field(default=8000, ge=1, le=65535)
    worker_poll_interval_seconds: float = Field(default=1.0, gt=0, le=60)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Load and cache the process settings."""

    return Settings()
