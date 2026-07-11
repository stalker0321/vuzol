"""Typed process settings and enforceable operational defaults."""

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ConcurrencyLimits(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    control: int = Field(default=2, ge=1, le=100)
    light: int = Field(default=2, ge=1, le=100)
    heavy: int = Field(default=1, ge=1, le=10)
    privileged: int = Field(default=1, ge=1, le=10)


class RetentionDefaults(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    completed_worktree_days: int = Field(default=3, ge=1)
    failed_worktree_days: int = Field(default=14, ge=1)
    artifact_days: int = Field(default=14, ge=1)
    voice_days: int = Field(default=3, ge=1)
    log_days: int = Field(default=14, ge=1)


class HardLimits(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    task_cost_units: float = Field(default=25, gt=0)
    task_input_tokens: int = Field(default=200_000, ge=1)
    task_output_tokens: int = Field(default=50_000, ge=1)
    provider_attempts: int = Field(default=3, ge=1, le=20)
    task_duration_seconds: int = Field(default=7_200, ge=1)
    artifact_bytes: int = Field(default=100_000_000, ge=1)
    input_bytes: int = Field(default=25_000_000, ge=1)


class DatabaseSettings(BaseModel):
    """Bounded connection and migration controls; the DSN remains a secret reference."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    pool_size: int = Field(default=5, ge=1, le=50)
    max_overflow: int = Field(default=5, ge=0, le=50)
    pool_timeout_seconds: int = Field(default=30, ge=1, le=300)
    statement_timeout_ms: int = Field(default=30_000, ge=100, le=3_600_000)
    lock_timeout_ms: int = Field(default=5_000, ge=100, le=300_000)
    migration_advisory_lock_key: int = 8_946_527_031


class TelegramSettings(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    max_text_chars: int = Field(default=16_000, ge=1, le=100_000)
    max_attachments: int = Field(default=10, ge=0, le=20)
    max_attachment_bytes: int = Field(default=25_000_000, ge=1)
    edit_min_interval_seconds: float = Field(default=2.0, ge=0.1, le=60)
    delivery_poll_interval_seconds: float = Field(default=1.0, ge=0.1, le=60)
    delivery_lease_seconds: int = Field(default=30, ge=5, le=600)
    delivery_max_attempts: int = Field(default=5, ge=1, le=20)
    delivery_retry_min_seconds: float = Field(default=1.0, ge=0.1, le=300)
    delivery_retry_max_seconds: float = Field(default=60.0, ge=0.1, le=3_600)
    allowed_media_types: tuple[str, ...] = (
        "audio/ogg",
        "audio/mpeg",
        "image/jpeg",
        "image/png",
        "text/plain",
        "application/pdf",
    )

    @model_validator(mode="after")
    def validate_retry_bounds(self) -> "TelegramSettings":
        if self.delivery_retry_min_seconds > self.delivery_retry_max_seconds:
            raise ValueError("delivery retry minimum must not exceed maximum")
        return self


class InterpretationSettings(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    profile_id: str | None = Field(default=None, pattern=r"^[a-z][a-z0-9_-]*$")
    fallback_profile_ids: tuple[str, ...] = ()
    transcription_profile_id: str | None = Field(default=None, pattern=r"^[a-z][a-z0-9_-]*$")
    poll_interval_seconds: float = Field(default=1.0, ge=0.1, le=60)
    lease_seconds: int = Field(default=300, ge=10, le=3_600)
    max_attempts: int = Field(default=3, ge=1, le=10)
    retry_min_seconds: float = Field(default=2.0, ge=0.1, le=300)
    retry_max_seconds: float = Field(default=120.0, ge=0.1, le=3_600)
    provider_timeout_seconds: float = Field(default=30, ge=1, le=300)
    transcription_timeout_seconds: float = Field(default=120, ge=1, le=600)
    language_hint: str | None = Field(default="ru", max_length=20)
    automatic_execution_enabled: bool = False
    evaluation_report_file: Path | None = None

    @model_validator(mode="after")
    def validate_retry_bounds(self) -> "InterpretationSettings":
        if self.retry_min_seconds > self.retry_max_seconds:
            raise ValueError("interpretation retry minimum must not exceed maximum")
        if self.automatic_execution_enabled and self.evaluation_report_file is None:
            raise ValueError("automatic interpretation execution requires an evaluation report")
        longest_call = max(self.provider_timeout_seconds, self.transcription_timeout_seconds)
        if self.lease_seconds <= longest_call + 5:
            raise ValueError("interpretation lease must exceed provider timeouts")
        return self


class Settings(BaseSettings):
    """Process settings loaded at the composition boundary."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="VUZOL_",
        env_nested_delimiter="__",
        extra="forbid",
        frozen=True,
    )

    environment: Literal["development", "test", "production"] = "development"
    service_name: str = Field(default="vuzol", min_length=1)
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    host: str = "127.0.0.1"
    port: int = Field(default=8000, ge=1, le=65535)
    worker_poll_interval_seconds: float = Field(default=1.0, gt=0, le=60)
    registry_file: Path | None = None
    database_dsn_reference: str | None = Field(default=None, pattern=r"^(env|file):.+$")
    telegram_bot_token_reference: str | None = Field(default=None, pattern=r"^(env|file):.+$")
    allowed_user_ids: tuple[int, ...] = ()
    allowed_chat_ids: tuple[int, ...] = ()
    repository_root: Path = Path("/srv/vuzol/repositories")
    artifact_root: Path = Path("/srv/vuzol/artifacts")
    secret_file_root: Path = Path("/run/secrets")
    concurrency: ConcurrencyLimits = ConcurrencyLimits()
    database: DatabaseSettings = DatabaseSettings()
    retention: RetentionDefaults = RetentionDefaults()
    telegram: TelegramSettings = TelegramSettings()
    interpretation: InterpretationSettings = InterpretationSettings()
    limits: HardLimits = HardLimits()
    redaction_patterns: tuple[str, ...] = ()

    @field_validator("repository_root", "artifact_root", "secret_file_root")
    @classmethod
    def require_absolute_root(cls, value: Path) -> Path:
        if not value.is_absolute():
            raise ValueError("configured roots must be absolute")
        return value


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Load and cache process settings."""

    return Settings()
