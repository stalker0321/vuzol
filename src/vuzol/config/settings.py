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
    step_cost_units: float = Field(default=10, gt=0)
    step_input_tokens: int = Field(default=100_000, ge=1)
    step_output_tokens: int = Field(default=25_000, ge=1)
    provider_call_input_tokens: int = Field(default=100_000, ge=1)
    provider_call_output_tokens: int = Field(default=25_000, ge=1)
    planner_output_tokens: int = Field(default=1_000, ge=1)
    provider_attempts: int = Field(default=3, ge=1, le=20)
    fallback_depth: int = Field(default=2, ge=0, le=10)
    daily_cost_units: float = Field(default=100, gt=0)
    daily_quota_units: float = Field(default=100, gt=0)
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


class WorkflowSettings(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    poll_interval_seconds: float = Field(default=1.0, ge=0.1, le=60)
    lease_seconds: int = Field(default=60, ge=15, le=3_600)
    heartbeat_seconds: int = Field(default=15, ge=1, le=1_200)
    shutdown_deadline_seconds: int = Field(default=30, ge=1, le=600)
    retry_min_seconds: float = Field(default=2.0, ge=0.1, le=300)
    retry_max_seconds: float = Field(default=120.0, ge=0.1, le=3_600)
    recovery_interval_seconds: float = Field(default=15.0, ge=1, le=600)
    recovery_batch_size: int = Field(default=100, ge=1, le=1_000)
    claim_candidate_limit: int = Field(default=20, ge=1, le=100)

    @model_validator(mode="after")
    def validate_timing(self) -> "WorkflowSettings":
        if self.heartbeat_seconds * 3 >= self.lease_seconds:
            raise ValueError("workflow heartbeat must be less than one third of lease")
        if self.retry_min_seconds > self.retry_max_seconds:
            raise ValueError("workflow retry minimum must not exceed maximum")
        return self


class ExecutionSettings(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    enabled: bool = False
    rootless_docker_socket: Path = Path("/run/user/1000/docker.sock")
    require_preflight: bool = True
    cleanup_interval_seconds: int = Field(default=300, ge=10, le=86_400)
    recovery_batch_size: int = Field(default=50, ge=1, le=1000)
    proxy_image: str | None = Field(
        default=None,
        pattern=r"^(?:[^\s@]+@)?sha256:[0-9a-f]{64}$",
    )
    proxy_runtime_root: Path = Path("/run/vuzol/proxy")
    sandbox_seccomp_profile: Path | None = None
    sandbox_seccomp_profile_sha256: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )

    @field_validator("rootless_docker_socket", "proxy_runtime_root", "sandbox_seccomp_profile")
    @classmethod
    def require_absolute_execution_path(cls, value: Path | None) -> Path | None:
        if value is None:
            return value
        if not value.is_absolute():
            raise ValueError("execution paths must be absolute")
        return value

    @model_validator(mode="after")
    def validate_seccomp_configuration(self) -> "ExecutionSettings":
        configured = self.sandbox_seccomp_profile is not None
        if configured != (self.sandbox_seccomp_profile_sha256 is not None):
            raise ValueError("sandbox seccomp path and digest must be configured together")
        if self.enabled and not configured:
            raise ValueError("enabled execution requires a pinned sandbox seccomp profile")
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
    registry_overlay_file: Path | None = None
    project_template_id: str = Field(default="vuzol", pattern=r"^[a-z][a-z0-9_-]*$")
    database_dsn_reference: str | None = Field(default=None, pattern=r"^(env|file):.+$")
    telegram_bot_token_reference: str | None = Field(default=None, pattern=r"^(env|file):.+$")
    allowed_user_ids: tuple[int, ...] = ()
    allowed_chat_ids: tuple[int, ...] = ()
    repository_root: Path = Path("/srv/vuzol/repositories")
    worktree_root: Path = Path("/srv/vuzol/worktrees")
    artifact_root: Path = Path("/srv/vuzol/artifacts")
    secret_file_root: Path = Path("/run/secrets")
    concurrency: ConcurrencyLimits = ConcurrencyLimits()
    database: DatabaseSettings = DatabaseSettings()
    retention: RetentionDefaults = RetentionDefaults()
    telegram: TelegramSettings = TelegramSettings()
    interpretation: InterpretationSettings = InterpretationSettings()
    workflow: WorkflowSettings = WorkflowSettings()
    execution: ExecutionSettings = ExecutionSettings()
    limits: HardLimits = HardLimits()
    redaction_patterns: tuple[str, ...] = ()

    @field_validator(
        "repository_root",
        "worktree_root",
        "artifact_root",
        "secret_file_root",
        "registry_overlay_file",
    )
    @classmethod
    def require_absolute_root(cls, value: Path | None) -> Path | None:
        if value is None:
            return None
        if not value.is_absolute():
            raise ValueError("configured roots must be absolute")
        return value

    @model_validator(mode="after")
    def require_distinct_storage_roots(self) -> "Settings":
        roots = (self.repository_root, self.worktree_root, self.artifact_root)
        resolved = tuple(root.resolve() for root in roots)
        for index, root in enumerate(resolved):
            for other in resolved[index + 1 :]:
                if root == other or root in other.parents or other in root.parents:
                    raise ValueError("repository, worktree, and artifact roots must be distinct")
        return self


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Load and cache process settings."""

    return Settings()
