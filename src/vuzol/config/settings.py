"""Typed process settings and enforceable operational defaults."""

from functools import lru_cache
from pathlib import Path
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ConcurrencyLimits(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    control: int = Field(default=2, ge=1, le=100)
    light: int = Field(default=2, ge=1, le=100)
    heavy: int = Field(default=1, ge=1, le=10)
    privileged: int = Field(default=1, ge=1, le=10)


class DiskPressureSettings(BaseModel):
    """Low-watermark gate for **new heavy** work only (S10-3a).

    ``min_free_bytes=0`` (default) disables the gate — compatible with prior
    behavior. When positive, free space on each measured path must be >= the
    threshold. Empty ``paths`` means measure ``worktree_root`` and
    ``artifact_root`` from process settings at evaluation time.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    min_free_bytes: int = Field(default=0, ge=0, le=100_000_000_000_000)
    paths: tuple[Path, ...] = ()

    @field_validator("paths")
    @classmethod
    def require_absolute_paths(cls, value: tuple[Path, ...]) -> tuple[Path, ...]:
        for path in value:
            if not path.is_absolute():
                raise ValueError("disk pressure paths must be absolute when configured")
        return value


class OpenCodeSnapshotBinding(BaseModel):
    """Usage-only transport; never changes execution profile revisions."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    account_id: str = Field(pattern=r"^[a-z][a-z0-9_-]*$", max_length=100)
    scope_id: str = Field(min_length=1, max_length=160)
    path: Path

    @field_validator("path")
    @classmethod
    def absolute_path(cls, value: Path) -> Path:
        if not value.is_absolute():
            raise ValueError("usage snapshot path must be absolute")
        return value


class SubscriptionLimitSettings(BaseModel):
    """Grok dashboard limit source (S1d wiring; default preserves legacy host collect).

    ``source=legacy`` ignores ``snapshot_file`` (S1c API). ``snapshot_required``
    needs an absolute configured path; the file need not exist at process start
    (exporter may publish later). No isolation claim while default is legacy.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    source: Literal["legacy", "snapshot_required"] = "legacy"
    snapshot_file: Path | None = None
    snapshot_max_age_seconds: int = Field(default=900, ge=1, le=86_400)
    opencode_snapshots: tuple[OpenCodeSnapshotBinding, ...] = ()

    @field_validator("opencode_snapshots")
    @classmethod
    def unique_meters(
        cls, value: tuple[OpenCodeSnapshotBinding, ...]
    ) -> tuple[OpenCodeSnapshotBinding, ...]:
        for field in ("account_id", "scope_id", "path"):
            if len({getattr(item, field) for item in value}) != len(value):
                raise ValueError("duplicate OpenCode snapshot binding")
        return value

    @model_validator(mode="after")
    def require_absolute_snapshot_when_required(self) -> "SubscriptionLimitSettings":
        if self.source != "snapshot_required":
            return self
        if self.snapshot_file is None:
            raise ValueError(
                "subscription_limits.snapshot_file is required when source is snapshot_required"
            )
        if not self.snapshot_file.is_absolute():
            raise ValueError(
                "subscription_limits.snapshot_file must be absolute "
                "when source is snapshot_required"
            )
        return self


class SecretIngressSettings(BaseModel):
    """Portable one-time web ingress for allowlisted deployment secrets."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    enabled: bool = False
    public_base_url: HttpUrl | None = None
    storage_root: Path = Path("/var/lib/vuzol-managed-secrets")
    allowed_names: tuple[str, ...] = ()
    ttl_seconds: int = Field(default=300, ge=60, le=3_600)
    maximum_secret_bytes: int = Field(default=16_384, ge=1, le=65_536)

    @field_validator("allowed_names")
    @classmethod
    def validate_names(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        import re

        if len(set(value)) != len(value) or any(
            re.fullmatch(r"[A-Z][A-Z0-9_]{0,99}", name) is None for name in value
        ):
            raise ValueError("secret ingress names must be unique uppercase identifiers")
        return value

    @model_validator(mode="after")
    def validate_enabled_configuration(self) -> "SecretIngressSettings":
        if not self.storage_root.is_absolute():
            raise ValueError("secret ingress storage_root must be absolute")
        if self.enabled and (self.public_base_url is None or not self.allowed_names):
            raise ValueError("enabled secret ingress requires public_base_url and allowed_names")
        return self


class CapabilityProvisioningSettings(BaseModel):
    """Default-off installation of reviewed, offline capability bundles."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    enabled: bool = False
    bundle_root: Path = Path("/etc/vuzol/capability-bundles")
    toolchain_root: Path = Path("/var/lib/vuzol/toolchains")
    download_cache_root: Path = Path("/var/lib/vuzol/capability-downloads")
    # Empty means every capability with a trusted, operator-staged manifest is
    # eligible. A non-empty tuple is an additional deployment allowlist.
    allowed_capabilities: tuple[str, ...] = ()
    maximum_bundle_bytes: int = Field(default=4_000_000_000, ge=1, le=20_000_000_000)
    maximum_files: int = Field(default=200_000, ge=1, le=1_000_000)
    # Static local node descriptor (WP03): identity, probe TTL and secret
    # references (never values).
    node_id: str = Field(default="local", pattern=r"^[a-z0-9][a-z0-9_-]{0,63}$")
    health_ttl_seconds: int = Field(default=900, ge=60, le=86_400)
    secret_refs: tuple[str, ...] = ()

    @field_validator("secret_refs")
    @classmethod
    def validate_secret_refs(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        import re

        if len(set(value)) != len(value) or any(
            re.fullmatch(r"(env|file):.+", item) is None for item in value
        ):
            raise ValueError("capability secret references must be env:/file: references")
        return value

    @field_validator("bundle_root", "toolchain_root", "download_cache_root")
    @classmethod
    def require_absolute_roots(cls, value: Path) -> Path:
        if not value.is_absolute():
            raise ValueError("capability provisioning roots must be absolute")
        return value

    @field_validator("allowed_capabilities")
    @classmethod
    def validate_capability_allowlist(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        import re

        if len(set(value)) != len(value) or any(
            re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", item) is None for item in value
        ):
            raise ValueError("capability provisioning allowlist contains an unsafe key")
        return value

    @model_validator(mode="after")
    def require_separate_roots(self) -> "CapabilityProvisioningSettings":
        roots = (
            self.bundle_root.resolve(),
            self.toolchain_root.resolve(),
            self.download_cache_root.resolve(),
        )
        for index, root in enumerate(roots):
            for other in roots[index + 1 :]:
                if root == other or root in other.parents or other in root.parents:
                    raise ValueError("capability storage roots must be separate")
        return self


class DependencyProvisioningSettings(BaseModel):
    """Default-off immutable project dependency environments."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    enabled: bool = False
    environment_root: Path = Path("/var/lib/vuzol/dependency-environments")
    maximum_environment_bytes: int = Field(default=5_000_000_000, ge=1, le=20_000_000_000)
    maximum_direct_dependencies: int = Field(default=200, ge=1, le=2_000)

    @field_validator("environment_root")
    @classmethod
    def require_absolute_root(cls, value: Path) -> Path:
        if not value.is_absolute():
            raise ValueError("dependency environment root must be absolute")
        return value


class RetentionDefaults(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    completed_worktree_days: int = Field(default=3, ge=1)
    failed_worktree_days: int = Field(default=14, ge=1)
    artifact_days: int = Field(default=14, ge=1)
    voice_days: int = Field(default=3, ge=1)
    log_days: int = Field(default=14, ge=1)
    sweep_batch_size: int = Field(default=50, ge=1, le=1_000)
    sweep_lock_timeout_seconds: float = Field(default=5.0, ge=0.1, le=120.0)

    @model_validator(mode="after")
    def validate_worktree_retention_order(self) -> "RetentionDefaults":
        # Unresolved and failed/blocked work needs at least as long as successful work.
        if self.failed_worktree_days < self.completed_worktree_days:
            raise ValueError("failed worktree retention must not be shorter than completed")
        return self


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
    lead_jobs_nonterminal_per_goal: int = Field(default=2, ge=1, le=100)
    lead_jobs_total_per_goal: int = Field(default=10, ge=1, le=1000)
    lead_tool_rounds: int = Field(default=3, ge=0, le=10)
    lead_tool_calls_per_round: int = Field(default=4, ge=1, le=16)
    lead_tool_result_bytes: int = Field(default=8192, ge=128)
    lead_tool_total_bytes: int = Field(default=24576, ge=128)
    # Primary plus bounded fallbacks. Four leaves room for a provider-originated
    # cancellation followed by a quota-triggered account failover.
    provider_attempts: int = Field(default=6, ge=1, le=20)
    fallback_depth: int = Field(default=2, ge=0, le=10)
    daily_cost_units: float = Field(default=100, gt=0)
    daily_quota_units: float = Field(default=100, gt=0)
    # Watchdog value in seconds. Admission does not enforce it, and it is
    # neither a money ceiling nor a provider-call timeout.
    task_duration_seconds: int = Field(default=7_200, ge=1)
    # Cumulative task/step token totals and estimated cost_units.
    # ``hard`` is the migration default: historical numbers have no recorded
    # origin, so this build does not grant extra spend. ``soft`` records the
    # overrun and continues, including required review. Production stays on
    # ``hard`` until the owner switches it.
    usage_total_enforcement: Literal["soft", "hard"] = "hard"
    estimated_cost_enforcement: Literal["soft", "hard"] = "hard"
    # Owner money ceiling in cost units, across epochs, retries, and child
    # tasks. None adds no ceiling. Review allowance does not bypass it.
    # Unknown cost cannot certify it. Daily and package lifetime budgets
    # stay in force either way.
    explicit_money_cap_units: float | None = None
    artifact_bytes: int = Field(default=100_000_000, ge=1)
    input_bytes: int = Field(default=25_000_000, ge=1)
    # D3 admission counters (lead W4): enforced alongside caps, never instead
    # of them. 0 means unlimited (documented opt-out, not "no calls").
    max_work_attempts: int = Field(default=50, ge=0)
    max_provider_calls: int = Field(default=500, ge=0)
    max_replans: int = Field(default=10, ge=0)
    # D3 review suballocation pool per task (lead Q4): deductible allowance
    # inside the shared ledger. 0 disables the allowance (full caps apply).
    review_allowance_input_tokens: int = Field(default=10_000, ge=0)
    review_allowance_output_tokens: int = Field(default=10_000, ge=0)
    review_allowance_cost_units: float = Field(default=1.0, ge=0)

    @model_validator(mode="after")
    def validate_money_cap(self) -> "HardLimits":
        if self.explicit_money_cap_units is not None and self.explicit_money_cap_units <= 0:
            raise ValueError("explicit money cap must be positive")
        return self


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
    orchestration_trace_enabled: bool = True
    orchestration_trace_sample_percent: int = Field(default=100, ge=0, le=100)
    orchestration_trace_always_include_anomalies: bool = True
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
    structure_profile_id: str | None = Field(default=None, pattern=r"^[a-z][a-z0-9_-]*$")
    manual_project_creation: bool = False
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
    # One term per line; read on every voice note, so edits apply without restart.
    transcription_keywords_file: Path | None = None
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
    # WP04 recovery bounds are policy, not magic constants (invariant 48).
    max_step_repairs: int = Field(default=3, ge=1, le=20)
    max_task_repairs: int = Field(default=6, ge=1, le=50)
    max_backpressure_waits: int = Field(default=5, ge=1, le=50)
    recovery_deadline_seconds: int = Field(default=3_600, ge=60, le=86_400)
    # D6 Q2 kill switch: when True, the workflow dispatcher defers (never
    # dead-letters) new dispatches. In-flight uncertain effects keep flowing
    # to reconciliation; unfinished runs are never marked completed by it.
    dispatch_freeze: bool = Field(default=False)

    @model_validator(mode="after")
    def validate_timing(self) -> "WorkflowSettings":
        if self.heartbeat_seconds * 3 >= self.lease_seconds:
            raise ValueError("workflow heartbeat must be less than one third of lease")
        if self.retry_min_seconds > self.retry_max_seconds:
            raise ValueError("workflow retry minimum must not exceed maximum")
        if self.max_step_repairs > self.max_task_repairs:
            raise ValueError("per-step repair cap must not exceed the task repair cap")
        return self


class ExecutionSettings(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    enabled: bool = False
    reviewer_profile_id: str | None = Field(default=None, pattern=r"^[a-z][a-z0-9_-]*$")
    default_executor_profile_id: str | None = Field(default=None, pattern=r"^[a-z][a-z0-9_-]*$")
    rootless_docker_socket: Path = Path("/run/user/1000/docker.sock")
    require_preflight: bool = True
    cleanup_interval_seconds: int = Field(default=300, ge=10, le=86_400)
    journal_root: Path = Path("/srv/vuzol/runtime-journal")
    journal_key_file: Path | None = None
    provider_parallelism: int = Field(default=2, ge=1, le=16)
    recovery_batch_size: int = Field(default=50, ge=1, le=1000)
    proxy_image: str | None = Field(
        default=None,
        pattern=r"^(?:[^\s@]+@)?sha256:[0-9a-f]{64}$",
    )
    proxy_runtime_root: Path = Path("/var/lib/vuzol-proxy-runtime")
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


class BackupSettings(BaseModel):
    """Backup settings. Timers are never implied; capture is manual and gated."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    # Product "enabled" remains false-by-default and does not mean scheduled.
    enabled: bool = False
    # Manual APPLY capture gate (default off).
    capture_cli_permitted: bool = False
    # Manual APPLY restore gate (default off).
    restore_cli_permitted: bool = False
    # Restore DSN secret ref; loader grants it only to system:backup.
    restore_dsn_reference: str | None = Field(default=None, pattern=r"^(env|file):.+$")
    # Overall restore deadline passthrough; None means no CLI-imposed bound.
    restore_overall_timeout_seconds: float | None = Field(default=None, gt=0)
    # APPLY safety probes stay on unless explicitly disabled for a lab.
    restore_require_empty_target: bool = True
    restore_probe_capture_lock: bool = True
    staging_root: Path | None = None
    drill_root: Path | None = None
    keep_local_runs: int = Field(default=3, ge=1, le=100)
    keep_offhost_days: int = Field(default=28, ge=1, le=3_650)
    keep_failed_runs: bool = False
    min_free_bytes: int = Field(default=2_147_483_648, ge=0, le=10_000_000_000_000)
    rpo_seconds_target: int = Field(default=86_400, ge=60, le=604_800)
    rto_seconds_target: int = Field(default=7_200, ge=60, le=86_400)
    drill_database_name_suffix: str = Field(default="_restore", min_length=1, max_length=32)
    kek_reference: str | None = Field(default=None, pattern=r"^(env|file):.+$")
    postgres_container: str = Field(default="vuzol-postgres-1", min_length=1, max_length=64)
    postgres_dump_argv: tuple[str, ...] | None = None
    allow_non_loopback_dump: bool = False
    quiesce_mode: Literal["none", "manual"] = "none"
    deploy_path: Path = Path("/opt/vuzol")

    @field_validator("staging_root", "drill_root", "deploy_path")
    @classmethod
    def require_absolute_optional_root(cls, value: Path | None) -> Path | None:
        if value is None:
            return None
        if not value.is_absolute():
            raise ValueError("backup roots must be absolute when configured")
        return value

    @field_validator("drill_database_name_suffix")
    @classmethod
    def normalize_suffix(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("drill database name suffix must not be empty")
        if any(character.isspace() for character in cleaned):
            raise ValueError("drill database name suffix must not contain whitespace")
        return cleaned

    @field_validator("postgres_container")
    @classmethod
    def validate_container(cls, value: str) -> str:
        import re

        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", value):
            raise ValueError("postgres_container name is unsafe")
        return value

    @model_validator(mode="after")
    def validate_enabled_is_not_schedule(self) -> "BackupSettings":
        # enabled=true is reserved / still forbidden so timers cannot be smuggled.
        if self.enabled:
            raise ValueError(
                "backup.enabled cannot be true (timers not implemented); "
                "use capture_cli_permitted for manual capture only"
            )
        return self


class ResearchModelChoice(BaseModel):
    """A per-call research assignment on an existing provider account."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    profile_id: str = Field(pattern=r"^[a-z][a-z0-9_-]*$")
    model: str | None = Field(default=None, pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$")
    reasoning_effort: str | None = Field(
        default=None, pattern=r"^(low|medium|high|xhigh|max|ultra)$"
    )


class ProjectLeadSettings(BaseModel):
    """Opt-in project-lead overlay. Off leaves legacy sequencing as the only path."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    enabled: bool = False
    default_goal_max_cost: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    # Empty means the feature can be on while no model is selected. The worker
    # then parks the goal with an explicit blocker instead of inventing a plan.
    profile_id: str | None = Field(default=None, pattern=r"^[a-z][a-z0-9_-]*$")
    reviewer_profile_id: str | None = Field(default=None, pattern=r"^[a-z][a-z0-9_-]*$")
    # Empty preserves the project's planner_strong assignment.
    research_models: list[ResearchModelChoice] = Field(default_factory=list)
    # Tried in order when the assigned planner hits its quota or is unavailable.
    planner_fallback_profile_ids: list[str] = Field(default_factory=list)
    # Wall-clock bound for each lead/review invocation, independent of model/provider.
    inference_timeout_seconds: int = Field(default=60, ge=1, le=600)
    # Each entry schedules one safe resend; never more than three retries.
    model_retry_delays_seconds: tuple[
        Annotated[float, Field(ge=0, le=3600, allow_inf_nan=False)], ...
    ] = Field(default=(30, 120, 600), max_length=3)
    repeat_limit: int = Field(default=2, ge=1, le=20)
    job_concurrency: int = Field(default=2, ge=1, le=100)
    lease_seconds: int = Field(default=120, ge=30, le=3600)
    integration_branch_prefix: str = Field(default="vuzol-managed/", min_length=1, max_length=80)


class ChatSettings(BaseModel):
    """Opt-in read-only conversation, independent of project executor preferences."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    enabled: bool = False
    profile_id: str | None = Field(default=None, pattern=r"^[a-z][a-z0-9_-]*$")
    # Capability opt-in, not "every answer is a post": delivery chooses a
    # rich post only for a multi-part reply or rich-only structure (tables).
    rich_messages: bool = False
    memory_enabled: bool = False

    @model_validator(mode="after")
    def require_responder(self) -> "ChatSettings":
        if self.enabled and self.profile_id is None:
            raise ValueError("chat.enabled requires an explicit chat.profile_id")
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
    project_discussion_enabled: bool = False
    project_lead: ProjectLeadSettings = ProjectLeadSettings()
    chat: ChatSettings = ChatSettings()
    # J5 canary: at most one Jev decision kind in a small deterministic cohort.
    # Empty means disabled (the default); the kill switch freezes kinds.
    jev_enabled_kinds: tuple[str, ...] = ()
    jev_canary_percent: int = Field(default=0, ge=0, le=100)
    jev_kill_switch_kinds: tuple[str, ...] = ()
    database_dsn_reference: str | None = Field(default=None, pattern=r"^(env|file):.+$")
    telegram_bot_token_reference: str | None = Field(default=None, pattern=r"^(env|file):.+$")
    allowed_user_ids: tuple[int, ...] = ()
    allowed_chat_ids: tuple[int, ...] = ()
    repository_root: Path = Path("/srv/vuzol/repositories")
    worktree_root: Path = Path("/srv/vuzol/worktrees")
    artifact_root: Path = Path("/srv/vuzol/artifacts")
    artifact_delivery_uid: int | None = Field(default=None, ge=1)
    static_site_root: Path = Path("/srv/vuzol/sites/hryshyn.dev")
    static_site_base_url: HttpUrl = HttpUrl("https://hryshyn.dev")
    preview_site_root: Path = Path("/srv/vuzol/sites/test.hryshyn.dev")
    preview_site_base_url: HttpUrl = HttpUrl("https://test.hryshyn.dev")
    preview_gateway_host: str = "127.0.0.1"
    preview_gateway_port: int = Field(default=8091, ge=1024, le=65535)
    preview_export_max_bytes: int = Field(default=250_000_000, ge=1)
    preview_export_max_files: int = Field(default=100_000, ge=1)
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
    backup: BackupSettings = Field(default_factory=BackupSettings)
    disk_pressure: DiskPressureSettings = Field(default_factory=DiskPressureSettings)
    subscription_limits: SubscriptionLimitSettings = Field(
        default_factory=SubscriptionLimitSettings
    )
    secret_ingress: SecretIngressSettings = Field(default_factory=SecretIngressSettings)
    capability_provisioning: CapabilityProvisioningSettings = Field(
        default_factory=CapabilityProvisioningSettings
    )
    dependency_provisioning: DependencyProvisioningSettings = Field(
        default_factory=DependencyProvisioningSettings
    )

    @field_validator(
        "repository_root",
        "worktree_root",
        "artifact_root",
        "static_site_root",
        "preview_site_root",
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
        roots = (
            self.repository_root,
            self.worktree_root,
            self.artifact_root,
            self.capability_provisioning.toolchain_root,
            self.capability_provisioning.download_cache_root,
            self.dependency_provisioning.environment_root,
            *((self.execution.journal_root,) if self.execution.journal_key_file else ()),
        )
        resolved = tuple(root.resolve() for root in roots)
        for index, root in enumerate(resolved):
            for other in resolved[index + 1 :]:
                if root == other or root in other.parents or other in root.parents:
                    raise ValueError(
                        "repository, worktree, artifact, and toolchain roots must be distinct"
                    )
        return self


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Load and cache process settings."""

    return Settings()
