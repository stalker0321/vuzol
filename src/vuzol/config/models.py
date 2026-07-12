"""Provider-neutral configuration models and bounded vocabularies."""

from enum import StrEnum
from ipaddress import ip_address
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, model_validator


class FrozenModel(BaseModel):
    """Strict immutable base for configuration snapshots."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class Capability(StrEnum):
    REPOSITORY_READ = "repository_read"
    FILESYSTEM_WRITE = "filesystem_write"
    CODE_EDIT = "code_edit"
    GIT = "git"
    PROJECT_SHELL = "project_shell"
    NETWORK = "network"
    WEB_RESEARCH = "web_research"
    TRANSCRIPTION = "transcription"
    SECRETS = "secrets"  # pragma: allowlist secret
    HOST_ADMIN = "host_admin"
    TELEGRAM_SEND = "telegram_send"


class LaunchMode(StrEnum):
    API = "api"
    CLI = "cli"
    TOOL = "tool"


class ProviderRole(StrEnum):
    INTERPRETER = "interpreter"
    PLANNER = "planner"
    EXECUTOR = "executor"
    REVIEWER = "reviewer"
    SUMMARIZER = "summarizer"
    TRANSCRIBER = "transcriber"


class BudgetMode(StrEnum):
    CHEAP = "cheap"
    BALANCED = "balanced"
    STRONG = "strong"


class CostClass(StrEnum):
    CHEAP = "cheap"
    BALANCED = "balanced"
    STRONG = "strong"


class TopicKind(StrEnum):
    INBOX = "inbox"
    TASK_DASHBOARD = "task_dashboard"
    APPROVALS = "approvals"
    CHANGELOG = "changelog"
    SYSTEM = "system"
    PROJECT = "project"
    PERSONAL = "personal"
    RESEARCH = "research"


class DeliveryMode(StrEnum):
    RETAIN = "retain"
    PATCH = "patch"
    APPLY = "apply"
    MERGE = "merge"
    PUSH = "push"


class CommandDefinition(FrozenModel):
    name: str = Field(pattern=r"^[a-z][a-z0-9_-]*$")
    argv: tuple[str, ...] = Field(min_length=1)
    timeout_seconds: int = Field(default=600, ge=1, le=86_400)
    required: bool = True


class EgressDestination(FrozenModel):
    url: HttpUrl
    purpose: str = Field(min_length=1, max_length=200)

    @model_validator(mode="after")
    def validate_origin(self) -> "EgressDestination":
        if self.url.username is not None or self.url.password is not None:
            raise ValueError("egress destination cannot contain credentials")
        if self.url.query is not None or self.url.fragment is not None:
            raise ValueError("egress destination must be an origin without query or fragment")
        host = self.url.host
        if host in {"localhost", "metadata.google.internal"}:
            raise ValueError("local and metadata endpoints are prohibited")
        if host is not None:
            try:
                parsed_address = ip_address(host)
            except ValueError:
                pass
            else:
                if not parsed_address.is_global:
                    raise ValueError("non-global IP egress destinations are prohibited")
        return self


class NetworkPolicy(FrozenModel):
    enabled: bool = False
    destinations: tuple[EgressDestination, ...] = ()

    @model_validator(mode="after")
    def validate_destinations(self) -> "NetworkPolicy":
        if not self.enabled and self.destinations:
            raise ValueError("disabled network policy cannot declare destinations")
        if self.enabled and not self.destinations:
            raise ValueError("enabled network policy requires at least one destination")
        if any(destination.url.scheme != "https" for destination in self.destinations):
            raise ValueError("egress destinations must use https")
        return self


class GitDeliveryPolicy(FrozenModel):
    allowed_modes: frozenset[DeliveryMode] = frozenset({DeliveryMode.RETAIN, DeliveryMode.PATCH})
    approval_required: frozenset[DeliveryMode] = frozenset()

    @model_validator(mode="after")
    def validate_approval_modes(self) -> "GitDeliveryPolicy":
        if not self.approval_required.issubset(self.allowed_modes):
            raise ValueError("approval-required delivery modes must also be allowed")
        return self


class ProjectConfig(FrozenModel):
    id: str = Field(pattern=r"^[a-z][a-z0-9_-]*$")
    display_name: str = Field(min_length=1, max_length=100)
    repository_path: Path
    default_branch: str = Field(min_length=1, max_length=255)
    allowed_capabilities: frozenset[Capability]
    validation_commands: tuple[CommandDefinition, ...] = ()
    sandbox_profile: str = Field(min_length=1)
    summary_path: Path | None = None
    enabled: bool = True
    git_delivery: GitDeliveryPolicy = GitDeliveryPolicy()
    network: NetworkPolicy = NetworkPolicy()


class ProviderProfileConfig(FrozenModel):
    id: str = Field(pattern=r"^[a-z][a-z0-9_-]*$")
    provider: str = Field(min_length=1)
    model: str = Field(min_length=1)
    api_base_url: HttpUrl | None = None
    launch_mode: LaunchMode
    credential_reference: str | None = Field(default=None, pattern=r"^(env|file):.+$")
    credential_required: bool = True
    capabilities: frozenset[Capability]
    concurrency_limit: int = Field(ge=1, le=100)
    context_limit: int | None = Field(default=None, ge=1)
    output_limit: int | None = Field(default=None, ge=1)
    cost_class: CostClass
    roles: frozenset[ProviderRole] = frozenset()
    routing_priority: int = Field(default=100, ge=0, le=10_000)
    supported_task_types: frozenset[str]
    fallback_profile_ids: tuple[str, ...] = ()
    sandbox_required: bool = True
    input_cost_units_per_million: float | None = Field(default=None, ge=0)
    output_cost_units_per_million: float | None = Field(default=None, ge=0)
    quota_units_per_call: float | None = Field(default=None, ge=0)
    minimum_unknown_usage_cost: float = Field(default=0.01, gt=0)
    runtime_identity: str | None = Field(default=None, min_length=1, max_length=100)
    state_directory: Path | None = None
    enabled: bool = True

    @model_validator(mode="after")
    def validate_credential_reference(self) -> "ProviderProfileConfig":
        if self.enabled and self.credential_required and self.credential_reference is None:
            raise ValueError("enabled profile requires a credential reference")
        if self.api_base_url is not None:
            url = self.api_base_url
            if url.scheme != "https" or url.username is not None or url.password is not None:
                raise ValueError("provider API base URL must be a credential-free HTTPS URL")
            if url.query is not None or url.fragment is not None:
                raise ValueError("provider API base URL cannot contain query or fragment")
            if url.host in {"localhost", "metadata.google.internal"}:
                raise ValueError("provider API base URL cannot target local or metadata hosts")
            if url.host is not None:
                try:
                    provider_address = ip_address(url.host)
                except ValueError:
                    pass
                else:
                    if not provider_address.is_global:
                        raise ValueError("provider API base URL must use a global address")
        if self.launch_mode is LaunchMode.API and self.api_base_url is None:
            raise ValueError("API profile requires api_base_url")
        if self.launch_mode is LaunchMode.CLI:
            if self.runtime_identity is None or self.state_directory is None:
                raise ValueError("CLI profile requires runtime_identity and state_directory")
            if not self.state_directory.is_absolute():
                raise ValueError("CLI profile state_directory must be absolute")
            home = Path.home().resolve()
            state = self.state_directory.resolve()
            if state in {home, home / ".codex"}:
                raise ValueError("CLI profile cannot use the application user's default home")
        elif self.runtime_identity is not None or self.state_directory is not None:
            raise ValueError("runtime identity and state directory are CLI-only")
        if (
            self.input_cost_units_per_million == 0
            and self.output_cost_units_per_million == 0
            and not self.quota_units_per_call
        ):
            raise ValueError("explicit zero pricing requires a non-zero quota charge")
        return self


class TopicConfig(FrozenModel):
    chat_id: int
    message_thread_id: int = Field(ge=1)
    kind: TopicKind
    project_id: str | None = None
    accepts_new_tasks: bool = True
    default_workflow: str = Field(min_length=1)
    enabled: bool = True

    @model_validator(mode="after")
    def validate_project_scope(self) -> "TopicConfig":
        if self.kind is TopicKind.PROJECT and self.project_id is None:
            raise ValueError("project topic requires project_id")
        if self.kind is not TopicKind.PROJECT and self.project_id is not None:
            raise ValueError("only project topics may declare project_id")
        return self


class RegistryDocument(FrozenModel):
    projects: tuple[ProjectConfig, ...] = ()
    profiles: tuple[ProviderProfileConfig, ...] = ()
    topics: tuple[TopicConfig, ...] = ()
