"""Provider-neutral configuration models and bounded vocabularies."""

from enum import StrEnum
from ipaddress import ip_address
from pathlib import Path
from typing import Literal

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


class AgentEditMechanism(StrEnum):
    SHELL_BACKED_REPOSITORY_TOOLS = "shell_backed_repository_tools"


class StructuredOutputSource(StrEnum):
    FINAL_AGENT_MESSAGE_JSON = "final_agent_message_json"
    TRANSPORT_OBJECT = "transport_object"


class InnerSandboxMode(StrEnum):
    PROVIDER_MANAGED = "provider_managed"
    OUTER_SANDBOX_ONLY = "outer_sandbox_only"


class AgentRuntimeContract(FrozenModel):
    """Versioned operational facts for one provider agent runtime."""

    schema_version: str = "agent-runtime-contract.v1"
    cli_version: str = Field(min_length=1, max_length=100)
    edit_mechanism: AgentEditMechanism
    working_directory: Path
    writable_roots: tuple[Path, ...] = Field(min_length=1, max_length=8)
    protected_roots: tuple[Path, ...] = Field(default=(), max_length=8)
    structured_output_source: StructuredOutputSource
    inner_sandbox_mode: InnerSandboxMode
    supports_read: bool
    supports_search: bool
    supports_edit: bool
    supports_git: bool
    supports_network: bool
    supports_local_checks: bool

    @model_validator(mode="after")
    def validate_paths(self) -> "AgentRuntimeContract":
        paths = (self.working_directory, *self.writable_roots, *self.protected_roots)
        if any(not path.is_absolute() for path in paths):
            raise ValueError("agent runtime paths must be absolute")
        if len(set(self.writable_roots)) != len(self.writable_roots):
            raise ValueError("agent writable roots must be unique")
        if set(self.writable_roots).intersection(self.protected_roots):
            raise ValueError("agent protected roots cannot also be writable roots")
        return self


class ProviderRole(StrEnum):
    INTERPRETER = "interpreter"
    PLANNER = "planner"
    EXECUTOR = "executor"
    REVIEWER = "reviewer"
    SUMMARIZER = "summarizer"
    TRANSCRIBER = "transcriber"
    LEAD = "lead"


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


class SandboxNetworkMode(StrEnum):
    NONE = "none"
    HTTPS_PROXY = "https_proxy"


class SandboxProfileConfig(FrozenModel):
    id: str = Field(pattern=r"^[a-z][a-z0-9_-]*$")
    image: str = Field(pattern=r"^[^\s@]+@sha256:[0-9a-f]{64}$")
    uid: int = Field(default=10001, ge=1, le=2_147_483_647)
    gid: int = Field(default=10001, ge=1, le=2_147_483_647)
    cpu_count: float = Field(default=1.0, gt=0, le=16)
    memory_bytes: int = Field(default=1_073_741_824, ge=67_108_864)
    pids_limit: int = Field(default=128, ge=16, le=4096)
    tmpfs_bytes: int = Field(default=134_217_728, ge=1_048_576)
    open_files_limit: int = Field(default=1024, ge=64, le=65_536)
    output_bytes: int = Field(default=10_000_000, ge=1024)
    timeout_seconds: int = Field(default=3600, ge=1, le=86_400)
    stop_grace_seconds: int = Field(default=10, ge=1, le=300)
    network_mode: SandboxNetworkMode = SandboxNetworkMode.NONE
    proxy_network: str | None = Field(default=None, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_.-]*$")
    https_proxy_url: HttpUrl | None = None
    inner_codex_sandbox_required: bool = True
    enabled: bool = True

    @model_validator(mode="after")
    def validate_network_transport(self) -> "SandboxProfileConfig":
        configured = self.proxy_network is not None or self.https_proxy_url is not None
        if configured:
            raise ValueError("proxy network and URL are materialized only by the execution runtime")
        return self


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
        if host:
            h = host
            if h.startswith("[") and h.endswith("]"):
                h = h[1:-1]
            if h.lower() in {"localhost", "metadata.google.internal"} or h.lower().endswith(
                (".local", ".localhost")
            ):
                raise ValueError("local and metadata endpoints are prohibited")
            try:
                ip_address(h)
                raise ValueError(
                    "IP literal egress destinations are prohibited; use hostnames only"
                )
            except ValueError as e:
                if "IP literal" in str(e):
                    raise
        if self.url.path and self.url.path not in ("", "/"):
            raise ValueError("egress destination must be an origin without path")
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


class StaticDeploymentConfig(FrozenModel):
    """A project-owned preview contract executed only by the trusted publisher."""

    enabled: bool = True
    url_path: str = Field(pattern=r"^[a-z][a-z0-9-]{0,62}$")
    source_directory: Path = Path(".")
    include: tuple[Path, ...] = (
        Path("index.html"),
        Path("app.js"),
        Path("styles.css"),
        Path("assets"),
    )
    entrypoint: Path = Path("index.html")
    build_command: Literal["make build"] | None = None
    keep_releases: int = Field(default=5, ge=2, le=20)

    @model_validator(mode="after")
    def validate_relative_paths(self) -> "StaticDeploymentConfig":
        paths = (self.source_directory, self.entrypoint, *self.include)
        if not self.include:
            raise ValueError("static deployment include list cannot be empty")
        if any(path.is_absolute() or ".." in path.parts for path in paths):
            raise ValueError("static deployment paths must stay relative to the repository")
        return self


class ProjectConfig(FrozenModel):
    id: str = Field(pattern=r"^[a-z][a-z0-9_-]*$")
    display_name: str = Field(min_length=1, max_length=100)
    repository_path: Path
    default_branch: str = Field(min_length=1, max_length=255)
    allowed_capabilities: frozenset[Capability]
    validation_commands: tuple[CommandDefinition, ...] = ()
    sandbox_profile: str = Field(min_length=1)
    validation_sandbox_profile: str | None = Field(default=None, min_length=1)
    summary_path: Path | None = None
    enabled: bool = True
    git_delivery: GitDeliveryPolicy = GitDeliveryPolicy()
    network: NetworkPolicy = NetworkPolicy()
    static_deployment: StaticDeploymentConfig | None = None


OpenRouterSortBy = Literal["price", "throughput", "latency"]
OpenRouterSortPartition = Literal["model", "none"]
OpenRouterQuantization = Literal[
    "int4", "int8", "fp4", "fp6", "fp8", "fp16", "bf16", "fp32", "unknown"
]


class OpenRouterSort(FrozenModel):
    by: OpenRouterSortBy
    partition: OpenRouterSortPartition = "model"


class OpenRouterPreferredMinThroughput(FrozenModel):
    p50: int | None = Field(default=None, gt=0)
    p75: int | None = Field(default=None, gt=0)
    p90: int | None = Field(default=None, gt=0)
    p99: int | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def require_percentile(self) -> "OpenRouterPreferredMinThroughput":
        if not any(value is not None for value in (self.p50, self.p75, self.p90, self.p99)):
            raise ValueError("OpenRouter throughput requires at least one percentile")
        return self


class OpenRouterProviderRouting(FrozenModel):
    """Bounded OpenRouter provider selection sent with each inference request."""

    sort: OpenRouterSort
    preferred_min_throughput: OpenRouterPreferredMinThroughput
    quantizations: tuple[OpenRouterQuantization, ...] = Field(min_length=1, max_length=8)
    allow_fallbacks: bool = True

    @model_validator(mode="after")
    def validate_unique_quantizations(self) -> "OpenRouterProviderRouting":
        if len(set(self.quantizations)) != len(self.quantizations):
            raise ValueError("OpenRouter quantizations must not contain duplicates")
        return self


class OpenCodeUsageOAuthConfig(FrozenModel):
    """Read-only meter credential, NOT an inference credential or account slot.

    OpenCode owns refresh/write-back. The collector only follows this exact
    record; desktop active-account changes must never select another identity.
    """

    store_path: Path
    store_format: Literal["credential_v2", "account_v1"] = "credential_v2"
    credential_id: str | None = Field(default=None, pattern=r"^[a-zA-Z0-9_.-]+$", max_length=160)
    account_id: str = Field(pattern=r"^[a-zA-Z0-9_.-]+$", max_length=60)
    org_id: str = Field(pattern=r"^[a-zA-Z0-9_.-]+$", max_length=60)
    refresh_owner: Literal["opencode_cli"] = "opencode_cli"

    @property
    def scope_id(self) -> str:
        return f"opencode:organization:{self.org_id}:account:{self.account_id}"

    @model_validator(mode="after")
    def validate_store(self) -> "OpenCodeUsageOAuthConfig":
        path = self.store_path
        # This is an explicitly selected read-only source, not the runtime's
        # writable canonical state. Do not force an auth migration merely to
        # measure an existing operator-owned login. ACL checks happen on read.
        if not path.is_absolute() or path.name != "opencode.db":
            raise ValueError("usage OAuth requires an explicit absolute opencode.db path")
        if (self.store_format == "credential_v2") != (self.credential_id is not None):
            raise ValueError(
                "credential_v2 requires credential_id; account_v1 uses account_id only"
            )
        return self


class AccountConfig(FrozenModel):
    """Operator-owned account facts, compiled into existing runtime profiles.

    No credentials or model choices belong here. Legacy inline profiles remain
    valid; adopting a descriptor is an explicit migration, never an ID rewrite.
    """

    id: str = Field(pattern=r"^[a-z][a-z0-9_-]*$", max_length=100)
    service: str = Field(pattern=r"^[a-z][a-z0-9_-]*$", max_length=100)
    provider: str
    launch_mode: LaunchMode
    auth_method: Literal["static_api_key", "cli_oauth", "none"]
    auth_owner: Literal["operator", "serialized_cli"] = "operator"
    state_directory: Path | None = None
    runtime_identity: str | None = None
    credential_reference: str | None = Field(default=None, pattern=r"^(env|file):.+$")
    credential_required: bool = False
    api_base_url: HttpUrl | None = None
    runtime_network: NetworkPolicy = NetworkPolicy()
    concurrency_limit: int = Field(default=1, ge=1, le=100)
    control_reserved_slots: int = Field(default=0, ge=0, le=99)
    lifecycle: Literal["enabled", "draining", "retired"] = "enabled"
    quota_collector: Literal["none", "opencode_go", "codex", "grok"] = "none"
    # This describes the configured meter, not independently verified workspace
    # identity. Never present an account-wide meter as per-model allowances.
    quota_scope_id: str | None = Field(default=None, pattern=r"^[a-zA-Z0-9:_.-]+$", max_length=160)
    quota_oauth: OpenCodeUsageOAuthConfig | None = None
    subscription_limits_visible: bool = True

    @model_validator(mode="after")
    def validate_account(self) -> "AccountConfig":
        if self.control_reserved_slots >= self.concurrency_limit:
            raise ValueError("account must leave a worker slot")
        if self.launch_mode is LaunchMode.CLI:
            if self.state_directory is None or self.runtime_identity is None:
                raise ValueError("CLI account requires state_directory and runtime_identity")
            if not self.state_directory.is_absolute():
                raise ValueError("account state path must be absolute")
            path = self.state_directory.resolve()
            if path == Path("/") or any(
                path.is_relative_to(root) for root in (Path("/home"), Path("/root"), Path("/Users"))
            ):
                raise ValueError("canonical account state must be outside personal homes")
        if self.auth_method == "cli_oauth":
            if self.auth_owner != "serialized_cli" or self.concurrency_limit != 1:
                raise ValueError("CLI OAuth requires one serialized refresh owner")
            if self.provider == "pi":
                raise ValueError("Pi tmpfs OAuth rotation has no supported canonical refresh owner")
        if self.quota_collector == "opencode_go":
            if (self.service, self.provider, self.launch_mode, self.auth_method) != (
                "opencode-go",
                "pi",
                LaunchMode.CLI,
                "static_api_key",
            ):
                raise ValueError(
                    "Go usage currently supports explicit Pi Go static-key bindings only"
                )
            if self.quota_scope_id is None:
                raise ValueError("Go usage requires an explicit quota_scope_id")
        if self.quota_oauth is not None:
            if self.quota_collector != "opencode_go":
                raise ValueError("usage OAuth is only supported by the Go collector")
            if self.quota_scope_id != self.quota_oauth.scope_id:
                raise ValueError("OAuth quota scope must identify the configured account and org")
        if self.quota_collector in {"codex", "grok"} and (
            self.provider != self.quota_collector or self.launch_mode is not LaunchMode.CLI
        ):
            raise ValueError("quota collector does not match CLI runtime binding")
        return self


class ProviderProfileConfig(FrozenModel):
    id: str = Field(pattern=r"^[a-z][a-z0-9_-]*$")
    # Operator-assigned, non-secret account identity, independent of model/role.
    # None preserves the default of isolated CLI profiles.
    account_id: str | None = Field(default=None, pattern=r"^[a-z][a-z0-9_-]*$", max_length=100)
    # Compiled descriptor, not another editable source of account settings.
    account: AccountConfig | None = None
    provider: str = Field(min_length=1)
    model: str = Field(min_length=1)
    # Optional reasoning effort for models that expose it. CLI profiles forward
    # it to the agent CLI; API profiles map it onto the OpenRouter unified
    # reasoning.effort hint.
    model_reasoning_effort: str | None = Field(
        default=None,
        pattern=r"^(low|medium|high|xhigh|max|ultra)$",
    )
    # Role-scoped reasoning controls for OpenAI-compatible API profiles.
    # max_reasoning_tokens is only a soft upstream hint: several providers
    # (DeepSeek family) ignore reasoning caps entirely, so output_limit must be
    # sized for the worst case. reasoning_enabled=False disables thinking where
    # the provider supports it; None keeps provider defaults.
    max_reasoning_tokens: int | None = Field(default=None, ge=1)
    reasoning_enabled: bool | None = None
    api_base_url: HttpUrl | None = None
    provider_routing: OpenRouterProviderRouting | None = None
    launch_mode: LaunchMode
    credential_reference: str | None = Field(default=None, pattern=r"^(env|file):.+$")
    credential_required: bool = True
    capabilities: frozenset[Capability]
    concurrency_limit: int = Field(ge=1, le=100)
    control_reserved_slots: int = Field(default=0, ge=0, le=99)
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
    # Dashboard/collector visibility only; retiring a subscription must not
    # silently disable or reroute execution profiles.
    subscription_limits_visible: bool = True
    runtime_network: NetworkPolicy = NetworkPolicy()
    agent_runtime_contract: AgentRuntimeContract | None = None
    enabled: bool = True

    @property
    def account_binding(self) -> tuple[object, ...]:
        """Fields that must not vary between profiles for one actual account."""
        return (
            self.account_id,
            self.provider,
            self.launch_mode,
            self.credential_reference,
            self.credential_required,
            self.api_base_url,
            self.runtime_network,
            self.state_directory.resolve() if self.state_directory is not None else None,
            self.runtime_identity,
            self.concurrency_limit,
            self.control_reserved_slots,
            self.account.model_copy(
                update={
                    "quota_oauth": None,
                    "quota_scope_id": None,
                    "quota_collector": "none",
                    "subscription_limits_visible": True,
                }
            )
            if self.account
            else None,
        )

    @model_validator(mode="after")
    def validate_credential_reference(self) -> "ProviderProfileConfig":
        if self.account is not None and self.account_id != self.account.id:
            raise ValueError("compiled account identity mismatch")
        if self.control_reserved_slots >= self.concurrency_limit:
            raise ValueError("control_reserved_slots must leave at least one worker slot")
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
        if self.provider_routing is not None:
            if self.launch_mode is not LaunchMode.API or self.api_base_url is None:
                raise ValueError("OpenRouter provider routing requires an API profile")
            if self.api_base_url.host != "openrouter.ai":
                raise ValueError("OpenRouter provider routing requires openrouter.ai")
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
        elif self.runtime_network.enabled:
            raise ValueError("runtime network policy is CLI-only")
        if (
            self.input_cost_units_per_million == 0
            and self.output_cost_units_per_million == 0
            and not self.quota_units_per_call
        ):
            raise ValueError("explicit zero pricing requires a non-zero quota charge")
        if self.provider == "codex" and self.agent_runtime_contract is not None:
            contract = self.agent_runtime_contract
            if contract.working_directory != Path("/workspace"):
                raise ValueError("Codex runtime contract must use /workspace")
            if contract.writable_roots != (Path("/workspace"),):
                raise ValueError("Codex writable roots must be exactly /workspace")
            if Path("/workspace/.git") not in contract.protected_roots:
                raise ValueError("Codex runtime contract must protect /workspace/.git")
            if Path("/artifacts") in contract.writable_roots:
                raise ValueError("Codex runtime contract cannot make /artifacts writable")
            if not (contract.supports_read and contract.supports_search and contract.supports_edit):
                raise ValueError("Codex runtime contract must support repository read/search/edit")
            if contract.supports_git or contract.supports_network or contract.supports_local_checks:
                raise ValueError(
                    "Codex worker runtime cannot support Git, network, or local checks"
                )
            if (
                contract.structured_output_source
                is not StructuredOutputSource.FINAL_AGENT_MESSAGE_JSON
            ):
                raise ValueError("Codex structured output must use final agent-message JSON")
        return self


class TopicConfig(FrozenModel):
    chat_id: int
    message_thread_id: int = Field(ge=1)
    kind: TopicKind
    display_name: str | None = Field(default=None, min_length=1, max_length=128)
    project_id: str | None = None
    accepts_new_tasks: bool = True
    default_workflow: str = Field(min_length=1)
    enabled: bool = True
    # Desired forum-topic pin state. None means "derive from product layout policy":
    # system control topics pin by kind; project topics stay unpinned unless set True
    # at provision time (cleared later on pause/complete).
    pinned: bool | None = None

    @model_validator(mode="after")
    def validate_project_scope(self) -> "TopicConfig":
        if self.kind is TopicKind.PROJECT and self.project_id is None:
            raise ValueError("project topic requires project_id")
        if self.kind is not TopicKind.PROJECT and self.project_id is not None:
            raise ValueError("only project topics may declare project_id")
        return self


class RegistryDocument(FrozenModel):
    accounts: tuple[AccountConfig, ...] = ()
    projects: tuple[ProjectConfig, ...] = ()
    profiles: tuple[ProviderProfileConfig, ...] = ()
    topics: tuple[TopicConfig, ...] = ()
    sandboxes: tuple[SandboxProfileConfig, ...] = ()
