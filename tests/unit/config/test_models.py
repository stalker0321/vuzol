from pathlib import Path

import pytest
from pydantic import ValidationError

from vuzol.config import (
    DeliveryMode,
    GitDeliveryPolicy,
    NetworkPolicy,
    ProviderProfileConfig,
    SandboxProfileConfig,
    TopicConfig,
)


def test_sandbox_requires_immutable_image_digest() -> None:
    with pytest.raises(ValidationError, match="string_pattern_mismatch"):
        SandboxProfileConfig(id="default", image="example/sandbox:latest")

    configured = SandboxProfileConfig(id="default", image=f"example/sandbox@sha256:{'a' * 64}")
    assert configured.uid != 0


def test_sandbox_proxy_transport_is_closed_and_complete() -> None:
    image = f"example/sandbox@sha256:{'a' * 64}"
    with pytest.raises(ValidationError, match="requires proxy network and URL"):
        SandboxProfileConfig.model_validate(
            {"id": "proxy", "image": image, "network_mode": "https_proxy"}
        )
    with pytest.raises(ValidationError, match="cannot configure a proxy"):
        SandboxProfileConfig.model_validate(
            {
                "id": "none",
                "image": image,
                "proxy_network": "egress",
                "https_proxy_url": "http://proxy:3128",
            }
        )
    configured = SandboxProfileConfig.model_validate(
        {
            "id": "proxy",
            "image": image,
            "network_mode": "https_proxy",
            "proxy_network": "egress",
            "https_proxy_url": "http://proxy:3128",
        }
    )
    assert configured.proxy_network == "egress"


def test_network_policy_requires_https_destinations() -> None:
    with pytest.raises(ValidationError, match="requires at least one destination"):
        NetworkPolicy(enabled=True)

    with pytest.raises(ValidationError, match="must use https"):
        NetworkPolicy.model_validate(
            {
                "enabled": True,
                "destinations": [{"url": "http://example.com", "purpose": "test"}],
            }
        )


def test_disabled_network_policy_rejects_destinations() -> None:
    with pytest.raises(ValidationError, match="disabled network policy"):
        NetworkPolicy.model_validate(
            {"destinations": [{"url": "https://example.com", "purpose": "test"}]}
        )


@pytest.mark.parametrize(
    "url",
    [
        "https://user@example.com",
        "https://example.com?target=other",
        "https://127.0.0.1",
        "https://169.254.169.254",
        "https://metadata.google.internal",
    ],
)
def test_egress_destination_rejects_credentials_metadata_and_local_hosts(url: str) -> None:
    with pytest.raises(ValidationError):
        NetworkPolicy.model_validate(
            {"enabled": True, "destinations": [{"url": url, "purpose": "test"}]}
        )


def test_git_delivery_approval_must_be_allowed() -> None:
    with pytest.raises(ValidationError, match="must also be allowed"):
        GitDeliveryPolicy(
            allowed_modes=frozenset({DeliveryMode.RETAIN}),
            approval_required=frozenset({DeliveryMode.PUSH}),
        )


def test_enabled_profile_requires_credential_reference() -> None:
    with pytest.raises(ValidationError, match="requires a credential reference"):
        ProviderProfileConfig.model_validate(
            {
                "id": "profile-a",
                "provider": "provider",
                "model": "model",
                "launch_mode": "api",
                "capabilities": ["repository_read"],
                "concurrency_limit": 1,
                "cost_class": "balanced",
                "supported_task_types": ["general"],
            }
        )


def test_provider_api_base_url_is_safe_https_origin() -> None:
    with pytest.raises(ValidationError, match="credential-free HTTPS"):
        ProviderProfileConfig.model_validate(
            {
                "id": "profile-a",
                "provider": "provider",
                "model": "model",
                "api_base_url": "http://user@example.com/v1",
                "launch_mode": "api",
                "credential_reference": "env:KEY",
                "capabilities": [],
                "concurrency_limit": 1,
                "cost_class": "balanced",
                "supported_task_types": ["general"],
            }
        )

    for unsafe_url in (
        "https://example.com/v1?token=x",
        "https://localhost/v1",
        "https://127.0.0.1/v1",
    ):
        with pytest.raises(ValidationError, match="provider API base URL"):
            ProviderProfileConfig.model_validate(
                {
                    "id": "profile-a",
                    "provider": "provider",
                    "model": "model",
                    "api_base_url": unsafe_url,
                    "launch_mode": "api",
                    "credential_reference": "env:KEY",
                    "capabilities": [],
                    "concurrency_limit": 1,
                    "cost_class": "balanced",
                    "supported_task_types": ["general"],
                }
            )


def test_unknown_capability_is_rejected() -> None:
    with pytest.raises(ValidationError, match="Input should be"):
        ProviderProfileConfig.model_validate(
            {
                "id": "profile-a",
                "provider": "provider",
                "model": "model",
                "launch_mode": "api",
                "credential_reference": "env:KEY",
                "capabilities": ["invented_capability"],
                "concurrency_limit": 1,
                "cost_class": "balanced",
                "supported_task_types": ["general"],
            }
        )


def test_cli_profile_requires_isolated_identity_and_absolute_state_directory() -> None:
    with pytest.raises(ValidationError, match="requires runtime_identity"):
        ProviderProfileConfig.model_validate(
            {
                "id": "codex-a",
                "provider": "codex",
                "model": "codex",
                "launch_mode": "cli",
                "credential_required": False,
                "capabilities": ["repository_read"],
                "concurrency_limit": 1,
                "cost_class": "strong",
                "supported_task_types": ["coding"],
            }
        )
    with pytest.raises(ValidationError, match="must be absolute"):
        ProviderProfileConfig.model_validate(
            {
                "id": "codex-a",
                "provider": "codex",
                "model": "codex",
                "launch_mode": "cli",
                "credential_required": False,
                "capabilities": ["repository_read"],
                "concurrency_limit": 1,
                "cost_class": "strong",
                "supported_task_types": ["coding"],
                "runtime_identity": "codex-a",
                "state_directory": "relative",
            }
        )


def test_explicit_zero_pricing_requires_nonzero_quota_charge() -> None:
    with pytest.raises(ValidationError, match="zero pricing"):
        ProviderProfileConfig.model_validate(
            {
                "id": "free-api",
                "provider": "openai-compatible",
                "model": "model",
                "api_base_url": "https://provider.example/v1",
                "launch_mode": "api",
                "credential_required": False,
                "capabilities": [],
                "concurrency_limit": 1,
                "cost_class": "cheap",
                "supported_task_types": ["general"],
                "input_cost_units_per_million": 0,
                "output_cost_units_per_million": 0,
            }
        )


def test_topic_project_scope_is_strict() -> None:
    with pytest.raises(ValidationError, match="requires project_id"):
        TopicConfig.model_validate(
            {
                "chat_id": -1,
                "message_thread_id": 1,
                "kind": "project",
                "default_workflow": "coding_task",
            }
        )

    with pytest.raises(ValidationError, match="only project topics"):
        TopicConfig.model_validate(
            {
                "chat_id": -1,
                "message_thread_id": 1,
                "kind": "inbox",
                "project_id": "example",
                "default_workflow": "simple_model_task",
            }
        )


def test_registry_model_does_not_accept_untyped_extra_fields() -> None:
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        NetworkPolicy.model_validate({"enabled": False, "unknown": Path("value")})
