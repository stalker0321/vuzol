import json
from pathlib import Path

from vuzol.projects.capabilities import (
    CapabilityState,
    blocking_capabilities,
    preflight_capabilities,
)
from vuzol.projects.toolchains import TOOLCHAIN_RECEIPT, ToolchainSpec


def test_preflight_classifies_host_external_and_privileged_requirements() -> None:
    contract: dict[str, object] = {
        "capabilities": {
            "node-runtime": {"label": "Node.js", "provisioning": "automatic"},
            "android-sdk": {"label": "Android SDK", "provisioning": "automatic"},
            "production-token": {"label": "Deploy token", "provisioning": "external_setup"},
            "system-service": {"label": "System service", "provisioning": "approval_required"},
        }
    }

    checks = preflight_capabilities(
        contract,
        which=lambda executable: "/usr/bin/node" if executable == "node" else None,
    )

    assert [(check.key, check.state) for check in checks] == [
        ("android-sdk", CapabilityState.NEEDS_SETUP),
        ("node-runtime", CapabilityState.READY),
        ("production-token", CapabilityState.NEEDS_SETUP),
        ("system-service", CapabilityState.NEEDS_APPROVAL),
    ]
    assert [check.key for check in blocking_capabilities(checks)] == [
        "android-sdk",
        "production-token",
        "system-service",
    ]


def test_unknown_automatic_capability_fails_closed() -> None:
    checks = preflight_capabilities(
        {"capabilities": {"magic": {"label": "Magic", "provisioning": "automatic"}}},
        which=lambda _executable: None,
    )

    assert checks[0].state is CapabilityState.NEEDS_SETUP
    assert checks[0].detail == "managed toolchain is not installed"


def test_preflight_accepts_complete_managed_android_toolchain(tmp_path: Path) -> None:
    root = tmp_path / "toolchains"
    for relative in (
        "android-sdk/android-sdk/platform-tools/adb",
        "android-sdk/jdk/bin/java",
        "android-sdk/gradle/bin/gradle",
    ):
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"tool")
        path.chmod(0o555)
    receipt = root / "android-sdk" / TOOLCHAIN_RECEIPT
    receipt.write_text(
        json.dumps(
            ToolchainSpec(
                capability_key="android-sdk",
                version="35.0-test",
                archive_sha256="a" * 64,
                executables=(
                    ("adb", "android-sdk/platform-tools/adb"),
                    ("gradle", "gradle/bin/gradle"),
                    ("java", "jdk/bin/java"),
                ),
            ).receipt()
        )
    )
    receipt.chmod(0o444)

    checks = preflight_capabilities(
        {"capabilities": {"android-sdk": {"label": "Android SDK", "provisioning": "automatic"}}},
        which=lambda _executable: None,
        managed_toolchain_root=root,
    )

    assert checks[0].state is CapabilityState.READY
    assert checks[0].detail == "managed android-sdk 35.0-test is available"


def test_preflight_does_not_expose_host_android_tools_to_artifact_sandbox() -> None:
    checks = preflight_capabilities(
        {"capabilities": {"android-sdk": {"label": "Android SDK"}}},
        which=lambda executable: f"/usr/bin/{executable}",
    )

    assert checks[0].state is CapabilityState.NEEDS_SETUP
    assert checks[0].detail == "managed toolchain is not installed"


def test_preflight_ignores_malformed_contract_and_defaults_label() -> None:
    assert preflight_capabilities({}) == ()
    assert preflight_capabilities({"capabilities": []}) == ()

    checks = preflight_capabilities(
        {
            "capabilities": {
                "bad": [],
                "git": {"provisioning": "automatic"},
            }
        },
        which=lambda executable: f"/usr/bin/{executable}",
    )

    assert len(checks) == 1
    assert checks[0].label == "git"
    assert checks[0].state is CapabilityState.READY
