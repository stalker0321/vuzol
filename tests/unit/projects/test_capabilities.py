from vuzol.projects.capabilities import (
    CapabilityState,
    blocking_capabilities,
    preflight_capabilities,
)


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
    assert checks[0].detail == "no trusted adapter is registered"


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
