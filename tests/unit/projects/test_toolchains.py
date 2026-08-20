import json
from pathlib import Path

import pytest

from vuzol.projects.toolchains import (
    TOOLCHAIN_RECEIPT,
    ToolchainReceiptError,
    ToolchainSpec,
    load_installed_toolchain,
    parse_toolchain_spec,
    toolchain_runtime,
)


def _install(root: Path, spec: ToolchainSpec) -> None:
    target = root / spec.capability_key
    for _command, relative in spec.executables:
        executable = target / relative
        executable.parent.mkdir(parents=True, exist_ok=True)
        executable.write_bytes(b"tool")
        executable.chmod(0o555)
    for _name, relative in spec.environment:
        (target / relative).mkdir(parents=True, exist_ok=True)
    receipt = target / TOOLCHAIN_RECEIPT
    receipt.write_text(json.dumps(spec.receipt()))
    receipt.chmod(0o444)


def test_generic_toolchain_receipt_builds_bounded_sandbox_runtime(tmp_path: Path) -> None:
    root = tmp_path / "toolchains"
    spec = ToolchainSpec(
        capability_key="rust-toolchain",
        version="1.89.0",
        archive_sha256="a" * 64,
        executables=(("cargo", "bin/cargo"), ("rustc", "bin/rustc")),
        environment=(("CARGO_HOME", "cargo-home"),),
    )
    _install(root, spec)

    assert load_installed_toolchain(root, "rust-toolchain") == spec
    runtime = toolchain_runtime(root, ("rust-toolchain",))
    assert dict(runtime.executables) == {
        "cargo": "/toolchains/rust-toolchain/bin/cargo",
        "rustc": "/toolchains/rust-toolchain/bin/rustc",
    }
    assert dict(runtime.environment) == {"CARGO_HOME": "/toolchains/rust-toolchain/cargo-home"}
    assert runtime.path_entries == ("/toolchains/rust-toolchain/bin",)


@pytest.mark.parametrize(
    "field",
    ("unsafe_key", "unsafe_version", "unsafe_command", "unsafe_path", "unsafe_environment"),
)
def test_toolchain_receipt_rejects_unsafe_manifest_fields(field: str) -> None:
    raw: dict[str, object] = {
        "schema_version": "capability-toolchain-receipt.v1",
        "capability_key": "rust-toolchain",
        "version": "1.89.0",
        "archive_sha256": "a" * 64,
        "executables": {"cargo": "bin/cargo"},
        "environment": {"CARGO_HOME": "cargo-home"},
    }
    if field == "unsafe_key":
        raw["capability_key"] = "../rust"
    elif field == "unsafe_version":
        raw["version"] = ""
    elif field == "unsafe_command":
        raw["executables"] = {"../cargo": "bin/cargo"}
    elif field == "unsafe_path":
        raw["executables"] = {"cargo": "../bin/cargo"}
    else:
        raw["environment"] = {"PATH": "bin"}

    with pytest.raises(ToolchainReceiptError):
        parse_toolchain_spec(raw, expected_key="rust-toolchain")


def test_toolchain_runtime_rejects_command_collisions(tmp_path: Path) -> None:
    root = tmp_path / "toolchains"
    for index, key in enumerate(("first-sdk", "second-sdk")):
        _install(
            root,
            ToolchainSpec(
                capability_key=key,
                version="1.0",
                archive_sha256=("a" if index == 0 else "b") * 64,
                executables=(("compiler", "bin/compiler"),),
            ),
        )

    with pytest.raises(ToolchainReceiptError, match="duplicate managed executable"):
        toolchain_runtime(root, ("first-sdk", "second-sdk"))


def test_missing_or_writable_receipt_fails_closed(tmp_path: Path) -> None:
    root = tmp_path / "toolchains"
    spec = ToolchainSpec(
        capability_key="go-toolchain",
        version="1.25.0",
        archive_sha256="b" * 64,
        executables=(("go", "bin/go"),),
    )
    _install(root, spec)
    receipt = root / "go-toolchain" / TOOLCHAIN_RECEIPT
    receipt.chmod(0o666)
    assert load_installed_toolchain(root, "go-toolchain") is None
    receipt.unlink()
    assert load_installed_toolchain(root, "go-toolchain") is None


def test_missing_toolchain_root_is_not_ready(tmp_path: Path) -> None:
    root = tmp_path / "absent"

    assert load_installed_toolchain(root, "go-toolchain") is None
