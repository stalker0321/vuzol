import hashlib
import io
import json
import tarfile
import uuid
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from vuzol.config import CapabilityProvisioningSettings
from vuzol.projects.capability_provisioning import (
    CAPABILITY_APPROVAL_SCHEMA,
    CAPABILITY_BUNDLE_SCHEMA,
    CapabilityBundle,
    CapabilityProvisioningError,
    CapabilityProvisioningHandler,
    OfflineCapabilityInstaller,
    _bundle_from_envelope,
    _requirements,
    _verify_environment,
)
from vuzol.storage.types import ApprovalStatus, StepStatus
from vuzol.workflows.domain import OutcomeKind
from vuzol.workflows.ports import CancellationContext, StepExecutionRequest
from vuzol.workflows.result_approval import envelope_hash


class AsyncContext:
    def __init__(self, value: object) -> None:
        self.value = value

    async def __aenter__(self) -> Any:
        return self.value

    async def __aexit__(self, *_args: object) -> None:
        return None


def _settings(tmp_path: Path, *, enabled: bool = True) -> CapabilityProvisioningSettings:
    bundle_root = tmp_path / "bundles"
    toolchain_root = tmp_path / "toolchains"
    bundle_root.mkdir(parents=True)
    toolchain_root.mkdir(parents=True)
    return CapabilityProvisioningSettings(
        enabled=enabled,
        bundle_root=bundle_root,
        toolchain_root=toolchain_root,
    )


def _android_bundle(
    settings: CapabilityProvisioningSettings,
    *,
    unsafe: str | None = None,
    omit: str | None = None,
) -> str:
    archive = settings.bundle_root / "android-sdk.tar"
    members = {
        "android-sdk/platform-tools/adb": b"adb",
        "jdk/bin/java": b"java",
        "gradle/bin/gradle": b"gradle",
    }
    with tarfile.open(archive, "w") as output:
        directory = tarfile.TarInfo("android-sdk")
        directory.type = tarfile.DIRTYPE
        directory.mode = 0o755
        output.addfile(directory)
        for name, content in members.items():
            if name == omit:
                continue
            info = tarfile.TarInfo(name)
            info.size = len(content)
            info.mode = 0o755
            output.addfile(info, io.BytesIO(content))
        if unsafe is not None:
            info = tarfile.TarInfo(unsafe)
            info.size = 1
            output.addfile(info, io.BytesIO(b"x"))
    archive.chmod(0o644)
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    manifest = settings.bundle_root / "android-sdk.json"
    manifest.write_text(
        json.dumps(
            {
                "schema_version": CAPABILITY_BUNDLE_SCHEMA,
                "capability_key": "android-sdk",
                "version": "35.0-test",
                "archive": archive.name,
                "sha256": digest,
                "executables": {
                    "adb": "android-sdk/platform-tools/adb",
                    "java": "jdk/bin/java",
                    "gradle": "gradle/bin/gradle",
                },
                "environment": {
                    "ANDROID_HOME": "android-sdk",
                    "ANDROID_SDK_ROOT": "android-sdk",
                    "JAVA_HOME": "jdk",
                    "GRADLE_HOME": "gradle",
                },
            }
        )
    )
    manifest.chmod(0o644)
    return digest


def _request() -> StepExecutionRequest:
    return cast(
        StepExecutionRequest,
        SimpleNamespace(
            task_id=uuid.uuid4(),
            run_id=uuid.uuid4(),
            step_id=uuid.uuid4(),
            lease=SimpleNamespace(owner="worker", generation=1),
        ),
    )


def _state(
    request: StepExecutionRequest, *, provisioning: str = "automatic"
) -> tuple[SimpleNamespace, SimpleNamespace, SimpleNamespace]:
    task = SimpleNamespace(id=request.task_id, project_id="android-project")
    step = SimpleNamespace(
        id=request.step_id,
        run_id=request.run_id,
        status=StepStatus.RUNNING,
        lease_owner="worker",
        lease_generation=1,
        payload={},
        external_idempotency_key=None,
    )
    environment = SimpleNamespace(
        id=uuid.uuid4(),
        project_id="android-project",
        content_hash="a" * 64,
        contract={
            "capabilities": {
                "android-sdk": {
                    "label": "Android SDK",
                    "provisioning": provisioning,
                }
            }
        },
    )
    return task, step, environment


def test_offline_android_bundle_is_verified_installed_and_idempotent(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    digest = _android_bundle(settings)
    installer = OfflineCapabilityInstaller(settings)

    bundle = installer.inspect_bundle("android-sdk")
    assert bundle.archive_sha256 == digest
    assert not installer.ready("android-sdk")

    installer.install(bundle)
    installer.install(bundle)

    assert installer.ready("android-sdk")
    assert (settings.toolchain_root / "android-sdk/jdk/bin/java").stat().st_mode & 0o111


def test_manifest_installs_a_new_toolchain_without_a_registered_adapter(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    archive = settings.bundle_root / "rust-toolchain.tar"
    with tarfile.open(archive, "w") as output:
        for name, content in (("bin/cargo", b"cargo"), ("bin/rustc", b"rustc")):
            info = tarfile.TarInfo(name)
            info.size = len(content)
            info.mode = 0o755
            output.addfile(info, io.BytesIO(content))
        cargo_home = tarfile.TarInfo("cargo-home")
        cargo_home.type = tarfile.DIRTYPE
        cargo_home.mode = 0o755
        output.addfile(cargo_home)
    archive.chmod(0o644)
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    manifest = settings.bundle_root / "rust-toolchain.json"
    manifest.write_text(
        json.dumps(
            {
                "schema_version": CAPABILITY_BUNDLE_SCHEMA,
                "capability_key": "rust-toolchain",
                "version": "1.89.0",
                "archive": archive.name,
                "sha256": digest,
                "executables": {"cargo": "bin/cargo", "rustc": "bin/rustc"},
                "environment": {"CARGO_HOME": "cargo-home"},
            }
        )
    )
    manifest.chmod(0o644)
    installer = OfflineCapabilityInstaller(settings)

    installer.install(installer.inspect_bundle("rust-toolchain"))

    assert installer.ready("rust-toolchain")


@pytest.mark.parametrize("unsafe", ("../escape", "/absolute"))
def test_offline_bundle_rejects_escaping_archive_entries(tmp_path: Path, unsafe: str) -> None:
    settings = _settings(tmp_path)
    _android_bundle(settings, unsafe=unsafe)
    installer = OfflineCapabilityInstaller(settings)

    with pytest.raises(CapabilityProvisioningError, match="unsafe entries"):
        installer.install(installer.inspect_bundle("android-sdk"))


def test_offline_bundle_is_default_off_allowlisted_and_hash_bound(tmp_path: Path) -> None:
    disabled = _settings(tmp_path / "disabled", enabled=False)
    with pytest.raises(CapabilityProvisioningError, match="disabled"):
        OfflineCapabilityInstaller(disabled).inspect_bundle("android-sdk")

    settings = _settings(tmp_path / "enabled")
    _android_bundle(settings)
    installer = OfflineCapabilityInstaller(settings)
    restricted = settings.model_copy(update={"allowed_capabilities": ("android-sdk",)})
    with pytest.raises(CapabilityProvisioningError, match="allowlist"):
        OfflineCapabilityInstaller(restricted).inspect_bundle("unknown-sdk")
    archive = settings.bundle_root / "android-sdk.tar"
    archive.write_bytes(archive.read_bytes() + b"tampered")
    with pytest.raises(CapabilityProvisioningError, match="hash"):
        installer.inspect_bundle("android-sdk")


def test_installer_probe_rejects_host_android_tools_and_unknown_capabilities(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = _settings(tmp_path)
    installer = OfflineCapabilityInstaller(settings)
    monkeypatch.setattr(
        "vuzol.projects.capability_provisioning.shutil.which",
        lambda executable: f"/usr/bin/{executable}",
    )

    assert installer.installation_root == settings.toolchain_root
    assert not installer.ready("android-sdk")
    assert not installer.ready("unknown-sdk")


@pytest.mark.parametrize("failure", ("json", "schema", "target", "fields", "empty"))
def test_bundle_manifest_rejects_invalid_metadata(tmp_path: Path, failure: str) -> None:
    settings = _settings(tmp_path)
    _android_bundle(settings)
    manifest = settings.bundle_root / "android-sdk.json"
    raw = json.loads(manifest.read_text())
    if failure == "json":
        manifest.write_text("{")
    elif failure == "schema":
        raw["schema_version"] = "future.v9"
        manifest.write_text(json.dumps(raw))
    elif failure == "target":
        raw["capability_key"] = "other-sdk"
        manifest.write_text(json.dumps(raw))
    elif failure == "fields":
        raw["archive"] = "../escape.tar"
        manifest.write_text(json.dumps(raw))
    else:
        archive = settings.bundle_root / "android-sdk.tar"
        archive.write_bytes(b"")
        raw["sha256"] = hashlib.sha256(b"").hexdigest()
        manifest.write_text(json.dumps(raw))
    manifest.chmod(0o644)

    with pytest.raises(CapabilityProvisioningError):
        OfflineCapabilityInstaller(settings).inspect_bundle("android-sdk")


def test_bundle_files_must_exist_and_be_non_writable(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    _android_bundle(settings)
    manifest = settings.bundle_root / "android-sdk.json"
    manifest.chmod(0o666)
    with pytest.raises(CapabilityProvisioningError, match="not trusted"):
        OfflineCapabilityInstaller(settings).inspect_bundle("android-sdk")
    manifest.unlink()
    with pytest.raises(CapabilityProvisioningError, match="unavailable"):
        OfflineCapabilityInstaller(settings).inspect_bundle("android-sdk")


def test_install_rejects_changed_incomplete_and_missing_toolchain(tmp_path: Path) -> None:
    changed_settings = _settings(tmp_path / "changed")
    _android_bundle(changed_settings)
    changed = OfflineCapabilityInstaller(changed_settings)
    bundle = changed.inspect_bundle("android-sdk")
    with pytest.raises(CapabilityProvisioningError, match="changed after approval"):
        changed.install(replace(bundle, archive_sha256="f" * 64))

    incomplete_settings = _settings(tmp_path / "incomplete")
    _android_bundle(incomplete_settings)
    incomplete = OfflineCapabilityInstaller(incomplete_settings)
    (incomplete_settings.toolchain_root / "android-sdk").mkdir()
    with pytest.raises(CapabilityProvisioningError, match="needs cleanup"):
        incomplete.install(incomplete.inspect_bundle("android-sdk"))

    missing_settings = _settings(tmp_path / "missing")
    _android_bundle(missing_settings, omit="gradle/bin/gradle")
    missing = OfflineCapabilityInstaller(missing_settings)
    with pytest.raises(CapabilityProvisioningError, match="missing required executable"):
        missing.install(missing.inspect_bundle("android-sdk"))


def test_install_rejects_excessive_or_invalid_tar(tmp_path: Path) -> None:
    many_settings = _settings(tmp_path / "many")
    _android_bundle(many_settings)
    bounded = many_settings.model_copy(update={"maximum_files": 2})
    with pytest.raises(CapabilityProvisioningError, match="too many"):
        OfflineCapabilityInstaller(bounded).install(
            OfflineCapabilityInstaller(bounded).inspect_bundle("android-sdk")
        )

    invalid_settings = _settings(tmp_path / "invalid")
    _android_bundle(invalid_settings)
    archive = invalid_settings.bundle_root / "android-sdk.tar"
    archive.write_bytes(b"not a tar archive")
    archive.chmod(0o644)
    manifest = invalid_settings.bundle_root / "android-sdk.json"
    raw = json.loads(manifest.read_text())
    raw["sha256"] = hashlib.sha256(archive.read_bytes()).hexdigest()
    manifest.write_text(json.dumps(raw))
    manifest.chmod(0o644)
    invalid = OfflineCapabilityInstaller(invalid_settings)
    with pytest.raises(CapabilityProvisioningError, match="extraction failed"):
        invalid.install(invalid.inspect_bundle("android-sdk"))


@pytest.mark.anyio
async def test_missing_android_bundle_requests_separate_hash_bound_approval(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = _settings(tmp_path)
    digest = _android_bundle(settings)
    installer = MagicMock(spec=OfflineCapabilityInstaller)
    installer.installation_root = settings.toolchain_root
    installer.ready.return_value = False
    installer.inspect_bundle.return_value = CapabilityBundle(
        "android-sdk",
        settings.bundle_root / "android-sdk.tar",
        digest,
        123,
        "35.0-test",
        (
            ("adb", "android-sdk/platform-tools/adb"),
            ("gradle", "gradle/bin/gradle"),
            ("java", "jdk/bin/java"),
        ),
    )
    request = _request()
    task, step, environment = _state(request)
    read = MagicMock()
    read.get = AsyncMock(side_effect=(task, step))
    read.scalar = AsyncMock(return_value=None)
    write = MagicMock()
    write.scalar = AsyncMock(side_effect=(step, None))
    write.flush = AsyncMock()
    factory = MagicMock(return_value=AsyncContext(read))
    factory.begin.return_value = AsyncContext(write)
    monkeypatch.setattr(
        "vuzol.projects.capability_provisioning.current_environment",
        AsyncMock(return_value=environment),
    )
    handler = CapabilityProvisioningHandler(cast(Any, factory), installer)

    outcome = await handler.execute(request, CancellationContext())

    assert outcome.kind is OutcomeKind.NEEDS_APPROVAL
    approval = write.add.call_args.args[0]
    assert approval.requested_action == "install_capabilities"
    assert step.payload["action_envelope"]["environment_hash"] == "a" * 64
    assert step.payload["action_envelope"]["bundles"][0]["archive_sha256"] == digest


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("ready", "provisioning", "expected_kind", "expected_category"),
    (
        (True, "automatic", OutcomeKind.SUCCEEDED, None),
        (False, "external_setup", OutcomeKind.BLOCKED, "capability_setup_required"),
    ),
)
async def test_capability_handler_ready_and_external_setup_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    ready: bool,
    provisioning: str,
    expected_kind: OutcomeKind,
    expected_category: str | None,
) -> None:
    request = _request()
    task, step, environment = _state(request, provisioning=provisioning)
    read = MagicMock()
    read.get = AsyncMock(side_effect=(task, step))
    read.scalar = AsyncMock(return_value=None)
    factory = MagicMock(return_value=AsyncContext(read))
    installer = MagicMock(spec=OfflineCapabilityInstaller)
    installer.ready.return_value = ready
    installer.installation_root = tmp_path / "toolchains"
    monkeypatch.setattr(
        "vuzol.projects.capability_provisioning.current_environment",
        AsyncMock(return_value=environment),
    )

    outcome = await CapabilityProvisioningHandler(cast(Any, factory), installer).execute(
        request, CancellationContext()
    )

    assert outcome.kind is expected_kind
    assert outcome.category == expected_category
    installer.install.assert_not_called()


@pytest.mark.anyio
async def test_approved_capability_bundle_is_installed_consumed_and_reported(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = _settings(tmp_path)
    digest = _android_bundle(settings)
    bundle = CapabilityBundle(
        "android-sdk",
        settings.bundle_root / "android-sdk.tar",
        digest,
        (settings.bundle_root / "android-sdk.tar").stat().st_size,
        "35.0-test",
        (
            ("adb", "android-sdk/platform-tools/adb"),
            ("gradle", "gradle/bin/gradle"),
            ("java", "jdk/bin/java"),
        ),
    )
    request = _request()
    task, step, environment = _state(request)
    envelope = {
        "schema_version": CAPABILITY_APPROVAL_SCHEMA,
        "requested_action": "install_capabilities",
        "step_id": str(step.id),
        "project_id": environment.project_id,
        "environment_revision_id": str(environment.id),
        "environment_hash": environment.content_hash,
        "installation_root": str(settings.toolchain_root),
        "bundles": [bundle.approval_record()],
    }
    approval = SimpleNamespace(
        id=uuid.uuid4(),
        status=ApprovalStatus.APPROVED,
        action_envelope_hash=envelope_hash(envelope),
        consumed_at=None,
    )
    step.payload = {"action_envelope": envelope}
    read = MagicMock()
    read.get = AsyncMock(side_effect=(task, step))
    read.scalar = AsyncMock(return_value=approval)
    lease_read = MagicMock()
    lease_read.scalar = AsyncMock(return_value=step)
    consume = MagicMock()
    consume.scalar = AsyncMock(side_effect=(step, approval))
    factory = MagicMock(side_effect=(AsyncContext(read), AsyncContext(lease_read)))
    factory.begin.return_value = AsyncContext(consume)
    installer = MagicMock(spec=OfflineCapabilityInstaller)
    installer.installation_root = settings.toolchain_root
    installer.ready.side_effect = (False, True)
    installer.inspect_bundle.return_value = bundle
    monkeypatch.setattr(
        "vuzol.projects.capability_provisioning.current_environment",
        AsyncMock(return_value=environment),
    )

    outcome = await CapabilityProvisioningHandler(cast(Any, factory), installer).execute(
        request, CancellationContext()
    )

    assert outcome.kind is OutcomeKind.SUCCEEDED
    assert outcome.result["status"] == "installed"
    installer.install.assert_called_once_with(bundle)
    assert approval.status is ApprovalStatus.CONSUMED
    assert approval.consumed_at is not None


@pytest.mark.anyio
async def test_pending_capability_approval_waits_without_installing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    request = _request()
    task, step, environment = _state(request)
    approval = SimpleNamespace(id=uuid.uuid4(), status=ApprovalStatus.PENDING)
    read = MagicMock()
    read.get = AsyncMock(side_effect=(task, step))
    read.scalar = AsyncMock(return_value=approval)
    factory = MagicMock(return_value=AsyncContext(read))
    installer = MagicMock(spec=OfflineCapabilityInstaller)
    installer.installation_root = tmp_path / "toolchains"
    installer.ready.return_value = False
    monkeypatch.setattr(
        "vuzol.projects.capability_provisioning.current_environment",
        AsyncMock(return_value=environment),
    )

    outcome = await CapabilityProvisioningHandler(cast(Any, factory), installer).execute(
        request, CancellationContext()
    )

    assert outcome.kind is OutcomeKind.NEEDS_APPROVAL
    installer.install.assert_not_called()


@pytest.mark.anyio
async def test_capability_handler_cancels_before_loading(tmp_path: Path) -> None:
    cancellation = CancellationContext()
    cancellation.request()
    handler = CapabilityProvisioningHandler(
        cast(Any, MagicMock()), MagicMock(spec=OfflineCapabilityInstaller)
    )

    outcome = await handler.execute(_request(), cancellation)

    assert outcome.kind is OutcomeKind.CANCELLED


def test_capability_approval_helpers_reject_stale_or_malformed_evidence(tmp_path: Path) -> None:
    assert _requirements({}) == ()
    assert _requirements({"capabilities": []}) == ()
    assert _requirements({"capabilities": {"bad": [], "git": {"provisioning": "automatic"}}}) == (
        ("git", "git", "automatic"),
    )

    settings = _settings(tmp_path)
    digest = _android_bundle(settings)
    installer = OfflineCapabilityInstaller(settings)
    environment = cast(
        Any,
        SimpleNamespace(id=uuid.uuid4(), project_id="demo", content_hash="a" * 64),
    )
    envelope = {
        "schema_version": CAPABILITY_APPROVAL_SCHEMA,
        "requested_action": "install_capabilities",
        "environment_revision_id": str(environment.id),
        "environment_hash": environment.content_hash,
        "project_id": environment.project_id,
        "installation_root": str(settings.toolchain_root),
    }
    _verify_environment(envelope, environment, installer)
    with pytest.raises(CapabilityProvisioningError, match="environment changed"):
        _verify_environment({**envelope, "environment_hash": "b" * 64}, environment, installer)
    with pytest.raises(CapabilityProvisioningError, match="malformed"):
        _bundle_from_envelope([], installer)
    with pytest.raises(CapabilityProvisioningError, match="no longer matches"):
        _bundle_from_envelope(
            {
                "capability_key": "android-sdk",
                "archive_sha256": digest,
                "archive_bytes": 1,
                "required_paths": [],
            },
            installer,
        )
