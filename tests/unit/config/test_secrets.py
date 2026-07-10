from pathlib import Path

import pytest

from vuzol.config import ScopedSecretResolver, SecretResolutionError


def resolver(tmp_path: Path, *, environment: dict[str, str] | None = None) -> ScopedSecretResolver:
    return ScopedSecretResolver(
        access_policy={
            "env:API_KEY": frozenset({"profile:allowed"}),
            "file:token": frozenset({"profile:file"}),
            "file:../escape": frozenset({"profile:escape"}),
        },
        secret_file_root=tmp_path,
        environment=environment or {},
    )


def test_environment_secret_is_scoped_and_hidden(tmp_path: Path) -> None:
    secret = resolver(tmp_path, environment={"API_KEY": "super-secret-value"}).get(
        "env:API_KEY", "profile:allowed"
    )

    assert secret.get_secret_value() == "super-secret-value"
    assert "super-secret-value" not in str(secret)
    assert "super-secret-value" not in repr(secret)

    with pytest.raises(SecretResolutionError, match="not allowed"):
        resolver(tmp_path, environment={"API_KEY": "value"}).get(  # pragma: allowlist secret
            "env:API_KEY", "profile:other"
        )


def test_missing_and_invalid_secret_references_fail_precisely(tmp_path: Path) -> None:
    configured = resolver(tmp_path)
    with pytest.raises(SecretResolutionError, match="missing environment secret"):
        configured.get("env:API_KEY", "profile:allowed")
    with pytest.raises(SecretResolutionError, match="not allowed"):
        configured.get("vault:item", "profile:allowed")
    with pytest.raises(SecretResolutionError, match="missing environment secret"):
        resolver(tmp_path, environment={"API_KEY": ""}).get("env:API_KEY", "profile:allowed")


def test_secret_files_are_constrained_to_root(tmp_path: Path) -> None:
    (tmp_path / "token").write_text("file-secret\n")
    configured = resolver(tmp_path)

    assert configured.get("file:token", "profile:file").get_secret_value() == "file-secret"
    with pytest.raises(SecretResolutionError, match="escapes configured root"):
        configured.get("file:../escape", "profile:escape")

    (tmp_path / "token").write_text("")
    with pytest.raises(SecretResolutionError, match="empty secret file"):
        configured.get("file:token", "profile:file")
