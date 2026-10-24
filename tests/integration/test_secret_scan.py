import subprocess
import sys
from pathlib import Path
from shutil import which


def test_secret_scanner_rejects_fixture(tmp_path: Path) -> None:
    secret_file = tmp_path / "secret.txt"
    secret_file.write_text("AKIAIOSFODNN7EXAMPLE\n")  # pragma: allowlist secret
    # The hook ships with the dev environment; look next to this interpreter
    # first so the test also works without an activated virtualenv.
    local = Path(sys.executable).parent / "detect-secrets-hook"
    scanner = str(local) if local.exists() else which("detect-secrets-hook")
    assert scanner is not None

    result = subprocess.run(
        [scanner, str(secret_file)],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )

    assert result.returncode != 0
    assert "AWS Access Key" in result.stdout
