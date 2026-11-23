import json
from pathlib import Path

import pytest

from vuzol.cli import add_opencode_account as cli
from vuzol.config.loader import load_document
from vuzol.config.registries import ProfileRegistry

KEY = "sk-test-not-a-real-key"


def _registry(tmp_path: Path) -> tuple[Path, Path]:
    state = tmp_path / "state"
    template = state / "pi-opencode-go-a"
    template.mkdir(parents=True)
    for name in ("auth.json", "models.json", "models-store.json"):
        (template / name).write_text("{}")
    registry = tmp_path / "account-profiles.toml"
    registry.write_text(
        f"""[[profiles]]
id = "pi-opencode-go-a"
account_id = "opencode-go-a"
account_pool = "opencode-go"
provider = "pi"
model = "deepseek-v4.1-flash"
launch_mode = "cli"
credential_required = false
capabilities = ["repository_read", "code_edit", "git", "project_shell"]
concurrency_limit = 4
control_reserved_slots = 1
cost_class = "balanced"
roles = ["lead", "planner", "reviewer", "executor"]
routing_priority = 240
supported_task_types = ["coding", "project_lead", "general"]
sandbox_required = true
runtime_identity = "vuzol-pi-a"
state_directory = "{template}"
enabled = true

[profiles.runtime_network]
enabled = true
destinations = [
  {{ url = "https://opencode.ai", purpose = "Pi opencode-go inference" }},
]

[[profiles]]
id = "pi-opencode-go-a-muse"
base_profile_id = "pi-opencode-go-a"
model = "muse-spark-1.3-contributor"
model_reasoning_effort = "high"
routing_priority = 242

[[profiles]]
id = "pi-opencode-go-ab"
base_profile_id = "pi-opencode-go-a"
runtime_identity = "vuzol-pi-ab"
state_directory = "{state}/pi-opencode-go-ab"
account_id = "opencode-go-ab"
roles = ["executor"]
"""
    )
    return registry, state


def _key(tmp_path: Path, value: str = KEY) -> Path:
    path = tmp_path / "key"
    path.write_text(value + "\n")
    return path


def test_a_new_account_gets_its_own_state_and_a_copy_of_the_template_profiles(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    registry, state = _registry(tmp_path)
    before = registry.read_text()
    code = cli.run(
        [
            "--name", "d", "--key-file", str(_key(tmp_path)),
            "--registry", str(registry), "--state-root", str(state),
        ]
    )  # fmt: skip
    output = capsys.readouterr().out
    assert code == 0
    assert KEY not in output
    result = json.loads(output)
    assert result["profiles"] == ["pi-opencode-go-d", "pi-opencode-go-d-muse"]
    assert result["account_pool"] == "opencode-go"
    profiles = {p.id: p for p in ProfileRegistry(load_document(registry).profiles).items()}
    new, muse = profiles["pi-opencode-go-d"], profiles["pi-opencode-go-d-muse"]
    assert (new.account_id, new.runtime_identity) == ("opencode-go-d", "vuzol-pi-d")
    assert new.state_directory == state / "pi-opencode-go-d"
    assert (muse.model, muse.account_pool, muse.roles) == (
        "muse-spark-1.3-contributor",
        "opencode-go",
        profiles["pi-opencode-go-a"].roles,
    )
    # The template and a look-alike name are untouched.
    assert profiles["pi-opencode-go-ab"].account_id == "opencode-go-ab"
    assert registry.read_text().startswith(before)
    assert Path(result["registry_backup"]).read_text() == before
    target = state / "pi-opencode-go-d"
    assert json.loads((target / "auth.json").read_text()) == {
        "opencode-go": {"type": "api_key", "key": KEY}
    }
    assert sorted(p.name for p in target.iterdir()) == [
        "auth.json",
        "models-store.json",
        "models.json",
    ]
    assert (target / "auth.json").stat().st_mode & 0o777 == 0o600
    assert target.stat().st_mode & 0o777 == 0o700
    assert not [p for p in state.iterdir() if p.name.startswith(".")]


@pytest.mark.parametrize(
    ("name", "key", "code"),
    [
        ("a", KEY, "name_invalid"),
        ("D", KEY, "name_invalid"),
        ("ab", KEY, "account_exists"),
        ("d", "two words", "key_invalid"),
        ("d", "", "key_invalid"),
    ],
)
def test_refusals_change_nothing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], name: str, key: str, code: str
) -> None:
    registry, state = _registry(tmp_path)
    before = registry.read_text()
    argv = [
        "--name", name, "--key-file", str(_key(tmp_path, key)),
        "--registry", str(registry), "--state-root", str(state),
    ]  # fmt: skip
    assert cli.run(argv) == 1
    assert json.loads(capsys.readouterr().out) == {"error": code}
    assert registry.read_text() == before
    assert [p.name for p in state.iterdir()] == ["pi-opencode-go-a"]


def test_dry_run_and_existing_state_directory(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    registry, state = _registry(tmp_path)
    before = registry.read_text()
    base = ["--key-file", str(_key(tmp_path)), "--registry", str(registry)]
    assert cli.run(["--name", "d", *base, "--state-root", str(state), "--dry-run"]) == 0
    assert json.loads(capsys.readouterr().out)["dry_run"] is True
    assert registry.read_text() == before
    (state / "pi-opencode-go-e").mkdir()
    assert cli.run(["--name", "e", *base, "--state-root", str(state)]) == 1
    assert json.loads(capsys.readouterr().out) == {"error": "state_directory_exists"}


def test_a_broken_registry_write_leaves_no_state_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry, state = _registry(tmp_path)

    def refuse(*_args: object) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(cli.os, "replace", refuse)
    with pytest.raises(OSError):
        cli.add_account(name="d", key_file=_key(tmp_path), registry=registry, state_root=state)
    assert not (state / "pi-opencode-go-d").exists()
