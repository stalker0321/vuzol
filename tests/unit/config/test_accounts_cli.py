import json
from pathlib import Path

from tests.unit.config.test_account_descriptors import document
from tests.unit.providers.test_opencode_usage import account

from vuzol.cli.accounts import run


def test_cli_uses_real_compiler_and_never_exposes_key(tmp_path: Path, capsys) -> None:
    account(tmp_path)
    assert run(["check", "--registry", str(document(tmp_path))]) == 0
    text = capsys.readouterr().out
    value = json.loads(text)
    assert len(value["accounts"]) == 1
    assert len(value["accounts"][0]["profiles"]) == 2
    assert value["network_requests"] == 0
    assert "offline-fixture-only" not in text
    assert "not verified" in value["accounts"][0]["auth_health"]


def test_cli_validation_errors_do_not_echo_config(tmp_path: Path, capsys) -> None:
    path = tmp_path / "bad.json"
    path.write_text('{"accounts":[{"secret":"never-echo-this"}]}')
    assert run(["list", "--registry", str(path)]) == 1
    assert capsys.readouterr().out == '{"error": "account_configuration_invalid"}\n'


def test_usage_is_one_explicit_account_with_real_http_boundary(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    import httpx
    from tests.unit.providers.test_opencode_oauth import (
        oauth_account,
        oauth_document,
        oauth_payload,
    )
    from tests.unit.providers.test_opencode_usage import collector

    binding = oauth_account(tmp_path)
    path = oauth_document(tmp_path, binding)
    calls = []

    def transport(request):
        calls.append(request)
        return httpx.Response(200, json=oauth_payload())

    monkeypatch.setattr("vuzol.providers.opencode_usage.go_usage_collector", collector(transport))
    assert run(["usage", "--registry", str(path)]) == 1
    assert not calls
    capsys.readouterr()
    assert run(["usage", "--registry", str(path), "--account", "go-c"]) == 0
    text = capsys.readouterr().out
    value = json.loads(text)
    assert value["source"] == "opencode_go_oauth_v1"
    assert len(value["windows"]) == 3 and value["plan"]["tier_id"] == "go"
    assert len(calls) == 1 and calls[0].headers["x-opencode-org-id"] == "org-c"
    assert "synthetic-access" not in text and "refresh-token" not in text


def test_usage_disabled_account_is_not_enabled_implicitly(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    import httpx
    from tests.unit.providers.test_opencode_oauth import oauth_account, oauth_document

    binding = oauth_account(tmp_path).model_copy(update={"lifecycle": "retired"})
    path = oauth_document(tmp_path, binding)
    monkeypatch.setattr(
        httpx.Client, "send", lambda *a, **kw: (_ for _ in ()).throw(AssertionError("no HTTP"))
    )
    assert run(["usage", "--registry", str(path), "--account", "go-c"]) == 1
    assert json.loads(capsys.readouterr().out)["error"] == "explicit_enabled_go_account_required"
