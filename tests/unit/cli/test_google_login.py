import json
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import httpx

from vuzol.cli import google_login as cli


def client_file(tmp_path: Path) -> Path:
    path = tmp_path / "client.json"
    path.write_text(json.dumps({"installed": {"client_id": "id1", "client_secret": "SECRET"}}))
    return path


def test_first_run_prints_a_consent_link_for_offline_access(tmp_path, capsys):
    assert cli.run(["--client", str(client_file(tmp_path))]) == 0
    query = parse_qs(urlparse(capsys.readouterr().out.strip()).query)
    assert query["client_id"] == ["id1"] and query["access_type"] == ["offline"]
    assert query["scope"][0].split() == list(cli.SCOPES)


def test_the_landing_address_is_exchanged_and_the_token_written_privately(tmp_path, capsys):
    def google(request):
        form = parse_qs(request.content.decode())
        assert form["code"] == ["4/abc"] and form["client_secret"] == ["SECRET"]
        return httpx.Response(200, json={"access_token": "a", "refresh_token": "REFRESH"})

    out = tmp_path / "google" / "token.json"
    code = cli.run(
        [
            "--client", str(client_file(tmp_path)),
            "--code", "http://127.0.0.1:8765/?code=4/abc&scope=x",
            "--out", str(out),
        ],
        client=httpx.Client(transport=httpx.MockTransport(google)),
    )  # fmt: skip
    printed = capsys.readouterr().out
    assert code == 0 and "SECRET" not in printed and "REFRESH" not in printed
    assert json.loads(out.read_text()) == {
        "client_id": "id1",
        "client_secret": "SECRET",
        "refresh_token": "REFRESH",
    }
    assert out.stat().st_mode & 0o777 == 0o600


def test_a_refused_exchange_writes_nothing(tmp_path, capsys):
    out = tmp_path / "token.json"
    code = cli.run(
        ["--client", str(client_file(tmp_path)), "--code", "bad", "--out", str(out)],
        client=httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(400))),
    )
    assert code == 1 and json.loads(capsys.readouterr().out) == {"error": "exchange_400"}
    assert not out.exists()
