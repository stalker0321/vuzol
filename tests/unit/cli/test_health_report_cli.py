import pytest

from vuzol.cli import health_report as cli


def test_dry_run_prints_and_never_sends(monkeypatch, capsys) -> None:
    async def report(**kwargs):
        assert kwargs["services"] == cli.SERVICES
        return "🟢 ok"

    async def send(_text):
        raise AssertionError("dry run must not send")

    monkeypatch.setattr(cli, "_report", report)
    monkeypatch.setattr(cli, "_send", send)
    cli.main(["--dry-run"])
    assert capsys.readouterr().out == "🟢 ok\n"


@pytest.mark.parametrize("status", [200, 400])
def test_send_posts_to_the_system_topic_and_hides_the_token(monkeypatch, status) -> None:
    import httpx

    sent = []

    def respond(request: httpx.Request) -> httpx.Response:
        sent.append(request)
        return httpx.Response(status, json={"ok": status == 200})

    real_client = httpx.AsyncClient
    monkeypatch.setattr(
        "vuzol.cli.health_report.httpx.AsyncClient",
        lambda **kwargs: real_client(transport=httpx.MockTransport(respond), **kwargs),
    )
    monkeypatch.setenv("VUZOL_TELEGRAM_BOT_TOKEN", "secret-token")
    monkeypatch.setenv("VUZOL_HEALTH_CHAT_ID", "-100")
    monkeypatch.setenv("VUZOL_HEALTH_THREAD_ID", "9")

    async def report(**_kwargs):
        return "🔴 disk"

    monkeypatch.setattr(cli, "_report", report)
    if status == 200:
        cli.main(["--service", "x.service"])
    else:
        with pytest.raises(SystemExit) as raised:
            cli.main([])
        assert "secret-token" not in str(raised.value)
    body = sent[0].read().decode()
    assert '"message_thread_id":9' in body.replace(" ", "")
    assert '"disable_notification":false' in body.replace(" ", "")
