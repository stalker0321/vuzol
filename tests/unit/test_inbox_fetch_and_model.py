"""Inbox links are read safely; the model client sends images and repairs bad JSON."""

import json

import httpx
import pytest
from pydantic import SecretStr

from vuzol.inbox import fetch
from vuzol.inbox.fetch import describe_link, page_text
from vuzol.inbox.model import Image, InboxModel, InboxModelError, ModelChain
from vuzol.inbox.schemas import Answer

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def public_dns(monkeypatch):
    async def public(url):
        if "127.0.0.1" in url or "internal" in url:
            raise fetch.FetchError("private address")

    monkeypatch.setattr(fetch, "_public", public)


async def test_private_addresses_are_refused_even_after_a_redirect(public_dns):
    def handler(request):
        if request.url.host == "short.example":
            return httpx.Response(302, headers={"location": "http://internal.local/admin"})
        return httpx.Response(200, text="secret")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        text = await describe_link("https://short.example/x", client)
    assert "не удалось открыть" in text and "secret" not in text


async def test_real_guard_rejects_loopback_and_odd_ports():
    with pytest.raises(fetch.FetchError):
        await fetch._public("http://127.0.0.1/")
    with pytest.raises(fetch.FetchError):
        await fetch._public("https://example.com:8443/")
    with pytest.raises(fetch.FetchError):
        await fetch._public("file:///etc/passwd")


def test_page_text_keeps_title_meta_and_body_without_scripts():
    html = (
        "<html><head><title>Rave X</title><meta property='og:description' content='15 ноября'>"
        "<script>steal()</script></head><body><nav>menu</nav><p>Арена, 23:00</p></body></html>"
    )
    text = page_text(html)
    assert "title: Rave X" in text and "og:description: 15 ноября" in text
    assert "Арена, 23:00" in text and "steal" not in text and "menu" not in text


async def test_github_repo_reads_facts_readme_and_install_files(public_dns):
    def handler(request):
        path = request.url.path
        if path == "/repos/acme/kit":
            return httpx.Response(
                200,
                json={
                    "full_name": "acme/kit",
                    "stargazers_count": 1200,
                    "license": {"spdx_id": "MIT"},
                    "pushed_at": "2026-10-30T00:00:00Z",
                    "default_branch": "main",
                },
            )
        if path == "/repos/acme/kit/contents":
            return httpx.Response(200, json=[{"name": "install.sh"}, {"name": "README.md"}])
        if path == "/repos/acme/kit/readme":
            return httpx.Response(200, text="# Kit\nAgent tools")
        if path == "/acme/kit/main/install.sh":
            return httpx.Response(200, text="curl https://x | sh")
        return httpx.Response(404)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        text = await describe_link("https://github.com/acme/kit", client)
    assert '"stars": 1200' in text and '"license": "MIT"' in text
    assert "Agent tools" in text and "curl https://x | sh" in text


async def test_model_sends_images_and_repairs_one_bad_answer():
    bodies = []
    replies = iter(["not json", '```json\n{"text": "Готово"}\n```'])

    def handler(request):
        bodies.append(json.loads(request.content))
        content = next(replies)
        return httpx.Response(200, json={"choices": [{"message": {"content": content}}]})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        model = InboxModel(
            base_url="https://llm.example/v1", credential=SecretStr("k"), model="m", client=client
        )
        answer = await model.ask(Answer, system="s", user="u", images=(Image(b"jpg"),))
    assert answer.text == "Готово"
    first = bodies[0]["messages"][1]["content"]
    assert first[1]["image_url"]["url"].startswith("data:image/jpeg;base64,")
    assert len(bodies[1]["messages"]) == 4  # the repair turn


async def test_model_http_failure_is_an_inbox_error():
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(429))
    ) as client:
        model = InboxModel(
            base_url="https://llm.example/v1", credential=SecretStr("k"), model="m", client=client
        )
        with pytest.raises(InboxModelError, match="http_429"):
            await model.ask(Answer, system="s", user="u")


async def test_chain_moves_to_the_next_account_on_quota_but_not_on_a_bad_answer():
    calls = []

    def account(name, status, content='{"text": "ok"}'):
        def handler(request):
            calls.append(name)
            return httpx.Response(status, json={"choices": [{"message": {"content": content}}]})

        return httpx.AsyncClient(transport=httpx.MockTransport(handler))

    def model(client):
        return InboxModel(
            base_url="https://llm.example/v1", credential=SecretStr("k"), model="m", client=client
        )

    async with account("c", 429) as c, account("a", 200) as a:
        assert (await ModelChain([model(c), model(a)]).ask(Answer, system="s", user="u")).text
    assert calls == ["c", "a"]
    calls.clear()
    async with account("c", 200, "nope") as c, account("a", 200) as a:
        with pytest.raises(InboxModelError, match="invalid_model_output"):
            await ModelChain([model(c), model(a)]).ask(Answer, system="s", user="u")
    assert calls == ["c", "c"]
