"""One OpenAI-compatible chat call that may carry images and must return JSON."""

from __future__ import annotations

import base64
import json
import re
from dataclasses import dataclass
from typing import Any, TypeVar

import httpx
from pydantic import BaseModel, SecretStr, ValidationError

M = TypeVar("M", bound=BaseModel)

_FENCE = re.compile(r"^\s*```(?:json)?\s*(.*?)\s*```\s*$", re.DOTALL)


class InboxModelError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class Image:
    content: bytes
    mime: str = "image/jpeg"

    def data_url(self) -> str:
        return f"data:{self.mime};base64,{base64.b64encode(self.content).decode()}"


class InboxModel:
    def __init__(
        self,
        *,
        base_url: str,
        credential: SecretStr,
        model: str,
        timeout_seconds: float = 120,
        output_tokens: int = 4_000,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._credential = credential
        self.model = model
        self._timeout = timeout_seconds
        self._output_tokens = output_tokens
        self._client = client

    async def ask(
        self,
        output: type[M],
        *,
        system: str,
        user: str,
        images: tuple[Image, ...] = (),
    ) -> M:
        """Return the model's JSON as ``output``; one repair turn on a bad shape."""
        content: Any = user
        if images:
            content = [{"type": "text", "text": user}] + [
                {"type": "image_url", "image_url": {"url": image.data_url()}} for image in images
            ]
        messages: list[dict[str, Any]] = [
            {
                "role": "system",
                "content": system
                + "\n\nReturn only one JSON object matching this JSON schema:\n"
                + json.dumps(output.model_json_schema(), ensure_ascii=False),
            },
            {"role": "user", "content": content},
        ]
        reply = await self._complete(messages)
        try:
            return output.model_validate_json(_unfenced(reply))
        except ValidationError as error:
            messages += [
                {"role": "assistant", "content": reply},
                {
                    "role": "user",
                    "content": "That JSON does not match the schema: "
                    + str(error)[:1500]
                    + "\nReturn the corrected JSON object only.",
                },
            ]
            try:
                return output.model_validate_json(_unfenced(await self._complete(messages)))
            except ValidationError as again:
                raise InboxModelError("invalid_model_output") from again

    async def _complete(self, messages: list[dict[str, Any]]) -> str:
        payload = {
            "model": self.model,
            "messages": messages,
            "response_format": {"type": "json_object"},
            "max_tokens": self._output_tokens,
        }
        headers = {"Authorization": f"Bearer {self._credential.get_secret_value()}"}
        try:
            if self._client is not None:
                response = await self._client.post(
                    f"{self._base_url}/chat/completions",
                    json=payload,
                    headers=headers,
                    timeout=self._timeout,
                )
            else:
                async with httpx.AsyncClient() as client:
                    response = await client.post(
                        f"{self._base_url}/chat/completions",
                        json=payload,
                        headers=headers,
                        timeout=self._timeout,
                    )
            response.raise_for_status()
            text = response.json()["choices"][0]["message"]["content"]
        except httpx.HTTPStatusError as error:
            raise InboxModelError(f"http_{error.response.status_code}") from error
        except (httpx.HTTPError, KeyError, IndexError, TypeError, ValueError) as error:
            raise InboxModelError(type(error).__name__) from error
        if not isinstance(text, str) or not text.strip():
            raise InboxModelError("empty_model_output")
        return text


def _unfenced(text: str) -> str:
    match = _FENCE.match(text)
    return match.group(1) if match else text
