"""Read-only lead tools. Provider output is untrusted until validated here."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter


class _Object(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, str_strip_whitespace=True)


class RecallArgs(_Object):
    query: str = Field(min_length=1, max_length=200)
    project: Literal["this", "all"] = "this"
    limit: int = Field(default=8, ge=1, le=8)


class ReadCall(_Object):
    name: Literal["read_status", "read_result"]
    args: _Object


class RecallCall(_Object):
    name: Literal["recall"]
    args: RecallArgs


class ToolBatch(_Object):
    # Validate the envelope at the provider boundary, individual calls here.
    # Otherwise adapters reject malformed calls before we can return tool errors.
    tool_calls: list[Any] = Field(
        min_length=1,
        description="Read calls: {name: read_status|read_result, args: {}} or "
        "{name: recall, args: {query: text (1-200 chars), project: this|all, limit: 1-8}}. "
        "project defaults to this; limit defaults to 8.",
    )


def parse_tool_call(raw: object) -> ReadCall | RecallCall:
    return TypeAdapter(ReadCall | RecallCall).validate_python(raw)
