"""Interpreter attempt lineage is accounted per attempt, including failures."""

from __future__ import annotations

from vuzol.interpretation.service import interpret_with_recovery

from ._test_interpretation_helpers import (
    FakeInterpreter,
    InterpreterUnavailable,
    InvalidInterpreterOutput,
    asyncio,
    draft,
    request,
    result,
)


def test_primary_repair_lineage_is_observed() -> None:
    async def scenario() -> None:
        primary = FakeInterpreter([InvalidInterpreterOutput("bad"), result(draft())])
        observed: list[dict[str, object]] = []

        async def observer(**kwargs: object) -> None:
            observed.append(kwargs)

        await interpret_with_recovery(primary, (), request(), on_attempt=observer)

        assert [item["attempt_kind"] for item in observed] == ["initial", "repair"]
        assert [item["outcome"] for item in observed] == ["invalid_output", "succeeded"]

    asyncio.run(scenario())


def test_fallback_lineage_is_observed() -> None:
    async def scenario() -> None:
        primary = FakeInterpreter([InterpreterUnavailable("down")])
        fallback = FakeInterpreter([result(draft(), profile="fallback")])
        observed: list[dict[str, object]] = []

        async def observer(**kwargs: object) -> None:
            observed.append(kwargs)

        await interpret_with_recovery(
            primary, (fallback,), request(), on_attempt=observer
        )

        assert [item["attempt_kind"] for item in observed] == ["initial", "retry"]
        assert [item["outcome"] for item in observed] == ["unavailable", "succeeded"]

    asyncio.run(scenario())


def test_repair_failure_then_fallback_is_observed() -> None:
    async def scenario() -> None:
        primary = FakeInterpreter(
            [InvalidInterpreterOutput("bad"), InterpreterUnavailable("still bad")]
        )
        fallback = FakeInterpreter([result(draft(), profile="fallback")])
        observed: list[dict[str, object]] = []

        async def observer(**kwargs: object) -> None:
            observed.append(kwargs)

        await interpret_with_recovery(
            primary, (fallback,), request(), on_attempt=observer
        )

        assert [item["outcome"] for item in observed] == [
            "invalid_output",
            "repair_failed",
            "succeeded",
        ]

    asyncio.run(scenario())
