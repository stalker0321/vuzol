"""WP06: research report flows into the synthesize consumer with provenance (PG)."""

from __future__ import annotations

import hashlib
import json
import uuid
from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tests.integration.storage.helpers import seed_task_run_step, storage
from vuzol.context.resolver import (
    RESEARCH_RESULT_SCHEMA,
    RESEARCH_RESULT_SCHEMA_VERSION,
    resolve_context,
)
from vuzol.execution.artifacts import ArtifactStore
from vuzol.research.report import Claim, Source, validate_report
from vuzol.research.retrieval import FixtureRetrieval
from vuzol.research.synthesize import bind_report, build_synthesis_context, sources_from_retrieved
from vuzol.storage.models import InputBinding, Task

pytestmark = [pytest.mark.postgresql, pytest.mark.anyio]


def _report_payload() -> bytes:
    retrieved = FixtureRetrieval(
        fixtures={"fixture://adapter.md": b"CI uses frozen fixtures."}
    ).fetch("fixture://adapter.md", now="2026-09-27T10:00:00Z")
    (source,) = sources_from_retrieved((retrieved,), retriever="local-docs-fixture", scope="vuzol")
    report = bind_report(
        research_id=str(uuid.uuid4()),
        question="Which adapter backs CI retrieval?",
        sources=(source,),
        claims=(
            Claim(
                claim_id="c1",
                statement="CI retrieval is fixture-based.",
                support="supported",
                citations=(("s1", "para 1"),),
            ),
        ),
        created_at="2026-09-27T10:10:00Z",
    )
    assert validate_report(report) == ()
    payload = {
        "schema": "research-result.v1",
        "research_id": report.research_id,
        "question": report.question,
        "sources": [
            {
                "source_id": source.source_id,
                "uri": source.uri,
                "retriever": source.retriever,
                "retrieved_at": source.retrieved_at,
                "content_hash": source.content_hash,
                "scope": source.scope,
                "freshness_class": source.freshness_class,
            }
        ],
        "claims": [
            {
                "claim_id": "c1",
                "statement": "CI retrieval is fixture-based.",
                "support": "supported",
                "citations": [{"source_id": "s1", "position": "para 1"}],
            }
        ],
        "created_at": report.created_at,
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()


async def _persisted_binding(
    factory: async_sessionmaker[AsyncSession],
    store: ArtifactStore,
    payload: bytes,
    *,
    required: bool,
    freshness_max_age_seconds: int | None,
) -> uuid.UUID:
    _task, run_id, step = await seed_task_run_step(factory)
    async with factory.begin() as session:
        task = await session.get(Task, _task.id, with_for_update=True)
        assert task is not None
        task.project_id = "vuzol"
    async with factory.begin() as session:
        artifact = await store.persist(
            session,
            task_id=_task.id,
            run_id=run_id,
            step_id=step.id,
            artifact_type="research_result",
            content=payload,
            media_type="application/json",
        )
        binding = InputBinding(
            consumer_step_id=step.id,
            producer_step_id=step.id,
            artifact_id=artifact.id,
            slot="predecessor_result",
            schema_name=RESEARCH_RESULT_SCHEMA,
            schema_version=RESEARCH_RESULT_SCHEMA_VERSION,
            content_hash=artifact.content_hash,
            scope_project_id="vuzol",
            required=required,
            status="resolved",
            freshness_max_age_seconds=freshness_max_age_seconds,
        )
        session.add(binding)
        await session.flush()
        return step.id


async def test_report_flows_into_synthesize_consumer(postgres_dsn: str, tmp_path: Path) -> None:
    engine, factory = storage(postgres_dsn)
    store = ArtifactStore(tmp_path / "artifacts", max_bytes=10_000_000, retention_days=14)
    payload = _report_payload()
    digest = hashlib.sha256(payload).hexdigest()
    consumer_step_id = await _persisted_binding(
        factory, store, payload, required=True, freshness_max_age_seconds=3600
    )
    async with factory() as session:
        resolved = await resolve_context(
            session, store, consumer_step_id=consumer_step_id, project_id="vuzol"
        )
    assert not resolved.is_empty
    (entry,) = resolved.bindings
    assert entry.content == payload
    assert entry.content_hash == digest
    assert entry.freshness == "fresh"
    assert entry.schema_version == "research-result.v1"
    body = json.loads(entry.content.decode())
    assert body["claims"][0]["citations"] == [{"source_id": "s1", "position": "para 1"}]
    expected_source_hash = hashlib.sha256(b"CI uses frozen fixtures.").hexdigest()
    assert body["sources"][0]["content_hash"] == expected_source_hash
    await engine.dispose()


async def test_stale_research_binding_is_visible_not_verified(
    postgres_dsn: str, tmp_path: Path
) -> None:
    engine, factory = storage(postgres_dsn)
    store = ArtifactStore(tmp_path / "artifacts", max_bytes=10_000_000, retention_days=14)
    payload = _report_payload()
    consumer_step_id = await _persisted_binding(
        factory, store, payload, required=False, freshness_max_age_seconds=0
    )
    async with factory() as session:
        resolved = await resolve_context(
            session, store, consumer_step_id=consumer_step_id, project_id="vuzol"
        )
    assert not resolved.is_empty
    (entry,) = resolved.bindings
    assert entry.freshness == "stale"
    assert entry.content == payload
    await engine.dispose()


def test_synthesis_block_marks_stale_citation() -> None:
    source = Source(
        source_id="s1",
        uri="fixture://adapter.md",
        retriever="local-docs-fixture",
        retrieved_at="2026-09-01T10:00:00Z",
        content="old bytes",
        scope="vuzol",
        freshness_class="stale",
    )
    report = bind_report(
        research_id=str(uuid.uuid4()),
        question="Which adapter backs CI retrieval?",
        sources=(source,),
        claims=(
            Claim(
                claim_id="c1",
                statement="CI retrieval is fixture-based.",
                support="supported",
                citations=(("s1", "para 1"),),
            ),
        ),
        created_at="2026-09-27T10:10:00Z",
    )
    block = build_synthesis_context(report)
    assert "(stale)" in block and "[verified] c1" in block
