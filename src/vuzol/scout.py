"""Bounded scout capability mode (D3 W3, REDO-1 repo probes).

A scout is an explicitly bounded evidence-gathering run: question, scope,
declared probes, deadline, max calls and stop condition up front; a typed
immutable packet (complete/partial/failed) out. Packets persist as typed
``Artifact`` bytes plus ``InputBinding`` (Q6 — no separate truth table);
observed revision/time travel inside the packet JSON. Packet-created/partial
moments emit ``Event`` rows instead.

Probe kinds (closed set): ``fetch`` reads through the retrieval seam
(offline fixtures in CI/tests, approved-HTTP only when explicitly configured
— live trials are forbidden); ``repo`` executes read-only repository
operations (``read_file``, ``git_log``) under worktree containment
(``trusted_root`` boundary, fixed command shapes, no network, no writes, no
installs — effect class always read-only). A docker-backed executor can
replace the local one through the ``RepoProbeExecutor`` seam (follow-up);
the sandbox/egress policy union for mutating ops stays deferred per brief.

Every scout call reserves and settles through the shared ledger (W4) —
no owner/reserve/settlement, no probes — so concurrent scouts share the
same capacity gate as everything else. Retry re-runs only missing probes
and supersedes the binding (documented refresh). A partial packet with
missing required probes fails closed at the consumer boundary (facts stay
persisted, the dependent decision is refused).
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.config.models import ProviderProfileConfig
from vuzol.config.settings import HardLimits
from vuzol.execution.artifacts import ArtifactStore
from vuzol.providers.budgets import (
    BudgetExceeded,
    accounting_for_profile,
    estimate_reservation,
    release_reservation_unfenced,
    reserve_invocation_budget,
    settle_invocation_budget,
)
from vuzol.research.retrieval import RetrievedSource
from vuzol.storage.models import Event, InputBinding, Step

SCOUT_PACKET_SCHEMA = "scout-packet.v1"
SCOUT_SLOT = "scout_packet"
# D3 REDO-1: fetch probes read through the retrieval seam; repo probes
# execute read-only repository operations under worktree containment.
# Closed kinds — anything else is an explicit refusal.
SCOUT_PROBE_KINDS = frozenset({"fetch", "repo"})
REPO_PROBE_OPS = frozenset({"read_file", "git_log"})
REPO_PROBE_MAX_BYTES = 65_536
REPO_PROBE_GIT_LINES = 20
SCOUT_STOP_CONDITIONS = frozenset({"all_required", "any_success"})
SCOUT_STATUSES = frozenset({"complete", "partial", "failed"})


class FetchProbe(Protocol):
    """Retrieval seam: fetch one probe URI at a logical time."""

    def __call__(self, uri: str, *, now: str) -> RetrievedSource: ...


@dataclass(frozen=True, slots=True)
class RepoProbeResult:
    observation: str
    evidence_hash: str


class RepoProbeExecutor(Protocol):
    """Repository probe seam: run one closed read-only op in a worktree."""

    async def run(
        self, *, op: str, path: str, root: str, max_bytes: int
    ) -> RepoProbeResult: ...


class ScoutError(RuntimeError):
    """Stable, fail-closed scout rejection."""

    def __init__(self, code: str, message: str | None = None) -> None:
        self.code = code
        super().__init__(message or code)


@dataclass(frozen=True, slots=True)
class ScoutProbe:
    name: str
    kind: str
    uri: str = ""
    required: bool = True
    # Repo-probe fields (kind == "repo" only): closed op + repo-relative path.
    op: str | None = None
    path: str | None = None


@dataclass(frozen=True, slots=True)
class ScoutRequest:
    question: str
    scope: str
    probes: tuple[ScoutProbe, ...]
    deadline: str
    max_calls: int
    stop_condition: str = "all_required"


@dataclass(frozen=True, slots=True)
class ScoutFact:
    probe: str
    observation: str
    source_hash: str
    # Effect-policy class of the probe execution (D3 REDO-1): repo and fetch
    # probes are read-only by construction (no writes, no network beyond the
    # retrieval seam, no installs — procedure approval unbypassable).
    effect_class: str = "read_only"


@dataclass(slots=True)
class ScoutPacket:
    packet_id: str
    request_hash: str
    status: str
    facts: list[ScoutFact]
    unresolved: list[str]
    evidence_hashes: list[str]
    observed_revision: str
    observed_at: str
    scope: str
    required_total: int
    required_done: int


def _canonical_json(payload: object) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def request_hash(request: ScoutRequest) -> str:
    return hashlib.sha256(
        _canonical_json(
            {
                "question": request.question,
                "scope": request.scope,
                "probes": [
                    {
                        "name": probe.name,
                        "kind": probe.kind,
                        "uri": probe.uri,
                        "required": probe.required,
                    }
                    for probe in request.probes
                ],
                "deadline": request.deadline,
                "max_calls": request.max_calls,
                "stop_condition": request.stop_condition,
            }
        ).encode()
    ).hexdigest()


def validate_request(request: ScoutRequest) -> tuple[str, ...]:
    """Fail-closed request validation (unknown kinds refused, never coerced)."""

    errors: list[str] = []
    if not request.question.strip():
        errors.append("scout_question_missing")
    if not request.scope.strip():
        errors.append("scout_scope_missing")
    if not request.probes:
        errors.append("scout_probes_missing")
    for probe in request.probes:
        if not probe.name.strip():
            errors.append("scout_probe_name_missing")
        if probe.kind not in SCOUT_PROBE_KINDS:
            errors.append("scout_probe_unsupported")
        elif probe.kind == "fetch":
            if not probe.uri.strip():
                errors.append("scout_probe_uri_missing")
        elif probe.kind == "repo":
            if probe.op not in REPO_PROBE_OPS:
                errors.append("scout_repo_op_unsupported")
            if not (probe.path or "").strip():
                errors.append("scout_repo_path_missing")
    if request.max_calls < 1:
        errors.append("scout_max_calls_invalid")
    if request.stop_condition not in SCOUT_STOP_CONDITIONS:
        errors.append("scout_stop_condition_unknown")
    try:
        deadline = datetime.fromisoformat(request.deadline.replace("Z", "+00:00"))
        if deadline.tzinfo is None:
            errors.append("scout_deadline_naive")
    except ValueError:
        errors.append("scout_deadline_invalid")
    return tuple(errors)


def packet_to_json(packet: ScoutPacket) -> bytes:
    return _canonical_json(
        {
            "schema": SCOUT_PACKET_SCHEMA,
            "packet_id": packet.packet_id,
            "request_hash": packet.request_hash,
            "status": packet.status,
            "facts": [
                {
                    "probe": fact.probe,
                    "observation": fact.observation,
                    "source_hash": fact.source_hash,
                    "effect_class": fact.effect_class,
                }
                for fact in packet.facts
            ],
            "unresolved": list(packet.unresolved),
            "evidence_hashes": list(packet.evidence_hashes),
            "observed_revision": packet.observed_revision,
            "observed_at": packet.observed_at,
            "scope": packet.scope,
            "required_total": packet.required_total,
            "required_done": packet.required_done,
        }
    ).encode()


def validate_scout_packet_bytes(content: bytes) -> tuple[str, ...]:
    """Fail-closed typed consumer for scout packets (W5 pair validation)."""

    try:
        payload = json.loads(content.decode("utf-8"))
    except Exception:
        return ("scout_packet_not_json",)
    if not isinstance(payload, dict):
        return ("scout_packet_not_object",)
    if payload.get("schema") != SCOUT_PACKET_SCHEMA:
        return ("scout_packet_schema_mismatch",)
    if payload.get("status") not in SCOUT_STATUSES:
        return ("scout_packet_status_invalid",)
    facts = payload.get("facts")
    if not isinstance(facts, list):
        return ("scout_packet_facts_invalid",)
    for fact in facts:
        if (
            not isinstance(fact, dict)
            or not isinstance(fact.get("probe"), str)
            or not isinstance(fact.get("source_hash"), str)
        ):
            return ("scout_packet_fact_malformed",)
    for field in ("packet_id", "request_hash", "observed_revision", "observed_at", "scope"):
        if not isinstance(payload.get(field), str) or not payload.get(field):
            return (f"scout_packet_{field}_missing",)
    return ()


def _observation(retrieved: RetrievedSource, *, limit: int = 4000) -> str:
    text = retrieved.as_text()
    return text if len(text) <= limit else text[:limit]


class LocalRepoProbeExecutor:
    """Default repo executor: read-only ops under worktree containment.

    No subprocess with caller-controlled input beyond a fixed ``git log``
    argv, no network, no writes, no installs — effect class is always
    read-only, so procedure/capability approval cannot be bypassed through
    it. A docker-backed executor can replace it through the seam (follow-up).
    """

    def __init__(self, *, timeout_seconds: float = 10.0) -> None:
        self._timeout_seconds = timeout_seconds

    async def run(
        self, *, op: str, path: str, root: str, max_bytes: int
    ) -> RepoProbeResult:
        import asyncio as _asyncio
        from pathlib import Path as _Path

        from vuzol.execution.paths import contained, trusted_root

        if op not in REPO_PROBE_OPS:
            raise ScoutError("scout_repo_op_unsupported", op)
        try:
            anchor = trusted_root(_Path(root), create=False)
        except Exception as error:
            raise ScoutError("scout_repo_root_unavailable", root) from error
        try:
            target = contained(anchor, anchor / (path or "."))
        except Exception as error:
            raise ScoutError("scout_repo_path_escape", path) from error
        if op == "read_file":
            try:
                raw = target.read_bytes()[:max_bytes]
            except OSError as error:
                raise ScoutError("scout_repo_read_failed", path) from error
            text = raw.decode("utf-8", errors="replace")
        else:
            try:
                process = await _asyncio.create_subprocess_exec(
                    "git",
                    "-C",
                    str(anchor),
                    "log",
                    "--format=%H %s",
                    "-n",
                    str(REPO_PROBE_GIT_LINES),
                    "--",
                    str(target.relative_to(anchor)),
                    stdout=_asyncio.subprocess.PIPE,
                    stderr=_asyncio.subprocess.PIPE,
                )
                out, _err = await _asyncio.wait_for(
                    process.communicate(), timeout=self._timeout_seconds
                )
            except (TimeoutError, OSError) as error:
                raise ScoutError("scout_repo_git_failed", path) from error
            if process.returncode != 0:
                raise ScoutError("scout_repo_git_failed", path)
            raw = out
            text = raw.decode("utf-8", errors="replace")
        if not text.strip():
            raise ScoutError("scout_repo_empty", path)
        return RepoProbeResult(
            observation=text[:4000],
            evidence_hash=hashlib.sha256(raw).hexdigest(),
        )


async def run_scout(
    session: AsyncSession,
    *,
    request: ScoutRequest,
    fetch: FetchProbe,
    profile: ProviderProfileConfig,
    limits: HardLimits,
    task_id: uuid.UUID | None = None,
    horizon_id: uuid.UUID | None = None,
    artifacts: ArtifactStore | None = None,
    consumer_step_id: uuid.UUID | None = None,
    project_id: str | None = None,
    now: str | None = None,
    repo_executor: RepoProbeExecutor | None = None,
    repo_root: str | None = None,
) -> ScoutPacket:
    """Execute a bounded scout: reserve → probe → persist → settle.

    Partial results survive probe failures (facts kept, missing probes in
    ``unresolved``); a run with zero facts is ``failed``, never silent.
    Raises ``ScoutError`` for refused requests and ``BudgetExceeded`` when
    capacity is gone — both fail-closed, neither invents evidence.
    """

    errors = validate_request(request)
    if errors:
        raise ScoutError(errors[0])
    required = [probe for probe in request.probes if probe.required]
    invocation_id = uuid.uuid4()
    estimate = estimate_reservation(profile, input_tokens=512, output_tokens=256)
    try:
        reservation = await reserve_invocation_budget(
            session,
            invocation_id=invocation_id,
            profile=profile,
            estimate=estimate,
            limits=limits,
            accounting=accounting_for_profile(
                profile,
                purpose="scout",
                attempt_kind="initial",
                horizon_id=horizon_id,
            ),
            task_id=task_id,
        )
    except BudgetExceeded as error:
        raise ScoutError("scout_budget_exhausted", str(error)) from error
    facts: list[ScoutFact] = []
    unresolved: list[str] = []
    evidence_hashes: list[str] = []
    fetched_bytes = 0
    calls = 0
    done_required = 0
    fetch_now = now or datetime.now(UTC).isoformat()
    executor = repo_executor or LocalRepoProbeExecutor()
    try:
        for probe in request.probes:
            if calls >= request.max_calls:
                unresolved.append(probe.name)
                continue
            calls += 1
            try:
                if probe.kind == "repo":
                    if repo_root is None:
                        raise ScoutError("scout_repo_root_missing", probe.name)
                    outcome = await executor.run(
                        op=probe.op or "",
                        path=probe.path or ".",
                        root=repo_root,
                        max_bytes=REPO_PROBE_MAX_BYTES,
                    )
                    observation, source_hash = outcome.observation, outcome.evidence_hash
                    fetched_bytes += len(observation.encode())
                else:
                    retrieved = fetch(probe.uri, now=fetch_now)
                    observation, source_hash = (
                        _observation(retrieved),
                        retrieved.content_hash,
                    )
                    fetched_bytes += len(retrieved.content)
            except ScoutError:
                unresolved.append(probe.name)
                await _emit_packet_event(
                    session,
                    request_hash(request),
                    status="partial",
                    probe=probe.name,
                )
                continue
            except Exception:
                unresolved.append(probe.name)
                await _emit_packet_event(
                    session,
                    request_hash(request),
                    status="partial",
                    probe=probe.name,
                )
                continue
            facts.append(
                ScoutFact(
                    probe=probe.name,
                    observation=observation,
                    source_hash=source_hash,
                )
            )
            evidence_hashes.append(source_hash)
            if probe.required:
                done_required += 1
        if not facts:
            status = "failed"
        elif request.stop_condition == "any_success":
            status = "complete" if done_required >= 1 else "partial"
        elif done_required >= len(required):
            status = "complete"
        else:
            status = "partial"
        packet = ScoutPacket(
            packet_id=str(uuid.uuid4()),
            request_hash=request_hash(request),
            status=status,
            facts=facts,
            unresolved=unresolved,
            evidence_hashes=sorted(set(evidence_hashes)),
            observed_revision=hashlib.sha256(
                "".join(sorted(set(evidence_hashes))).encode()
            ).hexdigest()
            if evidence_hashes
            else "0" * 64,
            observed_at=now or datetime.now(UTC).isoformat(),
            scope=request.scope,
            required_total=len(required),
            required_done=done_required,
        )
        if artifacts is not None:
            await _persist_packet(
                session,
                packet=packet,
                artifacts=artifacts,
                task_id=task_id,
                consumer_step_id=consumer_step_id,
                project_id=project_id or request.scope,
            )
        await _emit_packet_event(
            session, packet.request_hash, status=status, probe=None
        )
        from vuzol.providers.domain import NormalizedUsage

        await settle_invocation_budget(
            session,
            reservation=reservation,
            profile=profile,
            usage=NormalizedUsage(
                input_tokens=max(fetched_bytes // 4, 1),
                output_tokens=0,
                duration_ms=0,
            ),
            provider_request_id=None,
            outcome="succeeded" if status != "failed" else "failed",
        )
        return packet
    except Exception:
        from contextlib import suppress

        with suppress(Exception):
            await release_reservation_unfenced(session, reservation_id=reservation.id)
        raise


async def retry_scout(
    session: AsyncSession,
    *,
    packet: ScoutPacket,
    request: ScoutRequest,
    fetch: FetchProbe,
    profile: ProviderProfileConfig,
    limits: HardLimits,
    task_id: uuid.UUID | None = None,
    horizon_id: uuid.UUID | None = None,
    artifacts: ArtifactStore | None = None,
    consumer_step_id: uuid.UUID | None = None,
    project_id: str | None = None,
    now: str | None = None,
    repo_executor: RepoProbeExecutor | None = None,
    repo_root: str | None = None,
) -> ScoutPacket:
    """Retry only the missing probes of a partial packet (drill 14).

    Valid facts are reused untouched; the merged packet supersedes the old
    binding (documented refresh). Retrying a complete packet is a no-op
    returning it unchanged.
    """

    if packet.status == "complete" or not packet.unresolved:
        return packet
    if request_hash(request) != packet.request_hash:
        raise ScoutError("scout_request_mismatch")
    missing = {name for name in packet.unresolved}
    narrowed = ScoutRequest(
        question=request.question,
        scope=request.scope,
        probes=tuple(probe for probe in request.probes if probe.name in missing),
        deadline=request.deadline,
        max_calls=request.max_calls,
        stop_condition=request.stop_condition,
    )
    rerun = await run_scout(
        session,
        request=narrowed,
        fetch=fetch,
        profile=profile,
        limits=limits,
        task_id=task_id,
        horizon_id=horizon_id,
        artifacts=None,
        consumer_step_id=None,
        project_id=project_id,
        now=now,
        repo_executor=repo_executor,
        repo_root=repo_root,
    )
    merged_facts = list(packet.facts)
    seen = {fact.probe for fact in merged_facts}
    for fact in rerun.facts:
        if fact.probe not in seen:
            merged_facts.append(fact)
            seen.add(fact.probe)
    still_missing = [name for name in packet.unresolved if name not in seen]
    merged = ScoutPacket(
        packet_id=str(uuid.uuid4()),
        request_hash=packet.request_hash,
        status="complete"
        if not still_missing and merged_facts
        else ("partial" if merged_facts else "failed"),
        facts=merged_facts,
        unresolved=still_missing,
        evidence_hashes=sorted(
            {*(e for e in packet.evidence_hashes), *(e for e in rerun.evidence_hashes)}
        ),
        observed_revision=hashlib.sha256(
            "".join(
                sorted(
                    {*(e for e in packet.evidence_hashes), *(e for e in rerun.evidence_hashes)}
                )
            ).encode()
        ).hexdigest(),
        observed_at=now or datetime.now(UTC).isoformat(),
        scope=packet.scope,
        required_total=packet.required_total,
        required_done=packet.required_total - len(still_missing),
    )
    if artifacts is not None:
        await _persist_packet(
            session,
            packet=merged,
            artifacts=artifacts,
            task_id=task_id,
            consumer_step_id=consumer_step_id,
            project_id=project_id or request.scope,
            refresh=True,
        )
    await _emit_packet_event(
        session, merged.request_hash, status=merged.status, probe=None
    )
    return merged


async def _persist_packet(
    session: AsyncSession,
    *,
    packet: ScoutPacket,
    artifacts: ArtifactStore,
    task_id: uuid.UUID | None,
    consumer_step_id: uuid.UUID | None,
    project_id: str,
    refresh: bool = False,
) -> uuid.UUID:
    """Persist a packet as typed Artifact + InputBinding (Q6, no new table)."""

    from vuzol.storage.repositories import InputBindingRepository

    content = packet_to_json(packet)
    errors = validate_scout_packet_bytes(content)
    if errors:
        raise ScoutError(errors[0])
    # Artifact rows need task/run/step linkage: resolve the consumer context.
    # Step-less packets without any task context cannot persist bytes —
    # explicit refusal, never an orphan artifact.
    run_id: uuid.UUID | None = None
    step_id: uuid.UUID | None = None
    if consumer_step_id is not None:
        consumer = await session.get(Step, consumer_step_id)
        if consumer is not None:
            run_id = consumer.run_id
            step_id = consumer.id
    if task_id is None or run_id is None or step_id is None:
        raise ScoutError("scout_artifact_needs_task_context")
    artifact = await artifacts.persist(
        session,
        task_id=task_id,
        run_id=run_id,
        step_id=step_id,
        artifact_type="scout_packet",
        content=content,
        media_type="application/json",
        sensitivity="internal",
        visibility="private",
    )
    if consumer_step_id is not None:
        bindings = InputBindingRepository(session)
        existing = [
            row
            for row in await bindings.for_consumer(consumer_step_id)
            if row.slot == SCOUT_SLOT
        ]
        if existing and refresh:
            row = existing[0]
            row.artifact_id = artifact.id
            row.content_hash = artifact.content_hash
            row.schema_name = "scout-packet"
            row.schema_version = SCOUT_PACKET_SCHEMA
            row.scope_project_id = project_id
            await session.flush()
        elif not existing:
            session.add(
                InputBinding(
                    consumer_step_id=consumer_step_id,
                    producer_step_id=None,
                    artifact_id=artifact.id,
                    slot=SCOUT_SLOT,
                    schema_name="scout-packet",
                    schema_version=SCOUT_PACKET_SCHEMA,
                    content_hash=artifact.content_hash,
                    scope_project_id=project_id,
                    access_scope="private",
                    required=True,
                    status="resolved",
                )
            )
            await session.flush()
    return artifact.id


async def _emit_packet_event(
    session: AsyncSession,
    request_hash_value: str,
    *,
    status: str,
    probe: str | None,
) -> None:
    session.add(
        Event(
            entity_type="scout_packet",
            entity_id=uuid.uuid5(uuid.NAMESPACE_URL, f"scout:{request_hash_value}"),
            event_type="scout.packet_partial" if status == "partial" else "scout.packet_created",
            actor_type="system",
            payload={
                "request_hash": request_hash_value,
                "status": status,
                "probe": probe,
            },
        )
    )
    await session.flush()


@dataclass(frozen=True, slots=True)
class _Unused:
    marker: str = "reserved"
