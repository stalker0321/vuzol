"""Fail-closed Alembic migration-head verification (Step 10 S-2.0).

Never mutates schema. Never logs DSNs or secret material. Service CLI wiring is
out of scope for this module slice.

Taxonomy freeze (no ``migration_head_ahead`` code):

* **out-of-tree** observed revisions (not in the local script graph) →
  ``migration_head_unknown`` (includes DB stamps this code tree cannot name).
* **in-tree** skew that is not exact head equality and not classifiable as
  strict-ancestor **behind** (including code-rollback with DB still on a newer
  *known* non-head revision, multi-head partial sets, siblings) →
  ``migration_head_mismatch``.

Injection contract:

* Accurate ``unknown`` classification requires a ``known_revisions`` set (or
  script loading that supplies one).
* Production callers that pass only ``engine`` (and optional script location)
  always resolve the script directory and load heads + known revisions +
  ancestor predicate from it.
* Callers that inject ``expected_heads`` without ``known_revisions`` and without
  a resolvable script location will not map foreign stamps to ``unknown``;
  non-equal sets fall through to behind/mismatch using only the predicates
  provided.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, ProgrammingError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine

CODE_OK = "ok"
CODE_EMPTY = "migration_head_empty"
CODE_MISSING_TABLE = "migration_head_missing_table"
CODE_UNKNOWN = "migration_head_unknown"
CODE_BEHIND = "migration_head_behind"
CODE_MISMATCH = "migration_head_mismatch"
CODE_UNREACHABLE = "migration_head_unreachable"
CODE_SCRIPTS_UNAVAILABLE = "migration_scripts_unavailable"

# PostgreSQL undefined_table — preferred over redacted string matching.
_PG_UNDEFINED_TABLE = "42P01"

DEFAULT_TIMEOUT_SECONDS = 10.0

_FetchObserved = Callable[[], Awaitable[frozenset[str]]]
_LoadHeads = Callable[[], frozenset[str]]
_IsAncestor = Callable[[str, str], bool]
_KnownRevisions = Callable[[], frozenset[str]]


class MigrationHeadError(RuntimeError):
    """Schema head does not match this code tree (or could not be verified)."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        expected: Iterable[str] = (),
        observed: Iterable[str] = (),
    ) -> None:
        super().__init__(message)
        self.code = code
        self.expected = tuple(sorted(expected))
        self.observed = tuple(sorted(observed))


@dataclass(frozen=True, slots=True)
class MigrationHeadReport:
    """Outcome of a single non-mutating head verification."""

    ok: bool
    code: str
    expected_heads: tuple[str, ...]
    observed: tuple[str, ...]
    duration_ms: int


def resolve_alembic_script_location(
    explicit: Path | None = None,
    *,
    start: Path | None = None,
) -> Path:
    """Resolve the Alembic script directory that ships with this revision.

    Order: explicit path → walk parents of ``start`` (default: this file) looking
    for ``alembic/versions``. Production must not rely on process cwd.
    """

    if explicit is not None:
        path = explicit.expanduser()
        if not path.is_dir():
            raise MigrationHeadError(
                CODE_SCRIPTS_UNAVAILABLE,
                "alembic script location is not a directory",
            )
        return path.resolve()

    origin = (start or Path(__file__)).resolve()
    for parent in (origin, *origin.parents):
        candidate = parent / "alembic"
        if (candidate / "versions").is_dir():
            return candidate.resolve()
    raise MigrationHeadError(
        CODE_SCRIPTS_UNAVAILABLE,
        "alembic script location could not be resolved",
    )


def load_expected_heads(script_location: Path) -> frozenset[str]:
    """Return the head revision set from an on-disk Alembic script directory."""

    try:
        from alembic.script import ScriptDirectory
    except ImportError as error:
        raise MigrationHeadError(
            CODE_SCRIPTS_UNAVAILABLE,
            "alembic package is unavailable",
        ) from error

    try:
        scripts = ScriptDirectory(str(script_location))
        heads = frozenset(scripts.get_heads())
    except Exception as error:
        raise MigrationHeadError(
            CODE_SCRIPTS_UNAVAILABLE,
            "alembic script directory could not be loaded",
        ) from error
    if not heads:
        raise MigrationHeadError(
            CODE_SCRIPTS_UNAVAILABLE,
            "alembic script directory has no heads",
        )
    return heads


def load_known_revisions(script_location: Path) -> frozenset[str]:
    """Return every revision id present in the script directory."""

    try:
        from alembic.script import ScriptDirectory
    except ImportError as error:
        raise MigrationHeadError(
            CODE_SCRIPTS_UNAVAILABLE,
            "alembic package is unavailable",
        ) from error

    try:
        scripts = ScriptDirectory(str(script_location))
        return frozenset(script.revision for script in scripts.walk_revisions())
    except Exception as error:
        raise MigrationHeadError(
            CODE_SCRIPTS_UNAVAILABLE,
            "alembic revision graph could not be loaded",
        ) from error


def make_strict_ancestor_predicate(script_location: Path) -> _IsAncestor:
    """Build ``is_strict_ancestor(rev, head)`` using the local script graph only."""

    try:
        from alembic.script import ScriptDirectory
    except ImportError as error:
        raise MigrationHeadError(
            CODE_SCRIPTS_UNAVAILABLE,
            "alembic package is unavailable",
        ) from error

    try:
        scripts = ScriptDirectory(str(script_location))
    except Exception as error:
        raise MigrationHeadError(
            CODE_SCRIPTS_UNAVAILABLE,
            "alembic revision graph could not be loaded",
        ) from error

    def is_strict_ancestor(revision: str, head: str) -> bool:
        if revision == head:
            return False
        try:
            # Walk downward from head toward base; membership ⇒ ancestor-or-self.
            seen = {item.revision for item in scripts.iterate_revisions(head, "base")}
        except Exception:
            return False
        return revision in seen

    return is_strict_ancestor


def classify_migration_head(
    *,
    expected_heads: frozenset[str],
    observed: frozenset[str],
    known_revisions: frozenset[str] | None = None,
    is_strict_ancestor: _IsAncestor | None = None,
) -> str:
    """Return ``ok`` or a stable failure code for two revision sets.

    Taxonomy freeze (no ``ahead`` code):

    * Out-of-tree observed revs (``known_revisions`` provided and membership fails)
      → ``migration_head_unknown``.
    * In-tree non-equal sets that are not strict-ancestor **behind** (including
      code rollback onto a still-known newer DB stamp, multi-head partials)
      → ``migration_head_mismatch``.

    When ``known_revisions`` is omitted, foreign stamps cannot be distinguished
    from other non-equal sets and fall through to behind/mismatch.
    """

    if not expected_heads:
        return CODE_SCRIPTS_UNAVAILABLE
    if not observed:
        return CODE_EMPTY
    if observed == expected_heads:
        return CODE_OK

    if known_revisions is not None:
        unknown = frozenset(rev for rev in observed if rev not in known_revisions)
        if unknown:
            return CODE_UNKNOWN

    # Behind: every observed rev is a strict ancestor of at least one head,
    # and no observed rev is itself an expected head (sets already unequal).
    if (
        is_strict_ancestor is not None
        and observed.isdisjoint(expected_heads)
        and all(any(is_strict_ancestor(rev, head) for head in expected_heads) for rev in observed)
    ):
        return CODE_BEHIND

    # In-tree non-ancestor / partial heads / other skew — not a separate ahead code.
    return CODE_MISMATCH


def is_undefined_table_error(error: BaseException) -> bool:
    """True when the error chain indicates PostgreSQL undefined_table (42P01).

    Prefers driver SQLSTATE / ``pgcode`` / ``diag.sqlstate`` attributes; falls
    back to redacted substring heuristics only when attributes are absent.
    Does not return driver message text to callers.
    """

    current: BaseException | None = error
    seen: set[int] = set()
    saw_sqlstate = False
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        for attr in ("sqlstate", "pgcode"):
            value = getattr(current, attr, None)
            if value is not None:
                saw_sqlstate = True
                if str(value) == _PG_UNDEFINED_TABLE:
                    return True
        diag = getattr(current, "diag", None)
        if diag is not None:
            sqlstate = getattr(diag, "sqlstate", None)
            if sqlstate is not None:
                saw_sqlstate = True
                if str(sqlstate) == _PG_UNDEFINED_TABLE:
                    return True
        current = getattr(current, "orig", None)
        if current is None:
            # Only walk __cause__ after orig chain ends.
            break
    # Secondary: __cause__ chain
    current = getattr(error, "__cause__", None)
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        for attr in ("sqlstate", "pgcode"):
            value = getattr(current, attr, None)
            if value is not None:
                saw_sqlstate = True
                if str(value) == _PG_UNDEFINED_TABLE:
                    return True
        diag = getattr(current, "diag", None)
        if diag is not None:
            sqlstate = getattr(diag, "sqlstate", None)
            if sqlstate is not None:
                saw_sqlstate = True
                if str(sqlstate) == _PG_UNDEFINED_TABLE:
                    return True
        current = getattr(current, "orig", None) or getattr(current, "__cause__", None)

    # A driver-provided SQLSTATE is authoritative. Message matching is only for
    # drivers/wrappers that expose no structured state anywhere in the chain.
    if saw_sqlstate:
        return False
    message = str(getattr(error, "orig", error)).lower()
    return "alembic_version" in message and (
        "does not exist" in message or "undefinedtable" in message or "undefined table" in message
    )


async def fetch_observed_revisions(engine: AsyncEngine) -> frozenset[str]:
    """Read ``alembic_version``; never mutates schema."""

    try:
        async with engine.connect() as connection:
            result = await connection.execute(text("SELECT version_num FROM alembic_version"))
            rows = result.scalars().all()
    except ProgrammingError as error:
        if is_undefined_table_error(error):
            raise MigrationHeadError(
                CODE_MISSING_TABLE,
                "alembic_version table is missing",
            ) from error
        raise MigrationHeadError(
            CODE_UNREACHABLE,
            "database rejected the migration head query",
        ) from error
    except (TimeoutError, OSError, DBAPIError, SQLAlchemyError) as error:
        raise MigrationHeadError(
            CODE_UNREACHABLE,
            "database was unreachable during migration head query",
        ) from error
    except MigrationHeadError:
        raise
    except Exception as error:
        raise MigrationHeadError(
            CODE_UNREACHABLE,
            "migration head query failed",
        ) from error

    return frozenset(str(row) for row in rows if row is not None and str(row))


async def verify_migration_head(
    engine: AsyncEngine | None = None,
    *,
    alembic_script_location: Path | None = None,
    expected_heads: frozenset[str] | None = None,
    fetch_observed: _FetchObserved | None = None,
    known_revisions: frozenset[str] | None = None,
    is_strict_ancestor: _IsAncestor | None = None,
    load_heads: _LoadHeads | None = None,
    load_known: _KnownRevisions | None = None,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> MigrationHeadReport:
    """Verify DB revision set equals script heads; never mutates schema.

    Dependency injection (``expected_heads``, ``fetch_observed``, graph helpers)
    allows unit tests without a database. ``timeout_seconds`` bounds awaitable
    database/fetch work; trusted local Alembic graph discovery is synchronous and
    expected to remain small.

    **Production path:** pass ``engine`` only (optional explicit
    ``alembic_script_location``). The helper always resolves the script
    directory and loads expected heads, **known revisions**, and the ancestor
    predicate from it.

    **Injected path:** for accurate ``migration_head_unknown`` classification,
    supply ``known_revisions`` and/or ``load_known``, or pass an **explicit**
    ``alembic_script_location`` so known revisions can be loaded. Injecting only
    ``expected_heads`` (without known/script location) is allowed for pure unit
    tests but will not classify out-of-tree stamps as ``unknown``.
    """

    started = time.perf_counter()

    async def _run() -> MigrationHeadReport:
        expected: frozenset[str] = frozenset(expected_heads or ())
        known: frozenset[str] | None = known_revisions
        ancestor = is_strict_ancestor
        observed: frozenset[str] = frozenset()

        try:
            if expected_heads is not None:
                expected = frozenset(expected_heads)
                # C3: load known/ancestor only from explicit script location or
                # load_known — never walk from __file__ on partial DI (keeps unit
                # tests isolated from the repo alembic tree).
                if known is None and load_known is not None:
                    known = frozenset(load_known())
                if (known is None or ancestor is None) and alembic_script_location is not None:
                    location = resolve_alembic_script_location(alembic_script_location)
                    if known is None:
                        known = (
                            load_known_revisions(location)
                            if load_known is None
                            else frozenset(load_known())
                        )
                    if ancestor is None:
                        ancestor = make_strict_ancestor_predicate(location)
            elif load_heads is not None:
                expected = frozenset(load_heads())
                if known is None and load_known is not None:
                    known = frozenset(load_known())
            else:
                # Production / engine-only path: always load full script graph.
                location = resolve_alembic_script_location(alembic_script_location)
                expected = load_expected_heads(location)
                if known is None:
                    known = (
                        load_known_revisions(location)
                        if load_known is None
                        else frozenset(load_known())
                    )
                if ancestor is None:
                    ancestor = make_strict_ancestor_predicate(location)

            if fetch_observed is not None:
                observed = frozenset(await fetch_observed())
            else:
                if engine is None:
                    raise MigrationHeadError(
                        CODE_UNREACHABLE,
                        "database engine is required when fetch_observed is not provided",
                        expected=expected,
                    )
                observed = await fetch_observed_revisions(engine)
        except MigrationHeadError as error:
            duration_ms = int((time.perf_counter() - started) * 1000)
            return MigrationHeadReport(
                ok=False,
                code=error.code,
                expected_heads=error.expected or tuple(sorted(expected)),
                observed=error.observed or tuple(sorted(observed)),
                duration_ms=duration_ms,
            )

        code = classify_migration_head(
            expected_heads=expected,
            observed=observed,
            known_revisions=known,
            is_strict_ancestor=ancestor,
        )
        duration_ms = int((time.perf_counter() - started) * 1000)
        return MigrationHeadReport(
            ok=code == CODE_OK,
            code=code,
            expected_heads=tuple(sorted(expected)),
            observed=tuple(sorted(observed)),
            duration_ms=duration_ms,
        )

    try:
        return await asyncio.wait_for(_run(), timeout=timeout_seconds)
    except TimeoutError:
        duration_ms = int((time.perf_counter() - started) * 1000)
        return MigrationHeadReport(
            ok=False,
            code=CODE_UNREACHABLE,
            expected_heads=tuple(sorted(expected_heads or ())),
            observed=(),
            duration_ms=duration_ms,
        )


async def require_migration_head(
    engine: AsyncEngine | None = None,
    *,
    alembic_script_location: Path | None = None,
    expected_heads: frozenset[str] | None = None,
    fetch_observed: _FetchObserved | None = None,
    known_revisions: frozenset[str] | None = None,
    is_strict_ancestor: _IsAncestor | None = None,
    load_heads: _LoadHeads | None = None,
    load_known: _KnownRevisions | None = None,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> MigrationHeadReport:
    """Like :func:`verify_migration_head` but raises :class:`MigrationHeadError` on failure."""

    report = await verify_migration_head(
        engine,
        alembic_script_location=alembic_script_location,
        expected_heads=expected_heads,
        fetch_observed=fetch_observed,
        known_revisions=known_revisions,
        is_strict_ancestor=is_strict_ancestor,
        load_heads=load_heads,
        load_known=load_known,
        timeout_seconds=timeout_seconds,
    )
    if report.ok:
        return report
    raise MigrationHeadError(
        report.code,
        _message_for_code(report.code),
        expected=report.expected_heads,
        observed=report.observed,
    )


def _message_for_code(code: str) -> str:
    messages = {
        CODE_EMPTY: "database has no alembic revision rows",
        CODE_MISSING_TABLE: "alembic_version table is missing",
        CODE_UNKNOWN: "database revision is not present in this code tree",
        CODE_BEHIND: "database schema is behind this release; run alembic upgrade head",
        CODE_MISMATCH: (
            "database revision set does not match this code tree heads "
            "(in-tree skew or code/DB rollback mismatch; not an ahead code)"
        ),
        CODE_UNREACHABLE: "database was unreachable during migration head verification",
        CODE_SCRIPTS_UNAVAILABLE: "alembic scripts for this release are unavailable",
    }
    return messages.get(code, "migration head verification failed")
