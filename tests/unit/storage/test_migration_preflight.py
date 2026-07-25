"""Unit tests for S-2.0 migration-head preflight (no database required)."""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.ext.asyncio import AsyncEngine

from vuzol.storage.migration_preflight import (
    CODE_BEHIND,
    CODE_EMPTY,
    CODE_MISMATCH,
    CODE_MISSING_TABLE,
    CODE_OK,
    CODE_SCRIPTS_UNAVAILABLE,
    CODE_UNKNOWN,
    CODE_UNREACHABLE,
    MigrationHeadError,
    classify_migration_head,
    fetch_observed_revisions,
    is_undefined_table_error,
    require_migration_head,
    resolve_alembic_script_location,
    verify_migration_head,
)


class _OrigSqlState(Exception):
    """DBAPI-shaped original error with SQLSTATE attributes (valid BaseException)."""

    def __init__(
        self,
        message: str,
        *,
        sqlstate: str | None = None,
        pgcode: str | None = None,
    ) -> None:
        super().__init__(message)
        self.sqlstate = sqlstate
        self.pgcode = pgcode


class _OrigDiagSqlState(Exception):
    """DBAPI-shaped original error exposing diag.sqlstate."""

    def __init__(self, message: str, *, sqlstate: str) -> None:
        super().__init__(message)
        self.diag = SimpleNamespace(sqlstate=sqlstate)


async def _observed(*revisions: str) -> frozenset[str]:
    return frozenset(revisions)


def test_classify_equal_heads_ok() -> None:
    heads = frozenset({"rev_c"})
    assert (
        classify_migration_head(
            expected_heads=heads,
            observed=frozenset({"rev_c"}),
            known_revisions=frozenset({"rev_a", "rev_b", "rev_c"}),
        )
        == CODE_OK
    )


def test_classify_empty_observed() -> None:
    assert (
        classify_migration_head(
            expected_heads=frozenset({"rev_c"}),
            observed=frozenset(),
            known_revisions=frozenset({"rev_c"}),
        )
        == CODE_EMPTY
    )


def test_classify_unknown_revision() -> None:
    assert (
        classify_migration_head(
            expected_heads=frozenset({"rev_c"}),
            observed=frozenset({"rev_from_future"}),
            known_revisions=frozenset({"rev_a", "rev_b", "rev_c"}),
        )
        == CODE_UNKNOWN
    )


def test_classify_behind_with_ancestor_predicate() -> None:
    def is_strict_ancestor(revision: str, head: str) -> bool:
        order = {"rev_a": 0, "rev_b": 1, "rev_c": 2}
        return order.get(revision, -1) < order.get(head, -1) and revision in order

    assert (
        classify_migration_head(
            expected_heads=frozenset({"rev_c"}),
            observed=frozenset({"rev_b"}),
            known_revisions=frozenset({"rev_a", "rev_b", "rev_c"}),
            is_strict_ancestor=is_strict_ancestor,
        )
        == CODE_BEHIND
    )


def test_classify_mismatch_when_not_ancestor() -> None:
    def never_ancestor(_revision: str, _head: str) -> bool:
        return False

    assert (
        classify_migration_head(
            expected_heads=frozenset({"head_a", "head_b"}),
            observed=frozenset({"head_a"}),
            known_revisions=frozenset({"head_a", "head_b", "base"}),
            is_strict_ancestor=never_ancestor,
        )
        == CODE_MISMATCH
    )


def test_classify_empty_expected_is_scripts_unavailable() -> None:
    assert (
        classify_migration_head(
            expected_heads=frozenset(),
            observed=frozenset({"rev_c"}),
        )
        == CODE_SCRIPTS_UNAVAILABLE
    )


def test_c1_out_of_tree_is_unknown_not_ahead() -> None:
    """C1: out-of-tree stamps are unknown; no ahead code exists."""

    code = classify_migration_head(
        expected_heads=frozenset({"rev_b"}),
        observed=frozenset({"rev_c_not_in_tree"}),
        known_revisions=frozenset({"rev_a", "rev_b"}),
    )
    assert code == CODE_UNKNOWN
    assert code != "migration_head_ahead"


def test_c1_in_tree_rollback_skew_is_mismatch() -> None:
    """C1: known non-head in-tree stamp that is not a strict ancestor → mismatch.

    Models code rollback while DB remains on a newer-but-still-known revision
    that is not the expected head and not classified as behind.
    """

    def never_ancestor(_revision: str, _head: str) -> bool:
        return False

    assert (
        classify_migration_head(
            expected_heads=frozenset({"rev_b"}),
            observed=frozenset({"rev_c"}),  # in-tree, not ancestor of rev_b
            known_revisions=frozenset({"rev_a", "rev_b", "rev_c"}),
            is_strict_ancestor=never_ancestor,
        )
        == CODE_MISMATCH
    )


def test_c3_without_known_revisions_foreign_is_not_unknown() -> None:
    """C3: accurate unknown requires known_revisions; without it → mismatch."""

    assert (
        classify_migration_head(
            expected_heads=frozenset({"h1"}),
            observed=frozenset({"foreign_stamp"}),
            known_revisions=None,
        )
        == CODE_MISMATCH
    )


@pytest.mark.anyio
async def test_c3_inject_heads_only_skips_unknown_without_known() -> None:
    report = await verify_migration_head(
        expected_heads=frozenset({"h1"}),
        fetch_observed=lambda: _observed("foreign_stamp"),
        # no known_revisions, no alembic_script_location
    )
    assert report.ok is False
    assert report.code == CODE_MISMATCH


@pytest.mark.anyio
async def test_c3_inject_heads_with_known_classifies_unknown() -> None:
    report = await verify_migration_head(
        expected_heads=frozenset({"h1"}),
        fetch_observed=lambda: _observed("foreign_stamp"),
        known_revisions=frozenset({"h1", "h0"}),
    )
    assert report.ok is False
    assert report.code == CODE_UNKNOWN


@pytest.mark.anyio
async def test_c3_explicit_script_location_loads_known(tmp_path: Path) -> None:
    """Explicit script location + injected heads still loads known from scripts."""

    versions = tmp_path / "alembic" / "versions"
    versions.mkdir(parents=True)
    # Minimal single-head revision chain for ScriptDirectory.
    (versions / "aaaa_base.py").write_text(
        'revision = "aaaa"\ndown_revision = None\n',
        encoding="utf-8",
    )
    (versions / "bbbb_head.py").write_text(
        'revision = "bbbb"\ndown_revision = "aaaa"\n',
        encoding="utf-8",
    )

    report = await verify_migration_head(
        expected_heads=frozenset({"bbbb"}),
        fetch_observed=lambda: _observed("not_in_tree_zzzz"),
        alembic_script_location=tmp_path / "alembic",
    )
    assert report.ok is False
    assert report.code == CODE_UNKNOWN


@pytest.mark.anyio
async def test_c3_production_engine_path_loads_known(tmp_path: Path) -> None:
    """Engine-only path resolves scripts and loads known (no DI heads)."""

    versions = tmp_path / "alembic" / "versions"
    versions.mkdir(parents=True)
    (versions / "r1.py").write_text(
        'revision = "r1"\ndown_revision = None\n',
        encoding="utf-8",
    )

    async def observed() -> frozenset[str]:
        return frozenset({"ghost"})

    report = await verify_migration_head(
        alembic_script_location=tmp_path / "alembic",
        fetch_observed=observed,
        # no expected_heads — production-style script load
    )
    assert report.ok is False
    assert report.expected_heads == ("r1",)
    assert report.code == CODE_UNKNOWN


def test_c2_undefined_table_sqlstate_42p01() -> None:
    error = ProgrammingError(
        "stmt",
        {},
        _OrigSqlState("wrapped", sqlstate="42P01", pgcode="42P01"),
    )
    assert is_undefined_table_error(error) is True


def test_c2_undefined_table_diag_sqlstate() -> None:
    error = ProgrammingError(
        "stmt",
        {},
        _OrigDiagSqlState("wrapped", sqlstate="42P01"),
    )
    assert is_undefined_table_error(error) is True


def test_c2_non_undefined_programming_error_not_missing_table() -> None:
    error = ProgrammingError(
        "stmt",
        {},
        _OrigSqlState("permission denied", sqlstate="42501", pgcode="42501"),
    )
    assert is_undefined_table_error(error) is False


def test_c2_string_fallback_still_works() -> None:
    error = ProgrammingError(
        "stmt",
        {},
        Exception('relation "alembic_version" does not exist'),
    )
    assert is_undefined_table_error(error) is True


@pytest.mark.anyio
async def test_c2_fetch_maps_42p01_to_missing_table() -> None:
    pe = ProgrammingError(
        "SELECT version_num",
        {},
        _OrigSqlState("undefined", sqlstate="42P01", pgcode="42P01"),
    )

    class _Conn:
        async def execute(self, _statement: object) -> object:
            raise pe

        async def __aenter__(self) -> _Conn:
            return self

        async def __aexit__(self, *_args: object) -> None:
            return None

    engine = MagicMock(spec=AsyncEngine)
    engine.connect.return_value = _Conn()

    with pytest.raises(MigrationHeadError) as excinfo:
        await fetch_observed_revisions(engine)
    assert excinfo.value.code == CODE_MISSING_TABLE
    assert "postgresql://" not in str(excinfo.value).lower()


@pytest.mark.anyio
async def test_c2_fetch_other_programming_error_is_unreachable() -> None:
    pe = ProgrammingError(
        "SELECT version_num",
        {},
        _OrigSqlState("conn", sqlstate="08006", pgcode="08006"),
    )

    class _Conn:
        async def execute(self, _statement: object) -> object:
            raise pe

        async def __aenter__(self) -> _Conn:
            return self

        async def __aexit__(self, *_args: object) -> None:
            return None

    engine = MagicMock(spec=AsyncEngine)
    engine.connect.return_value = _Conn()

    with pytest.raises(MigrationHeadError) as excinfo:
        await fetch_observed_revisions(engine)
    assert excinfo.value.code == CODE_UNREACHABLE


@pytest.mark.anyio
async def test_verify_ok_with_injected_sets() -> None:
    report = await verify_migration_head(
        expected_heads=frozenset({"h1"}),
        fetch_observed=lambda: _observed("h1"),
        known_revisions=frozenset({"h1"}),
    )
    assert report.ok is True
    assert report.code == CODE_OK
    assert report.expected_heads == ("h1",)
    assert report.observed == ("h1",)
    assert report.duration_ms >= 0


@pytest.mark.anyio
async def test_verify_empty_observed() -> None:
    report = await verify_migration_head(
        expected_heads=frozenset({"h1"}),
        fetch_observed=lambda: _observed(),
        known_revisions=frozenset({"h1"}),
    )
    assert report.ok is False
    assert report.code == CODE_EMPTY


@pytest.mark.anyio
async def test_verify_missing_table_via_fetch_error() -> None:
    async def boom() -> frozenset[str]:
        raise MigrationHeadError(CODE_MISSING_TABLE, "alembic_version table is missing")

    report = await verify_migration_head(
        expected_heads=frozenset({"h1"}),
        fetch_observed=boom,
        known_revisions=frozenset({"h1"}),
    )
    assert report.ok is False
    assert report.code == CODE_MISSING_TABLE


@pytest.mark.anyio
async def test_verify_unreachable_without_engine() -> None:
    report = await verify_migration_head(expected_heads=frozenset({"h1"}))
    assert report.ok is False
    assert report.code == CODE_UNREACHABLE


@pytest.mark.anyio
async def test_verify_timeout() -> None:
    async def slow() -> frozenset[str]:
        await asyncio.sleep(1.0)
        return frozenset({"h1"})

    report = await verify_migration_head(
        expected_heads=frozenset({"h1"}),
        fetch_observed=slow,
        known_revisions=frozenset({"h1"}),
        timeout_seconds=0.05,
    )
    assert report.ok is False
    assert report.code == CODE_UNREACHABLE


@pytest.mark.anyio
async def test_require_raises_with_stable_code() -> None:
    with pytest.raises(MigrationHeadError) as excinfo:
        await require_migration_head(
            expected_heads=frozenset({"h1"}),
            fetch_observed=lambda: _observed("other"),
            known_revisions=frozenset({"h1"}),
        )
    assert excinfo.value.code == CODE_UNKNOWN
    assert excinfo.value.expected == ("h1",)
    assert excinfo.value.observed == ("other",)
    text = str(excinfo.value).lower()
    assert "postgresql://" not in text
    assert "password" not in text


@pytest.mark.anyio
async def test_require_returns_on_ok() -> None:
    report = await require_migration_head(
        expected_heads=frozenset({"h1"}),
        fetch_observed=lambda: _observed("h1"),
        known_revisions=frozenset({"h1"}),
    )
    assert report.ok is True


@pytest.mark.anyio
async def test_scripts_unavailable_from_load_heads() -> None:
    def bad_heads() -> frozenset[str]:
        raise MigrationHeadError(CODE_SCRIPTS_UNAVAILABLE, "alembic scripts unavailable")

    report = await verify_migration_head(
        load_heads=bad_heads,
        fetch_observed=lambda: _observed("h1"),
    )
    assert report.ok is False
    assert report.code == CODE_SCRIPTS_UNAVAILABLE


@pytest.mark.anyio
async def test_verify_behind_end_to_end_injected() -> None:
    def is_strict_ancestor(revision: str, head: str) -> bool:
        return revision == "parent" and head == "head"

    report = await verify_migration_head(
        expected_heads=frozenset({"head"}),
        fetch_observed=lambda: _observed("parent"),
        known_revisions=frozenset({"parent", "head"}),
        is_strict_ancestor=is_strict_ancestor,
    )
    assert report.ok is False
    assert report.code == CODE_BEHIND


def test_resolve_script_location_explicit_and_walk(tmp_path: Path) -> None:
    versions = tmp_path / "alembic" / "versions"
    versions.mkdir(parents=True)
    resolved = resolve_alembic_script_location(tmp_path / "alembic")
    assert resolved == (tmp_path / "alembic").resolve()

    nested = tmp_path / "src" / "vuzol" / "storage" / "migration_preflight.py"
    nested.parent.mkdir(parents=True)
    nested.write_text("# sentinel\n", encoding="utf-8")
    walked = resolve_alembic_script_location(start=nested)
    assert walked == (tmp_path / "alembic").resolve()


def test_resolve_script_location_missing_raises(tmp_path: Path) -> None:
    start = tmp_path / "orphan" / "file.py"
    start.parent.mkdir(parents=True)
    start.write_text("x\n", encoding="utf-8")
    with pytest.raises(MigrationHeadError) as excinfo:
        resolve_alembic_script_location(start=start)
    assert excinfo.value.code == CODE_SCRIPTS_UNAVAILABLE


def test_resolve_explicit_missing_directory(tmp_path: Path) -> None:
    with pytest.raises(MigrationHeadError) as excinfo:
        resolve_alembic_script_location(tmp_path / "nope")
    assert excinfo.value.code == CODE_SCRIPTS_UNAVAILABLE
