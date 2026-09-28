"""Deterministic partition manifest for large review diffs (WP07).

A large diff is split by files/areas into bounded partitions. Invariants:

- coverage: union of partition files == inspection changed files;
- overlap: partitions are pairwise disjoint;
- determinism: files sorted, first-fit packing, stable partition ids;
- inventory: generated/lockfile files are listed explicitly, never dropped;
- honesty: truncation flags reflect reality instead of hardcoded False.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Sequence

from vuzol.execution.domain import GitInspection
from vuzol.experiments.domain import FrozenModel
from vuzol.review.policy import (
    REVIEW_POLICY_REVISION,
    FileClass,
    IndependentReviewError,
    ReviewLevel,
    classify_file,
    level_for,
)
from vuzol.storage.types import RiskLevel

PARTITION_MANIFEST_SCHEMA = "review-partition-manifest.v1"
AGGREGATE_SCHEMA = "review-aggregate.v1"

_DIFF_GIT_PREFIX = b"diff --git "


class Partition(FrozenModel):
    partition_id: str
    files: tuple[str, ...]
    file_classes: tuple[str, ...] = ()
    level: str = ReviewLevel.L2.value
    diff_hash: str | None = None
    char_count: int = 0
    diff_truncated: bool = False
    generated_files: tuple[str, ...] = ()
    lockfile_files: tuple[str, ...] = ()


class PartitionManifest(FrozenModel):
    schema_version: str = PARTITION_MANIFEST_SCHEMA
    policy_revision: str = REVIEW_POLICY_REVISION
    base_commit: str
    result_commit: str
    diff_hash: str | None = None
    partitions: tuple[Partition, ...] = ()
    generated_inventory: tuple[str, ...] = ()
    lockfile_inventory: tuple[str, ...] = ()
    truncated: bool = False
    total_files: int = 0
    total_chars: int = 0


def _unquote_git_path(quoted: bytes) -> bytes:
    """Decode a git C-quoted path (core.quotePath=true) back to raw bytes.

    Non-ASCII bytes arrive as octal escapes (``\\303\\251``); ``\\\\`` and
    ``\\"`` are literal. The result decodes with the same
    ``utf-8/surrogateescape`` convention as ``LocalGit.inspect`` file lists.
    """

    out = bytearray()
    index = 0
    while index < len(quoted):
        byte = quoted[index]
        if byte != 0x5C:  # backslash
            out.append(byte)
            index += 1
            continue
        index += 1
        if index >= len(quoted):
            raise IndependentReviewError("truncated escape in quoted diff path")
        nxt = quoted[index]
        if nxt in b"01234567":
            triple = quoted[index : index + 3]
            if len(triple) != 3 or any(c not in b"01234567" for c in triple):
                raise IndependentReviewError("truncated octal escape in quoted diff path")
            out.append(int(triple.decode("ascii"), 8))
            index += 3
            continue
        if nxt == 0x5C:
            out.append(0x5C)
        elif nxt == 0x22:
            out.append(0x22)
        elif nxt == ord("n"):
            out.append(0x0A)
        elif nxt == ord("t"):
            out.append(0x09)
        else:
            out.append(nxt)
        index += 1
    return bytes(out)


def _parse_header_path(header: bytes) -> str:
    """Return the b-side path of one ``diff --git`` header line.

    Handles both ``a/X b/X`` and C-quoted ``"a/X" "b/X"`` forms
    (``core.quotePath=true`` quotes every non-ASCII path). Raises instead of
    guessing: an unparseable header must never silently drop a file slice.
    """

    rest = header[len(_DIFF_GIT_PREFIX) :]
    if rest.startswith(b'"a/'):
        match = re.fullmatch(rb'"a/(.*)" "b/(.*)"', rest, flags=re.DOTALL)
        if match is None:
            raise IndependentReviewError(f"unparseable quoted diff header: {rest[:80]!r}")
        raw_b = _unquote_git_path(match.group(2))
        return raw_b.decode("utf-8", "surrogateescape")
    if not rest.startswith(b"a/"):
        raise IndependentReviewError(f"unparseable diff header: {rest[:80]!r}")
    body = rest[2:]
    # Renames are disabled in inspection, so a-path == b-path; accept the
    # split where both sides agree (robust to spaces in names).
    for index in range(len(body)):
        if body[index : index + 3] == b" b/":
            candidate_a, candidate_b = body[:index], body[index + 3 :]
            if candidate_a == candidate_b:
                return candidate_b.decode("utf-8", "surrogateescape")
    raise IndependentReviewError(f"unparseable diff header: {rest[:80]!r}")


def split_diff_by_file(diff: bytes) -> dict[str, bytes]:
    """Split a unified ``--binary`` diff into per-file slices.

    Falls back to ``{"__full__": diff}`` when no ``diff --git`` headers are
    present (e.g. empty or hand-built diffs in tests).
    """

    lines = diff.split(b"\n")
    starts: list[tuple[int, str]] = []
    offset = 0
    for line in lines:
        if line.startswith(_DIFF_GIT_PREFIX):
            starts.append((offset, _parse_header_path(line)))
        offset += len(line) + 1
    if not starts:
        return {"__full__": diff}
    # Byte-exact slices between header offsets (binary-safe).
    out: dict[str, bytes] = {}
    for position, (start, path) in enumerate(starts):
        end = starts[position + 1][0] if position + 1 < len(starts) else len(diff)
        out[path] = diff[start:end]
    return out


def validate_manifest(
    manifest: PartitionManifest,
    changed_files: Sequence[str],
    per_file: dict[str, bytes] | None = None,
) -> None:
    """Enforce coverage + overlap invariants (fail-closed).

    When the per-file diff slices are provided (and attributable, i.e. not
    the ``__full__`` fallback), coverage is checked by content: every
    required file must own a non-empty slice of the actual diff. A listed
    file with no delivered content raises instead of passing silently.
    """

    expected = tuple(sorted(changed_files))
    seen: list[str] = []
    for partition in manifest.partitions:
        if tuple(sorted(partition.files)) != partition.files:
            raise IndependentReviewError(
                f"partition {partition.partition_id} files are not deterministically ordered"
            )
        seen.extend(partition.files)
    if sorted(seen) != list(expected):
        raise IndependentReviewError(
            "partition manifest does not cover all required partitions "
            f"(expected {len(expected)} files, got {len(seen)})"
        )
    if len(set(seen)) != len(seen):
        raise IndependentReviewError("partition manifest has overlapping partitions")
    if per_file is not None and set(per_file.keys()) != {"__full__"}:
        missing = [path for path in seen if not per_file.get(path)]
        if missing:
            raise IndependentReviewError(
                "partition manifest lists files with no delivered diff content: "
                + ", ".join(missing[:5])
            )


def build_manifest(
    inspection: GitInspection,
    risk: RiskLevel,
    *,
    base_commit: str,
    result_commit: str,
    max_files_per_partition: int,
    max_chars_per_partition: int,
    l1_enabled: bool = True,
    policy_revision: str = REVIEW_POLICY_REVISION,
) -> PartitionManifest:
    """Deterministically pack sorted files into bounded partitions."""

    changed = tuple(sorted(inspection.changed_files))
    per_file = split_diff_by_file(inspection.diff)
    single_blob = set(per_file.keys()) == {"__full__"}
    if not single_blob:
        missing = [path for path in changed if path not in per_file]
        if missing:
            raise IndependentReviewError(
                "partition manifest has no diff content for required files: "
                + ", ".join(missing[:5])
            )

    file_classes = {path: classify_file(path) for path in changed}
    generated = tuple(sorted(p for p in changed if file_classes[p] is FileClass.GENERATED))
    lockfiles = tuple(sorted(p for p in changed if file_classes[p] is FileClass.LOCKFILE))

    # First-fit packing in sorted order (deterministic).
    buckets: list[list[str]] = []
    bucket_chars: list[int] = []
    for path in changed:
        size = len(inspection.diff) // max(len(changed), 1) if single_blob else len(per_file[path])
        placed = False
        for index, bucket in enumerate(buckets):
            if len(bucket) < max_files_per_partition and (
                bucket_chars[index] + size <= max_chars_per_partition or not bucket
            ):
                bucket.append(path)
                bucket_chars[index] += size
                placed = True
                break
        if not placed:
            buckets.append([path])
            bucket_chars.append(size)

    partitions: list[Partition] = []
    overall_truncated = len(buckets) > 1
    for number, bucket in enumerate(buckets):
        files = tuple(bucket)
        if single_blob:
            blob = per_file["__full__"]
            # Attribute an even slice only for accounting; the honest flag is
            # set when the whole diff does not fit one partition budget.
            char_count = len(blob) // len(buckets) if buckets else 0
            digest: str | None = hashlib.sha256(blob).hexdigest()
            truncated = overall_truncated
        else:
            blob = b"".join(per_file[path] for path in files)
            char_count = len(blob.decode("utf-8", "replace"))
            digest = hashlib.sha256(blob).hexdigest() if blob else None
            truncated = char_count > max_chars_per_partition
            if truncated:
                overall_truncated = True
        level = max(
            (level_for(risk, file_classes[path], l1_enabled=l1_enabled) for path in files),
            key=_level_rank,
            default=ReviewLevel.L2,
        )
        partitions.append(
            Partition(
                partition_id=f"p{number:02d}",
                files=files,
                file_classes=tuple(file_classes[path].value for path in files),
                level=level.value,
                diff_hash=digest,
                char_count=char_count,
                diff_truncated=truncated,
                generated_files=tuple(p for p in files if file_classes[p] is FileClass.GENERATED),
                lockfile_files=tuple(p for p in files if file_classes[p] is FileClass.LOCKFILE),
            )
        )

    manifest = PartitionManifest(
        policy_revision=policy_revision,
        base_commit=base_commit,
        result_commit=result_commit,
        diff_hash=inspection.diff_hash,
        partitions=tuple(partitions),
        generated_inventory=generated,
        lockfile_inventory=lockfiles,
        truncated=overall_truncated,
        total_files=len(changed),
        total_chars=len(inspection.diff.decode("utf-8", "replace")),
    )
    validate_manifest(manifest, changed, per_file)
    return manifest


def _level_rank(level: ReviewLevel) -> int:
    return {ReviewLevel.L0: 0, ReviewLevel.L1: 1, ReviewLevel.L2: 2, ReviewLevel.L3: 3}[level]


def verify_chunk_receipts(chunks: Sequence[object]) -> None:
    """Fail closed on duplicate or incomplete chunk batches.

    Each chunk must expose ``reference`` (``...:part-i-of-n``), ``content``
    and ``content_hash`` (sha256 of content). Duplicates, hash mismatches,
    missing parts or inconsistent totals raise ``IndependentReviewError``.
    """

    if not chunks:
        raise IndependentReviewError("review bundle has no chunks to verify")
    seen: dict[str, str] = {}
    totals: set[int] = set()
    indices: set[int] = set()
    pattern = re.compile(r":part-(\d+)-of-(\d+)$")
    for chunk in chunks:
        reference = str(getattr(chunk, "reference", ""))
        content = str(getattr(chunk, "content", ""))
        digest = str(getattr(chunk, "content_hash", ""))
        if reference in seen:
            raise IndependentReviewError(
                f"duplicate chunk receipt invalidates the review bundle: {reference}"
            )
        expected = hashlib.sha256(content.encode()).hexdigest()
        if digest != expected:
            raise IndependentReviewError(
                f"chunk hash mismatch invalidates the review bundle: {reference}"
            )
        match = pattern.search(reference)
        if match is None:
            raise IndependentReviewError(
                f"chunk reference is not a verifiable batch item: {reference}"
            )
        index, total = int(match.group(1)), int(match.group(2))
        seen[reference] = content
        totals.add(total)
        indices.add(index)
    if len(totals) != 1:
        raise IndependentReviewError("incomplete chunk batch: inconsistent totals")
    (total,) = tuple(totals)
    if total != len(chunks) or indices != set(range(1, total + 1)):
        raise IndependentReviewError("incomplete chunk batch: missing parts")
