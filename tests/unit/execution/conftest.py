"""Host prerequisites for tests that exercise real POSIX ACLs."""

from pathlib import Path

import pytest


@pytest.fixture
def mapped_acl_ids() -> None:
    # A single-UID user namespace cannot put the test's named users in an ACL.
    # Hosted CI has the normal full UID/GID maps and executes these checks.
    for kind in ("uid", "gid"):
        ranges = [
            (int(start), int(length))
            for start, _outside, length in (
                line.split() for line in Path(f"/proc/self/{kind}_map").read_text().splitlines()
            )
        ]
        if any(
            not any(start <= identity < start + length for start, length in ranges)
            for identity in (0, 60_001, 60_002, 60_003)
        ):
            pytest.skip(f"POSIX ACL tests require mapped {kind}s 0 and 60001-60003")
