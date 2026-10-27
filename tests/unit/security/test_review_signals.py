"""Mechanical review signal tests."""

from __future__ import annotations

from vuzol.review.signals import scan_suspicious_patterns


def test_suspicious_pattern_report_has_locations_and_classifications() -> None:
    findings = scan_suspicious_patterns(
        {
            "tests/test_bad.py": "def test_bad():\n    assert call() or True\n",
            "src/bad.py": "try:\n    work()\nexcept Exception: pass\n",
        }
    )
    assert {(item.path, item.line, item.classification) for item in findings} == {
        ("tests/test_bad.py", 2, "forced_success"),
        ("src/bad.py", 3, "exception_swallowing"),
    }
