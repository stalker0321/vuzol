"""The goal checker reads the criterion description, not a plan item id."""

from vuzol.leadership.evidence import judge_description


def test_marker_description_is_cited_by_the_marker_file() -> None:
    satisfied, citations = judge_description(
        "ship the marker",
        {"README.md": "# Before\n", "MARKER.txt": "marker-a\nrepaired\n"},
    )
    assert satisfied
    assert citations
    assert citations[0]["path"] == "MARKER.txt"


def test_oauth_description_is_not_satisfied_by_marker_files() -> None:
    satisfied, citations = judge_description(
        "Working OAuth login",
        {"MARKER.txt": "marker-a\nrepaired\n", "SECOND.txt": "marker-b\n"},
    )
    assert not satisfied
    assert citations == []


def test_description_without_a_distinctive_word_fails_closed() -> None:
    satisfied, citations = judge_description("ship", {"MARKER.txt": "marker-a\n"})
    assert not satisfied
    assert citations == []
