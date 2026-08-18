import pytest

from vuzol.projects.importing import ProjectImportError, parse_github_repository_url


def test_github_repository_url_derives_project_identity() -> None:
    imported = parse_github_repository_url("https://github.com/example/three-body-problems.git")

    assert imported.url == "https://github.com/example/three-body-problems.git"
    assert imported.project_id == "three-body-problems"
    assert imported.display_name == "Three Body Problems"


@pytest.mark.parametrize(
    "value",
    (
        "file:///srv/repository",
        "https://user:token@github.com/example/repository",
        "https://127.0.0.1/example/repository",
        "https://github.com/example/repository/tree/main",
        "https://github.com/example/repository?token=value",
        "https://github.com/1_bad/repository",
    ),
)
def test_repository_url_rejects_unsafe_or_ambiguous_sources(value: str) -> None:
    with pytest.raises(ProjectImportError):
        parse_github_repository_url(value)
