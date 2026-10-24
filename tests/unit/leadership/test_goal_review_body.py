from types import SimpleNamespace

# Known import cycle (leadership.persistence -> storage -> providers -> discussion ->
# leadership.persistence) when a leadership module is imported first; see BACKLOG.
import vuzol.storage  # noqa: F401
from vuzol.leadership.verification import review_body


def goal(scope_revision):
    return SimpleNamespace(
        scope_revision=scope_revision,
        original_request="Добавить parse_duration",
        scope_text="Реализовать format_duration и примеры в README",
        criteria=[{"id": "c1", "description": "README: примеры"}],
    )


def test_unamended_goal_is_reviewed_against_the_request():
    body = review_body(goal(1), "a" * 40, {"README.md": "x"}, None)
    assert body["request"] == "Добавить parse_duration"
    assert "original_request_superseded" not in body


def test_amended_goal_is_reviewed_against_the_current_scope():
    # Live pilot: the reviewer judged the amended goal against its first wording.
    body = review_body(goal(4), "a" * 40, {"README.md": "x"}, None)
    assert body["request"] == "Реализовать format_duration и примеры в README"
    assert body["original_request_superseded"] == "Добавить parse_duration"
    assert body["scope_revision"] == 4 and body["runtime_validation"] == []
