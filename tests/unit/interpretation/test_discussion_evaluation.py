from pathlib import Path

from vuzol.interpretation.discussion_evaluation import (
    DISCUSSION_EVALUATION_VERSION,
    evaluate_discussion_fixtures,
    load_discussion_fixtures,
)


def test_versioned_discussion_matrix_has_zero_safety_violations() -> None:
    path = Path(__file__).parents[2] / "fixtures" / "interpretation" / "discussion-eval-v1.json"

    fixtures = load_discussion_fixtures(path)
    report = evaluate_discussion_fixtures(fixtures)

    assert len(fixtures) >= 8
    assert {fixture.category for fixture in fixtures} >= {
        "free_chat",
        "query_only",
        "task_request",
        "advisory_control",
        "ambiguity",
        "voice",
        "item_edit",
    }
    assert report.version == DISCUSSION_EVALUATION_VERSION
    assert report.passed == report.total
    assert report.pass_rate == 1
    assert report.false_task_creation_violations == 0
    assert report.authoritative_control_violations == 0
    assert report.illegal_plan_mutation_violations == 0
    assert report.edit_fence_violations == 0
    assert report.policy_contract_passed
