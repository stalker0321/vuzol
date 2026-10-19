"""A required criterion needs a stored check of the exact candidate."""

from vuzol.workflows.acceptance import (
    criteria_are_proven,
    evidence_ref_for_criterion,
    exit_criterion_specs,
    unproven_criterion_ids,
)

COMMIT = "b" * 40
OTHER = "c" * 40


def _doc(*ids: str, commit: str = COMMIT) -> dict[str, object]:
    return {
        "result_commit": commit,
        "criteria": [
            {
                "criterion_id": criterion_id,
                "satisfied": True,
                "evidence_ref": f"gate:{criterion_id}:{commit}",
            }
            for criterion_id in ids
        ],
    }


def test_gate_binds_only_the_commit_it_measured() -> None:
    current = {"name": "done", "exit_code": 0, "result_commit": COMMIT}
    assert (
        evidence_ref_for_criterion(
            "done", result_commit=COMMIT, test_results=[current], findings=[]
        )
        == f"gate:done:{COMMIT}"
    )
    assert (
        evidence_ref_for_criterion(
            "done",
            result_commit=COMMIT,
            test_results=[{"name": "done", "exit_code": 0, "result_commit": OTHER}],
            findings=[],
        )
        is None
    )
    assert (
        evidence_ref_for_criterion(
            "done",
            result_commit=COMMIT,
            test_results=[{"name": "done", "exit_code": 0}],
            findings=[],
        )
        is None
    )


def test_review_finding_proves_only_with_its_own_commit() -> None:
    finding = {
        "criterion_id": "done",
        "classification": "criterion_pass",
        "evidence": "observed the shipped path",
        "acceptance_key": "aa" * 32,
        "result_commit": COMMIT,
    }
    assert (
        evidence_ref_for_criterion(
            "done", result_commit=COMMIT, test_results=[], findings=[finding]
        )
        == f"review-criterion:{'aa' * 32}:done:{COMMIT}"
    )
    claimed = {**finding, "classification": "human_accepted"}
    assert (
        evidence_ref_for_criterion(
            "done", result_commit=COMMIT, test_results=[], findings=[claimed]
        )
        is None
    )


def test_format_and_a_subset_are_not_proof() -> None:
    gates = (
        {"name": "done", "exit_code": 0, "result_commit": COMMIT},
        {"name": "clean", "exit_code": 0, "result_commit": COMMIT},
    )
    cited = _doc("done")
    assert (
        criteria_are_proven(
            cited,
            result_commit=COMMIT,
            required_ids=("done", "clean"),
            gates=gates,
            findings=(),
        )
        is False
    )
    assert (
        criteria_are_proven(
            cited,
            result_commit=COMMIT,
            required_ids=("done",),
            gates=(),
            findings=(),
        )
        is False
    )
    assert (
        criteria_are_proven(
            _doc("done", "clean"),
            result_commit=COMMIT,
            required_ids=("done", "clean"),
            gates=gates,
            findings=(),
        )
        is True
    )
    failed = ({"name": "done", "exit_code": 1, "result_commit": COMMIT},)
    assert (
        criteria_are_proven(
            _doc("done"),
            result_commit=COMMIT,
            required_ids=("done",),
            gates=failed,
            findings=(),
        )
        is False
    )


def test_specs_keep_the_statement_and_do_not_invent_one() -> None:
    specs = exit_criterion_specs(
        [
            {"criterion_id": "keeps-saved-work", "statement": "  data survives restart  "},
            {"criterion_id": "done"},
            {"criterion_id": "bad:id", "statement": "ignored"},
        ]
    )
    assert [(item.criterion_id, item.statement) for item in specs] == [
        ("keeps-saved-work", "data survives restart"),
        ("done", None),
    ]
    missing = unproven_criterion_ids(
        result_commit=COMMIT,
        required_ids=tuple(item.criterion_id for item in specs),
        gates=({"name": "done", "exit_code": 0, "result_commit": COMMIT},),
        findings=(),
    )
    assert missing == ("keeps-saved-work",)
