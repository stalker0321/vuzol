"""Stable semantic decision interface (D4 W1).

Closed value sets linking what the model proposed with what deterministic
policy allows. Hints never grant authority: capabilities, approvals, review
floors and execution eligibility stay in ``policy.py`` / ``review/policy.py``.

Ownership mapping follows ARCHITECTURE_REVIEW §5.1:

- explicit code / ingress state owns command, reply target, principal,
  project and active IDs (before any model);
- bounded semantic classifier proposes effect/relation, policy tightens;
- Jev may only choose an opaque candidate ID after deterministic selection;
  confidence is never permission;
- context needs are hints + mandatory policy minima.
"""

from __future__ import annotations

from enum import StrEnum

from vuzol.interpretation.domain import FrozenModel, TaskAction, TaskDraft, TaskOperation


class EffectIntent(StrEnum):
    """What the turn wants to change in the world. Closed set."""

    INSPECT_CODE = "inspect_code"
    MODIFY_CODE = "modify_code"
    DESIGN_ADVICE = "design_advice"
    PROVISION_PROJECT = "provision_project"
    CONTROL_LIFECYCLE = "control_lifecycle"
    ANSWER_QUESTION = "answer_question"
    DISCUSS_ONLY = "discuss_only"


class RelationHint(StrEnum):
    """How the turn relates to existing work. Closed set."""

    CREATES_TASK = "creates_task"
    CONTINUES_TASK = "continues_task"
    CONTROLS_PACKAGE = "controls_package"
    PROVISIONS_PROJECT = "provisions_project"
    QUERIES_ONLY = "queries_only"


class ContextHint(StrEnum):
    """Where the signal came from. Closed set, no vendor names."""

    EXPLICIT_COMMAND = "explicit_command"
    REPLY_AFFINITY = "reply_affinity"
    SLASH_COMMAND = "slash_command"
    DISCUSSION_FREE_TEXT = "discussion_free_text"


class SemanticDecision(FrozenModel):
    """Model proposal bound to the policy verdict. Advisory only."""

    effect: EffectIntent
    relation: RelationHint
    context: ContextHint
    policy_allowed: bool = False


def classify_decision(draft: TaskDraft, *, explicit: bool = False) -> SemanticDecision:
    """Derive a closed decision triple from a validated draft.

    Deterministic and side-effect free. ``explicit`` reflects ingress state
    (explicit task command / direct-task create / slash), never model output.
    ``policy_allowed`` defaults to False; only policy code may set True.
    """

    if draft.action is TaskAction.CREATE_PROJECT:
        effect = EffectIntent.PROVISION_PROJECT
        relation = RelationHint.PROVISIONS_PROJECT
    elif draft.action is TaskAction.CONTINUE_TASK:
        effect = EffectIntent.CONTROL_LIFECYCLE
        relation = RelationHint.CONTINUES_TASK
    elif draft.action in {
        TaskAction.PAUSE_TASK,
        TaskAction.RESUME_TASK,
        TaskAction.CANCEL_TASK,
        TaskAction.APPROVE_STEP,
        TaskAction.REJECT_STEP,
    }:
        effect = EffectIntent.CONTROL_LIFECYCLE
        relation = RelationHint.CONTROLS_PACKAGE
    elif draft.action is TaskAction.ANSWER_QUESTION:
        effect = EffectIntent.ANSWER_QUESTION
        relation = RelationHint.QUERIES_ONLY
    elif draft.action is TaskAction.GENERAL_CONVERSATION:
        effect = EffectIntent.DISCUSS_ONLY
        relation = RelationHint.QUERIES_ONLY
    elif draft.operation in {TaskOperation.INSPECT, TaskOperation.EXPLAIN}:
        effect = EffectIntent.DESIGN_ADVICE
        relation = RelationHint.CREATES_TASK
    else:
        effect = EffectIntent.MODIFY_CODE
        relation = RelationHint.CREATES_TASK
    context = ContextHint.EXPLICIT_COMMAND if explicit else ContextHint.DISCUSSION_FREE_TEXT
    return SemanticDecision(effect=effect, relation=relation, context=context)
