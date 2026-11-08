"""Dream responses must partition the complete notebook without growing it."""

import uuid

import pytest
from pydantic import ValidationError

from vuzol.config.settings import ProjectLeadSettings
from vuzol.discussion.domain import DomainError
from vuzol.leadership.dreaming_model import DreamResponse, validate_dream
from vuzol.leadership.provider_model import strict_response_schema

A, B, C = (str(uuid.uuid4()) for _ in range(3))
NOTES = [{"id": A, "text": "fact"}, {"id": B, "text": "old fact"}]


def test_defaults_and_strict_schema():
    assert not ProjectLeadSettings().dreaming_enabled
    assert ProjectLeadSettings().dreaming_hour == 4
    with pytest.raises(ValidationError):
        ProjectLeadSettings(dreaming_hour=24)
    schema = strict_response_schema(DreamResponse)
    assert schema["additionalProperties"] is False
    assert set(schema["required"]) == {"keep_ids", "merge", "drop"}


@pytest.mark.parametrize(
    "payload",
    [
        {"keep_ids": [A]},
        {"keep_ids": [A, A, B]},
        {"keep_ids": [A, B, C]},
        {"keep_ids": [A], "drop": [{"id": A, "reason": "duplicate"}]},
        {"merge": [{"replaces": [A, B], "text": "я" * 10}]},
        {"merge": [{"replaces": [A, B], "text": "password=supersecret123"}]},
        {"merge": [{"replaces": [A, B], "text": "x" * 601}]},
        {"merge": [{"replaces": [], "text": "fact"}], "keep_ids": [A, B]},
    ],
)
def test_invalid_partition_or_text(payload):
    with pytest.raises((ValueError, DomainError)):
        response = DreamResponse.model_validate(
            {"keep_ids": [], "merge": [], "drop": [], **payload}
        )
        validate_dream(response, NOTES)


def test_merge_drop_and_keep():
    response = DreamResponse.model_validate(
        {
            "keep_ids": [],
            "merge": [{"replaces": [A], "text": "fact"}],
            "drop": [{"id": B, "reason": "stale"}],
        }
    )
    validate_dream(response, NOTES)
