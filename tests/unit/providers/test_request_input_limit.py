"""The temporary provider input guard preserves complete discussion prompts."""

import pytest
from pydantic import ValidationError

from tests.unit.providers._test_providers_helpers import provider_request
from vuzol.providers.domain import ProviderRequest


@pytest.mark.parametrize("length", [0, 20_000, 20_001, 21_651, 40_000])
def test_provider_input_up_to_40k_survives_validation_and_serialization(length: int) -> None:
    suffix = " Если условие не выполнено, ничего не меняй; пока не запускай."
    raw = "" if length == 0 else "я" * (length - len(suffix)) + suffix
    data = provider_request().model_dump()
    data["original_input"] = raw

    request = ProviderRequest.model_validate(data)
    restored = ProviderRequest.model_validate_json(request.model_dump_json())

    assert len(raw) == length
    assert restored.original_input == raw
    assert restored.max_input_tokens == data["max_input_tokens"]
    assert restored.max_output_tokens == data["max_output_tokens"]
    assert (
        ProviderRequest.model_json_schema()["properties"]["original_input"]["anyOf"][0]["maxLength"]
        == 40_000
    )


def test_provider_input_over_40k_is_rejected_not_truncated() -> None:
    data = provider_request().model_dump()
    data["original_input"] = "я" * 40_001

    with pytest.raises(ValidationError) as caught:
        ProviderRequest.model_validate(data)

    error = caught.value.errors(include_input=False)[0]
    assert error["loc"] == ("original_input",)
    assert error["type"] == "string_too_long"
    assert error["ctx"] == {"max_length": 40_000}


def test_provider_input_can_still_be_reference_only() -> None:
    data = provider_request().model_dump()
    data["original_input"] = None
    assert ProviderRequest.model_validate(data).original_input is None
