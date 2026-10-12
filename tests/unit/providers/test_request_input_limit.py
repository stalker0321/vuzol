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
        == 90_000
    )


def test_provider_input_over_40k_is_rejected_not_truncated() -> None:
    data = provider_request().model_dump()
    data["original_input"] = "я" * 40_001

    with pytest.raises(ValidationError) as caught:
        ProviderRequest.model_validate(data)

    error = caught.value.errors(include_input=False)[0]
    assert error["type"] == "value_error"
    assert "40000" in error["msg"]


@pytest.mark.parametrize("length", [40_001, 50_000, 90_000])
def test_document_partition_is_additive_and_token_guards_unchanged(length):
    data = provider_request().model_dump()
    data.update(original_input="я" * length, document_text_chars=min(length, 50_000))
    parsed = ProviderRequest.model_validate(data)
    assert parsed.original_input == data["original_input"]
    assert parsed.max_input_tokens == data["max_input_tokens"]
    assert parsed.max_output_tokens == data["max_output_tokens"]
    assert ProviderRequest.model_validate_json(parsed.model_dump_json()) == parsed


@pytest.mark.parametrize("length,documents", [(90_001, 50_000), (80_001, 40_000), (90_000, 50_001)])
def test_document_partition_does_not_relax_other_input_guards(length, documents):
    data = provider_request().model_dump()
    data.update(original_input="я" * length, document_text_chars=documents)
    with pytest.raises(ValidationError):
        ProviderRequest.model_validate(data)


def test_provider_input_can_still_be_reference_only() -> None:
    data = provider_request().model_dump()
    data["original_input"] = None
    assert ProviderRequest.model_validate(data).original_input is None


def test_document_measurement_cannot_exceed_included_input():
    data = provider_request().model_dump()
    data.update(original_input="short", document_text_chars=50_000)
    with pytest.raises(ValidationError, match="document text count"):
        ProviderRequest.model_validate(data)
