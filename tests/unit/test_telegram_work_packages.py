import uuid

import pytest

from vuzol.telegram.work_packages import (
    WorkPackageCallback,
    WorkPackageCallbackError,
    WorkPackageCallbackKind,
    encode_work_package_callback,
    parse_work_package_callback,
)

PACKAGE_ID = uuid.UUID("12345678-1234-5678-9abc-def012345678")


@pytest.mark.parametrize("kind", list(WorkPackageCallbackKind))
def test_wp_cb_v1_round_trip_and_budget(kind: WorkPackageCallbackKind) -> None:
    value = (
        999
        if kind
        in {
            WorkPackageCallbackKind.OPEN_ITEM,
            WorkPackageCallbackKind.OPEN_EDIT,
            WorkPackageCallbackKind.SET_PAGE,
        }
        else None
    )
    callback = WorkPackageCallback(kind, PACKAGE_ID, 9_999_999_999, "abcdef01", value)

    encoded = encode_work_package_callback(callback)

    assert len(encoded.encode()) <= 64
    assert parse_work_package_callback(encoded) == callback


@pytest.mark.parametrize(
    "value",
    [
        "",
        "v1:wp:Q:12345678123456789abcdef012345678:1:abcdef01",
        "v1:wp:A:12345678-1234-5678-9abc-def012345678:1:abcdef01",
        "v1:wp:A:12345678123456789ABCDEF012345678:1:abcdef01",
        "v1:wp:A:12345678123456789abcdef012345678:0:abcdef01",
        "v1:wp:A:12345678123456789abcdef012345678:01:abcdef01",
        "v1:wp:A:12345678123456789abcdef012345678:1:ABCDEF01",
        "v1:wp:A:12345678123456789abcdef012345678:1:2:abcdef01",
        "v1:wp:I:12345678123456789abcdef012345678:1:0:abcdef01",
        "v1:wp:I:12345678123456789abcdef012345678:1:1000:abcdef01",
        "v1:wp:I:12345678123456789abcdef012345678:1:1:abcdef01:extra",
        "x" * 65,
    ],
)
def test_wp_cb_v1_rejects_bad_wire(value: str) -> None:
    with pytest.raises(WorkPackageCallbackError):
        parse_work_package_callback(value)


def test_wp_cb_v1_rejects_wrong_value_shape_when_encoding() -> None:
    with pytest.raises(WorkPackageCallbackError, match="unexpected_value"):
        WorkPackageCallback(WorkPackageCallbackKind.APPROVE, PACKAGE_ID, 1, "abcdef01", 1)
    with pytest.raises(WorkPackageCallbackError, match="value_out_of_range"):
        WorkPackageCallback(WorkPackageCallbackKind.OPEN_ITEM, PACKAGE_ID, 1, "abcdef01")
