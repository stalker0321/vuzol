"""A proven-not-applied intent must become active before an approved retry."""

import uuid
from dataclasses import replace
from unittest.mock import AsyncMock, MagicMock

import pytest

from vuzol.execution.effect import EffectIntent, record_intent
from vuzol.storage.models import Effect


@pytest.mark.anyio
@pytest.mark.parametrize("drift", [None, "payload_hash", "approval_id", "context", "lease"])
async def test_only_identical_denied_apply_can_rearm_before_retry(drift: str | None) -> None:
    intent = EffectIntent(
        operation_key="apply:approved",
        step_id=uuid.uuid4(),
        task_id=uuid.uuid4(),
        run_id=uuid.uuid4(),
        effect_class="isolated_mutation",
        target_kind="git_ref",
        target_reference="refs/heads/main",
        idempotency="reconcilable",
        payload_hash="a" * 64,
        lease_generation=2,
        approval_id=uuid.uuid4(),
        approval_envelope_hash="a" * 64,
        permission_envelope_hash="a" * 64,
        context={"expected_head": "base", "result_commit": "result"},
    )
    effect = Effect(
        id=uuid.uuid4(),
        **{
            key: getattr(intent, key)
            for key in (
                "step_id",
                "task_id",
                "run_id",
                "target_kind",
                "effect_class",
                "idempotency",
                "target_reference",
                "payload_hash",
                "approval_id",
                "approval_envelope_hash",
                "permission_envelope_hash",
                "context",
            )
        },
        lease_generation=1,
        status="failed",
        reconcile_status="denied",
        receipt_status="failed",
    )
    session = MagicMock(scalar=AsyncMock(return_value=effect), flush=AsyncMock())
    if drift == "lease":
        with pytest.raises(ValueError, match="newer lease"):
            await record_intent(session, replace(intent, lease_generation=1))
        assert effect.status == "failed"
        session.add.assert_not_called()
    elif drift:
        values = {"payload_hash": "b" * 64, "approval_id": uuid.uuid4(), "context": {}}
        with pytest.raises(ValueError, match="does not match"):
            await record_intent(session, replace(intent, **{drift: values[drift]}))
        assert effect.status == "failed"
        session.add.assert_not_called()
    else:
        assert await record_intent(session, intent) is effect
        assert effect.status == "dispatched"
        assert effect.reconcile_status == "not_started"
        assert effect.receipt_status is None
        assert effect.launch_generation == effect.lease_generation == 2
        assert effect.launch_started_at is not None
        assert session.add.call_args.args[0].event_type == "effect.retry_dispatched"
