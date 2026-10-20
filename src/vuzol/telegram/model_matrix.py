"""Project role picker over configured profiles, independent of model families."""

import hashlib
import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from vuzol.config import RegistryError, RuntimeConfiguration
from vuzol.config.models import ProviderProfileConfig
from vuzol.config.revision import content_revision
from vuzol.projects.executor_preference import ExecutorPreferenceError, ensure_preference_row
from vuzol.projects.model_matrix import (
    SLOT_LABELS,
    ModelAssignment,
    assignment_snapshot,
    eligible_profiles,
    set_assignment,
)
from vuzol.storage.models import TransactionalOutbox
from vuzol.telegram.domain import ControlUpdate
from vuzol.telegram.projections import telegram_html


def profile_token(profile: ProviderProfileConfig) -> str:
    # Long profile IDs still fit Telegram's 64-byte callback limit. Include the
    # revision so a catalog edit cannot silently change a displayed choice.
    return hashlib.sha256(f"{profile.id}:{content_revision(profile)}".encode()).hexdigest()[:16]


def profile_label(profile: ProviderProfileConfig) -> str:
    account = profile.account_id or profile.runtime_identity or profile.id
    return (
        f"{profile.provider}/{profile.model} · {profile.id} · "
        f"{account} · {profile.launch_mode.value}"
    )


def default_label(runtime: RuntimeConfiguration, slot: str) -> str:
    configured = {
        "lead": runtime.settings.project_lead.profile_id,
        "goal_reviewer": runtime.settings.project_lead.reviewer_profile_id,
        "reviewer_light": runtime.settings.execution.reviewer_profile_id,
        "reviewer_strong": runtime.settings.execution.reviewer_profile_id,
        "executor": runtime.settings.execution.default_executor_profile_id,
    }.get(slot)
    if isinstance(configured, str):
        try:
            profile = runtime.registries.profiles.get(configured)
        except (RegistryError, LookupError):
            return f"{configured} · недоступен"
        label = profile_label(profile)
        if profile not in eligible_profiles(runtime.registries, slot):
            label += " · недоступен для роли"
        return label
    return "автоматический выбор по политике"


async def enqueue_matrix(
    session: AsyncSession,
    *,
    runtime: RuntimeConfiguration,
    project_id: str,
    chat_id: int,
    thread_id: int,
    identity: uuid.UUID,
    slot: str | None = None,
    entity_type: str = "telegram_control_action",
) -> None:
    if slot is not None and slot not in SLOT_LABELS:
        raise ExecutorPreferenceError("unknown model role")
    row = await ensure_preference_row(session, project_id)
    assignments = assignment_snapshot(row.model_assignments or {})
    text = [f"<b>Модели проекта</b> · {telegram_html(project_id)}"]
    buttons: list[tuple[tuple[str, str], ...]] = []
    for name, label in SLOT_LABELS.items():
        selected = assignments.get(name)
        if selected:
            pinned = ModelAssignment.model_validate(selected)
            try:
                profile = runtime.registries.profiles.get(pinned.profile_id)
                current = content_revision(profile) == pinned.content_revision
            except (RegistryError, LookupError):
                current = False
            value = (
                profile_label(profile)
                if current
                else f"{pinned.model} · {pinned.profile_id} · профиль изменён или недоступен"
            )
        else:
            value = f"по умолчанию: {default_label(runtime, name)}"
        text.append(f"{label}: <b>{telegram_html(value)}</b>")
        if slot is None:
            buttons.append(((label, f"v3:pm:s:{row.revision}:{name}"),))
    if slot is not None:
        text.extend(["", f"Выберите: {SLOT_LABELS[slot]}"])
        buttons.append((("По умолчанию", f"v3:pm:p:{row.revision}:{slot}:auto"),))
        for profile in eligible_profiles(runtime.registries, slot):
            buttons.append(
                (
                    (
                        profile_label(profile),
                        f"v3:pm:p:{row.revision}:{slot}:{profile_token(profile)}",
                    ),
                )
            )
        buttons.append((("Назад", f"v3:pm:b:{row.revision}"),))
    text.extend(
        ["", "Light/strong выбирает система. Изменения применяются к новым запускам и обращениям."]
    )
    session.add(
        TransactionalOutbox(
            destination="telegram",
            operation_type="send_message",
            linked_entity_type=entity_type,
            linked_entity_id=identity,
            idempotency_key=f"telegram:model_matrix:{identity}:{row.revision}:{slot or 'menu'}",
            payload={
                "role": "project_model_picker",
                "chat_id": chat_id,
                "message_thread_id": thread_id,
                "project_id": project_id,
                "revision": row.revision,
                "html": "\n".join(text),
                "callback_buttons": buttons,
            },
        )
    )


async def apply_matrix(
    session: AsyncSession,
    *,
    runtime: RuntimeConfiguration,
    update: ControlUpdate,
    project_id: str,
    identity: uuid.UUID,
) -> None:
    if update.preference_revision is None or update.message_thread_id is None:
        raise ExecutorPreferenceError("model preference target is incomplete")
    if (
        update.action_kind != "project_model_matrix_menu"
        and update.preference_slot not in SLOT_LABELS
    ):
        raise ExecutorPreferenceError("model role is missing")
    row = await ensure_preference_row(session, project_id)
    if row.revision != update.preference_revision:
        raise ExecutorPreferenceError("model options are stale; send /model again")
    slot = update.preference_slot
    if update.action_kind == "project_model_matrix_pick":
        assert slot is not None
        profile_id = None
        if update.preference_profile_token is not None:
            matches = [
                profile
                for profile in eligible_profiles(runtime.registries, slot)
                if profile_token(profile) == update.preference_profile_token
            ]
            if len(matches) != 1:
                raise ExecutorPreferenceError("model options changed; send /model again")
            profile_id = matches[0].id
        await set_assignment(
            session,
            project_id=project_id,
            slot=slot,
            profile_id=profile_id,
            expected_revision=update.preference_revision,
            user_id=update.user_id,
            registries=runtime.registries,
        )
        slot = None
    await enqueue_matrix(
        session,
        runtime=runtime,
        project_id=project_id,
        chat_id=update.chat_id,
        thread_id=update.message_thread_id,
        identity=identity,
        slot=slot,
    )
