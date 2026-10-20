# Повторная сдача этапа 2: R8 / R9

20 октября 2026. Только исправление двух findings из
`/home/vodkolyan/vuzol-audit-2026-10-20/REVIEW_ASTRA_STAGE2.md`.
Это handoff для независимого ревью, не разрешение на пилот или production.

## Исходное состояние и изоляция

- Ветка `agent/project-lead-v1`, исходный HEAD
  `c8b21aaefea79bb90ac22eff501134bae5425a9a`, рабочее дерево было чистым.
- Полностью прочитаны отчёт и оба внешних probe. Старые findings R1–R7 не
  объявлялись открытыми заново. Новую архитектуру и этапы 3–5 не начинали.
- Оба probe воспроизведены до изменений: **2 failed на assertions, 6.62s**.
  R8: три lead prompt не содержали ни одного сохранённого отрицательного
  объяснения. R9: один вызов модели, active goal, ни пакета, ни продолжения.
- Создан новый контейнер `vuzol-feedback-ksapan`, label
  `vuzol.disposable=feedback-ksapan`, из уже имеющегося
  `postgres:16.13-bookworm`, без pull. Порт `127.0.0.1:32770`,
  БД `vuzol_feedback_test`, пользователь `vuzol_feedback`.
  Проверены имя БД и пустая public schema, затем применены миграции.
- Использован прежний отдельный `/tmp/vuzol-stage2.jzwfpD/venv`.
  Production venv, работающие сервисы, пользовательские задачи и внешние
  review-файлы не изменялись. Платных вызовов, реальных TG mutations,
  host installs, push и deploy не было.
- После всех прогонов контейнер и его собственный anonymous data volume удалены.
  Тестовая БД не сохранена; данные воспроизводятся fixtures, резервной копии нет.
  Пустой временный каталог `/tmp/vuzol-feedback.KSaPaN` удалён; прежний venv
  сохранён. Чужие контейнеры/БД не затронуты.

## R8: результат проверки поступает руководителю

`leadership/feedback.py` строит `LeadSnapshot.result_context` из существующих
authoritative rows. `ProviderLeadModel` передаёт этот JSON в фактический
`ProviderRequest.original_input`; prompt revision — `project-lead-prompt.v2`.
Это не неиспользуемый getter и не объявление несуществующего read tool.

Контекст содержит:

- goal/scope/package/head binding;
- текущий сохранённый proposed plan и immutable plan revision исполнявшегося
  пакета с ID и content hash;
- статус пакета, pause reason и failure task;
- implementation summary, structured failure/category/unknown effects,
  workflow review findings с step/run/task refs;
- per-criterion verdict, explanation, evidence paths, evidence ID,
  artifact digest и result commit.

Отбор evidence проверяет goal, актуальный завершённый пакет, живой integration
ref, scope revision и описание критерия. Старые записи не смешиваются с текущими:
передаётся `stale_evidence_count`, но не их объяснения под видом текущей проверки.
`satisfied_criteria` в snapshot тоже проверяет живой ref.
Workflow findings помечаются как `current_head`, `historical_commit` или
`run_only`; старое замечание не становится доказательством текущего результата.

Бюджет проекции — **16 000 UTF-8 bytes**. До восьми criterion reviews и восьми
workflow steps; отрицательные verdicts приоритетнее положительных. План показывает
до четырёх элементов; списки findings/evidence paths/criteria также ограничены.
Есть явные omission counts, `[truncated]` и общий флаг усечения.
Недостающий контекст не дорисовывается.

Выбираются только известные поля, а не весь payload/observation: owner identity,
отдельные credential fields, host repository path, полный source snapshot и логи
не копируются. Текст результатов по-прежнему недоверенный; он не выдаёт права,
не снимает approval и не изменяет критерии приёмки.

Проверено не только присутствие текста: новый fake transport выбирает следующий
исправляющий пакет **на основании конкретного замечания из provider input**.
Отрицательное объяснение и его evidence ID входят в сохранённое решение.
Никакого owner answer между замечанием и доработкой нет.

## R9: принятый nonterminal turn имеет продолжение

Выбран вариант **durable continuation**, а не запрет одиночного `propose_batch`.

В транзакции `_ack`, под goal lock, вместе с завершением текущего outbox/turn
проверяется, есть ли:

1. terminal/paused goal;
2. работающий пакет, которым управляет sequencer;
3. сохранённое ожидание/вопрос;
4. другой pending/leased lead wakeup.

Если active goal не имеет ни одного такого пути, сохраняется один wakeup
`lead:wakeup:<goal>:continue:<version>`. Ack, счётчик и continuation коммитятся
или откатываются вместе. Immutable commands, expected versions и receipts
не переписываются. Новый worker видит proposed plan из БД и может выполнить
`start_batch` следующим turn.

Reconciler восстанавливает также прежний тупик `active + saved proposal +
delivered enabled wakeup`. Он не сбрасывает delivered rows и не создаёт
безусловный периодический inference. Два конкурирующих reconciler создают
один continuation под общим goal lock.

Защита от цикла:

- используется существующий `repeat_limit`;
- при default `2` допускаются два continuation без нового внешнего прогресса;
  третий подряд nonterminal/no-job ответ переводит цель в waiting;
- переименование предложения не обнуляет счётчик;
- новым прогрессом считаются scope/package version/head или новый owner answer,
  но не собственное увеличение goal version от очередного предложения;
- blocker `continuation_without_progress` имеет durable owner question
  с понятным текстом и обычной привязкой к goal/question version.

Pause/revoke до обработки уже сохранённого continuation предотвращают новый
inference/запуск. Работающий пакет не получает лишний параллельный lead turn.

## Проверки

Исходные внешние probes после исправления: **2 passed, 6.67s**;
финальный повтор без изменения внешнего файла: **2 passed, 8.24s**.
Их проверки перенесены в
`tests/integration/leadership/test_feedback_continuation.py`.
R9 дополнительно требует ровно два вызова модели и один пакет.

Новый suite проверяет:

- actual provider input: отрицательное замечание → исправляющий proposal →
  отдельный continuation → start → положительный verdict;
- fresh worker между proposal и start;
- crash до ack, после enqueue внутри транзакции и после commit;
- ровно один continuation, один пакет, исходные receipt preconditions после replay;
- восстановление старого orphan state двумя конкурентными reconciler;
- одинаковые и переименованные предложения без бесконечного inference;
- pause/revoke уже поставленного continuation;
- stale scope/description/package/head, structured blocker и ограничение размера;
- allowlist projection без копирования произвольных identity/credential fields.

Первый расширенный прогон: 80 passed, 1 failed в новом тесте подготовки
structured blocker. Причина: fixture изменила ORM rows, но не сделала flush при
отключённом autoflush. Добавлен flush только в fixture, assertions сохранены.
Повтор этого теста и конкурентного reconciler: **2 passed, 9.72s**.

Полный unit suite: **2874 passed, 1 skipped, 3 warnings, 34.98s**.
Skip — прежний `test_remote_execution_not_verified` (remote execution вне scope).
Ruff — passed; mypy leadership — **22 source files, no errors**.

Полный integration suite без Docker-marked tests:
**829 passed, 1 skipped, 15 deselected, 13 warnings, 540.51s**.
Skip — `test_completed_plan_enqueues_system_trace` (per-task planner отключён).
15 Docker-marked tests не запускались. 12 warnings — Telegram deprecation;
ещё один — SQLAlchemy non-checked-in connection в
`test_terminal_projections_sequence_work_package_observation_once`.
Этот warning был и в предыдущей сдаче; здесь он не исправлялся.

Итоговый focused leadership suite: **81 passed, 97.39s** (unit + integration).
Это прежние 66 проверок плюс 15 новых (13 integration cases и 2 unit).
Отдельный сквозной прогон с печатью trace: **1 passed, 10.73s**.
Live providers/TG и coverage floor не считаются проверенными.

## Точные команды

Из корня репозитория, последовательно для одной disposable БД:

```sh
export UV_PROJECT_ENVIRONMENT=/tmp/vuzol-stage2.jzwfpD/venv
export PYTHONPATH=src:.
export PYTHONDONTWRITEBYTECODE=1
export VUZOL_TEST_DATABASE_DSN='postgresql://vuzol_feedback:feedback-disposable-only@127.0.0.1:32770/vuzol_feedback_test'

VUZOL_DATABASE_DSN_REFERENCE=env:VUZOL_FEEDBACK_DSN \
  VUZOL_FEEDBACK_DSN='postgresql+psycopg://vuzol_feedback:feedback-disposable-only@127.0.0.1:32770/vuzol_feedback_test' \
  /tmp/vuzol-stage2.jzwfpD/venv/bin/alembic upgrade head

uv run --frozen pytest -c pyproject.toml \
  /home/vodkolyan/vuzol-audit-2026-10-20/test_lead_feedback.py \
  --no-cov -p no:cacheprovider -q --tb=short
uv run --frozen pytest tests/unit --no-cov -p no:cacheprovider -q --tb=short
uv run --frozen pytest tests/integration -m 'not docker' \
  --no-cov -p no:cacheprovider -q --tb=short
uv run --frozen pytest tests/unit/leadership tests/integration/leadership \
  --no-cov -p no:cacheprovider -q --tb=short
uv run --frozen pytest tests/integration/leadership/test_feedback_continuation.py::test_review_drives_a_corrective_package_through_actual_provider_inputs \
  --no-cov -p no:cacheprovider -q -s
uv run --frozen ruff check src/vuzol/leadership tests/unit/leadership tests/integration/leadership
uv run --frozen mypy src/vuzol/leadership --follow-imports=silent --show-error-codes
git diff --check
```

Fixture integration завершает подключения и TRUNCATE-ит выбранную БД. Никогда
не запускать эти команды на default/shared/production DSN или параллельно на
одной БД. После удаления контейнера сначала создать новый disposable PostgreSQL,
проверить имя/пустую схему и подставить его порт в оба DSN.

Команды создания и проверки использованного контейнера:

```sh
docker run -d --pull=never --name vuzol-feedback-ksapan \
  --label vuzol.disposable=feedback-ksapan \
  -e POSTGRES_USER=vuzol_feedback -e POSTGRES_PASSWORD=feedback-disposable-only \
  -e POSTGRES_DB=vuzol_feedback_test -p 127.0.0.1::5432 postgres:16.13-bookworm
docker port vuzol-feedback-ksapan 5432/tcp
docker exec vuzol-feedback-ksapan pg_isready -U vuzol_feedback -d vuzol_feedback_test
docker exec vuzol-feedback-ksapan psql -U vuzol_feedback -d vuzol_feedback_test \
  -c "SELECT current_database(); SELECT tablename FROM pg_tables WHERE schemaname='public';"
```

## Границы и включение

Новых миграций нет; schema head остаётся `f3a8c2d14e70`.
Новых commands, прав, framework, inbox, provisioning и deploy не добавлено.

Feature **off по умолчанию**, никакое рабочее окружение не включалось.
Для отдельного разрешённого тестового стенда:

```sh
export VUZOL_PROJECT_LEAD__ENABLED=true
export VUZOL_PROJECT_LEAD__PROFILE_ID='<lead-api-profile>'
export VUZOL_PROJECT_LEAD__REVIEWER_PROFILE_ID='<separate-review-api-profile>'
```

Настройки сами по себе не делают реальные модели/TG тестовыми. Все прогоны выше
подставляют fake transports. Для выключения сначала остановить тестовую цель
через `/lead_pause`, затем выставить `VUZOL_PROJECT_LEAD__ENABLED=false` во всех
тестовых процессах и перезапустить только их.

Остаются ограничения предыдущей сдачи: API openai-compatible profiles;
goal verifier читает bounded commit snapshot, не запускает runtime/AVD/deploy
проверки; `request_revision` явно unsupported. Большой/неполный контекст может
потребовать решения владельца. Model verdict не является математическим
доказательством. Live costs/rollout требуют отдельного разрешения.

## Коммиты и trace

Код и regression tests:
`5f2ee0c905f67c2851690774e1a758eeb96009fa`
— `fix: feed scoped review results to lead and persist bounded continuations`.
Этот отчёт сдаётся отдельным documentation commit.

Ниже значения из финального самостоятельного запуска
`test_review_drives_a_corrective_package_through_actual_provider_inputs`.
Это actual provider requests через application composition, с fake внешними
транспортами; не live model call.

```text
goal:       1673db40-3991-4148-a6a1-12c72d0673d8
branch:     vuzol-managed/vuzol/c59cd9b8b04e
scope:      1
package A:  6b851427-782e-49dd-8173-9d3abc1fbfb9
head A:     798a06ea1a6b0c344ee14d57f3786bd5764e739a
evidence A: 1a554938-a4a9-4f5c-8819-516c94e3746b
verdict A:  insufficient
explanation: SECOND.txt is missing. Add SECOND.txt with exactly marker-b followed by a newline.

next command: propose_batch
next plan:    Add the missing SECOND marker
next scope:   SECOND.txt
next criterion: SECOND.txt contains marker-b
next reason: Address evidence 1a554938-a4a9-4f5c-8819-516c94e3746b: <explanation above>
following turn, fresh worker: start_batch {}

package B:  54bcf8a8-48e5-4880-8e5c-5baae6cba587
head B:     3600302b46d1558f603bd2ac53c27c8eda844d1b
evidence B: 8d2a4d6e-9871-4810-ad5b-f12282d57a29
verdict B:  pass
explanation: Both requested markers are implemented.
evidence_paths: MARKER.txt, SECOND.txt
last command: propose_completion {}
goal lifecycle: completed
main unchanged: 9fcc13d6fbf74390e853434cbfff8127f97e5d80
```

Четыре руководящих provider input: первоначальный запуск A; конкретное
отрицательное замечание + исполненный план + implementation summary; сохранённый
исправляющий proposal для отдельного запуска; положительное evidence нового head.
В БД сохранено решение с текстом замечания и evidence ID. Между проверкой и
доработкой нет `answer_permission`, всего создано два пакета.

R8/R9 исправлены и переданы на повторное независимое ревью; это не утверждение
об их независимой приёмке. После сдачи — остановка, без live-пилота и перехода
к следующим этапам.
