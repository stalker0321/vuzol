# Аккаунты, подписки и карта — промежуточная сдача 23 октября

Продолжение после референса bb: [OAuth usage — реализованная дельта и границы](OPENCODE_OAUTH_USAGE_2026_10_23.md).
Ниже сохранён исходный срез; его static-key probe не является OAuth-приёмкой.
В продолжении уже подтверждён live OAuth C (used 0/28/58); остаются сервисный
доступ, единый refresh owner и отдельный rollout. Не читать исходный отказ
ниже как утверждение о новом endpoint.

Статус: локальный проверяемый срез, **не завершённая live-приёмка задания**.
Production не менялся. Browser OAuth/Go usage и миграция существующих аккаунтов
остаются открытыми. Числа из кабинета владельца не подставлены в сборщик.

## Сохранность и границы

- Ветка `agent/accounts-limits-map-20261023`, исходный HEAD
  `f5a34228822b45e6dae4de681197b719627c0fe9`.
- До работы были 43 изменённых tracked файла и 22 untracked файла. Согласованный
  private backup: `/home/vodkolyan/vuzol-accounts-baseline-20261023-ybmcvocy`:
  919 файлов, manifest с SHA-256, binary patches, index, проверенный Git bundle.
- Существующие изменения сохранены; новых коммитов нет: некоторые изменения
  пересекаются с незакоммиченным предыдущим срезом. Автоматически включать его
  в свой коммит нельзя. Production venv, auth, registry, tasks не изменялись.
- Ни push/deploy, ни платных model calls, ни Telegram mutations. Внешние проверки:
  официальные документы/исходники и read-only GET usage, без refresh/login.

## Что реализовано локально

1. Account descriptor + `account_ref`, компилируемый в существующий profile.
   Модель/роль остаются отдельно. Конфликты inline полей не скрываются overlay.
   Старые профили и их revisions сохранены, переход на descriptor не автоматический.
2. Go collector через существующий executor `/update`: фиксированный endpoint,
   read-only auth boundary, три типизированных окна, дробные проценты, отдельные
   измерение/попытка, bounded HTTP/body/cache/concurrency, coalescing/Retry-After.
   Redirect не получает секрет. Неизвестность не превращается в 100%.
3. JSONB round-trip всех окон без новой SQL migration. Исторические integer
   Codex/Grok поля читаются; месяц Go не маскируется под неделю.
4. Информация о тарифе: `SubscriptionPlan` хранит service/tier/status/source/time.
   Codex использует полученный `plan_type`, Grok — явный `subscriptionTier`.
   Go подтверждается только валидным Go meter; общий 403 не доказывает отсутствие
   подписки. Старые выдуманные defaults Plus/Super больше не показываются как факт.
   Смена Plus → Pro обновляет карточку, но не admission, model roles и execution pins.
   Старые Grok snapshot-файлы без происхождения тарифа остаются неподтверждёнными.
5. Read-only `python -m vuzol.cli.accounts list|check|status --registry ...
   [--overlay ...]`: реальные account IDs, профили/роли/fallbacks/revisions,
   visibility, state ACL и локальный auth check без HTTP/inference.
   Это пока НЕ import/reauth/retire и НЕ live DB/slots/plan-status CLI.
6. [Автономная карта](system-map/index.html): 16 семейств, поиск/фильтры/pan/zoom,
   source/test links, отдельные признаки implementation/live/route/evidence,
   coverage, outbox contracts и датированная runtime-сверка. Validator сверяет
   AST inventory, callback actions, consumers, ссылки, статусы, duplicate keys,
   полноту queue contracts и актуальность сгенерированного HTML.

Изменения этой итерации: `config/{accounts,loader,models,revision}.py`,
`providers/{account_auth,opencode_usage,quota,subscription_plan,subscription_limits}.py`,
`cli/accounts.py`, небольшой guard optional timestamps в `telegram/projections.py`,
тесты accounts/usage/plan/boundary/map и `devtools/system_map.py`/`docs/system-map`.
Не весь вывод `git diff HEAD` принадлежит этой итерации.

## Auth: проверенные различия, не универсальный Bearer

| Потребитель | Проверенный контракт | Identity / refresh | Что ещё не доказано |
| --- | --- | --- | --- |
| Новый OpenCode | Device/browser OAuth; отдельный Console login | `opencode-cli`, org-scoped token; CLI обновляет через `/console/auth/device/token` | Наш Go meter с этим OAuth, entitlement нужного workspace |
| Установленный Pi 1.0.3 | Локально `opencode-go: type=api_key`; официальная Pi-инструкция тоже отделяет Go от PAYG | B/C отдельные canonical seeds; копия auth в приватном tmpfs invocation | Поддерживаемый Pi inference OAuth с durable rotation |
| Collector этого среза | Только явно объявленный Pi Go static key → исторический Go usage route | Секрет только из указанного account-state; никакого login/refresh | Live B/C пока оба 403, не готов к OAuth/rollout |
| Console MCP | Отдельный browser OAuth/DCR для workspace tools | Не тождественен Pi inference login | Наличие подходящего quota read tool не подтверждено |

Исходники нового OpenCode проверялись на
[`096c2ba5551d1f8000148b1bf4d167888fda212c`](https://github.com/anomalyco/opencode/blob/096c2ba5551d1f8000148b1bf4d167888fda212c/packages/core/src/plugin/provider/opencode.ts).
OAuth token получает org identity и config через Console; это не доказательство,
что его можно посылать на старый Go usage URL. Такие пробы не делались.

Исторический Go usage контракт зафиксирован на
[`2b8a5969e932c15083e599c82d34ce0268f81b9e`](https://github.com/anomalyco/opencode/blob/2b8a5969e932c15083e599c82d34ce0268f81b9e/packages/console/app/src/routes/zen/go/v1/usage.ts).
В новом public tree прежнего Console route уже нет. Поэтому offline fixtures
проверяют именно этот контракт, а не гарантируют современный server response.

[Pi-документация](https://opencode.ai/v2/docs/console/harnesses/pi/) различает
inference Go/PAYG и Console MCP browser login.
[Console API](https://opencode.ai/v2/docs/console/api/) документирует отдельно
API-key permissions; это не основание требовать у владельца новые ключи вместо OAuth.
[Codex App Server](https://learn.chatgpt.com/docs/app-server) документирует
опциональные plan details. В этом срезе не внедрялся новый app-server daemon:
используется явное поле существующего usage collector, без Plus по умолчанию.

Следующий необходимый шаг — подтвердить поддерживаемый OAuth quota contract
и привязать одобренный владельцем browser login к тому же account/workspace.
Не запускать login, способный заменить рабочую identity, без выбора точного
аккаунта. Если нужен другой runtime, сначала согласовать смену/certification;
не подменять Pi/OpenCode скрытым transport или PAYG.

## Live evidence и сверка владельца

- GET старого usage route: B/C → HTTP 403 JSON, без quota windows. Во второй
  ограниченной проверке тело имело `error/type`, признак access denied; причина
  entitlement/permissions не установлена. Тела и credentials не публиковались.
- Владелец 23 октября: C использовано **0 / 28 / 58%**, A **0 / 0 / 0%**,
  порядок **5 часов / неделя / месяц**. B сейчас без подписки. Это пользовательская
  сверка кабинета, не автоматическое измерение и не proof matching credentials.
- Deployment read-only: контейнеры `vuzol:t137-execution-visibility-r2`, host-side
  executor/applier/static publisher `/opt/vuzol/releases/t137-r2`.
- В 00:03 UTC: account tickets 0; supervised processes 55, все exited.
  Остались paused/blocked/quota-exhausted задачи и pinned назначения; zero slots
  не разрешает переписывать их identity/revision.
- Pi C объединяет DeepSeek/Muse/MiMo; роли в двух проектах: executor/planners
  DeepSeek C, lead Muse C, reviewers MiMo C. Это проверка сохранённых назначений,
  не разрешение переключать их на другие модели.
- Grok A скрыт на dashboard, но enabled и остаётся в fallback. B OpenCode enabled,
  хотя по владельцу без подписки. Автоматически отключать эти профили нельзя.

## Dry-run миграции: НЕ применено

Сокращения hash ниже предназначены для чтения, не для SQL/операторских mutations.
Полный ID получать через CLI. Нельзя вручную переписывать reservations/pins.

| Аккаунт | Сейчас admission ID / state | Целевое описание / условие перехода |
| --- | --- | --- |
| Pi C | `opencode-go-c`, `/var/lib/vuzol-provider-state/pi-opencode-go-c` | Сохранить ID, 3 profile refs; сначала OAuth/product/scope и ACL доказательство |
| Pi B | `profile:953a55f5…`, canonical provider-state | `opencode-go-b` только после drain/frozen audit; сейчас подписки нет по владельцу |
| Pi A | `profile:e6932496…`, `/home/vodkolyan/.opencode-profiles/1`, disabled | Импорт выбранного owner account в service-state; не включать автоматически |
| Codex A | `profile:3bcf6776…`, `/var/lib/vuzol-provider-state/codex-a` | Auth читается host executor; новый descriptor ID требует migration, не silent alias |
| Codex B | `profile:e11020b3…`, `/home/vodkolyan/.local/share/vuzol-auth/codex-b` | Canonical service-state вне home, identity/login/refresh и ACL check |
| Codex C | `profile:2306f375…`, `/home/vodkolyan/.codex-c` | То же; не сканировать home при каждом запросе |
| Grok A/B | `profile:a1770492…` / `profile:6c28eff1…`, canonical provider-state | Отдельное решение о retirement A; stale meter B не равен нулевому остатку |

У Pi B/C файлы auth 0600 с rootless uid 110000; host executor их не читает.
Codex B/C 0600 uid 1000 тоже не читаются executor. Проверка root успешно читает
файл, но не доказывает работу сборщика от service UID. Не открывать весь home.

Порядок будущей миграции: точный inventory frozen steps/config/policies → drain
только затронутых аккаунтов → backup auth/registry с закрытыми ACL → explicit
import/login с проверкой identity/workspace → service-UID check и rotation/revoke
tests → isolated-project canary → согласованный rollout всех consumers.
Uncertain effects не освобождать по таймеру; существующие pins не заменять defaults.
Сейчас не реализованы import/reauth/retire, атомарный OAuth refresh owner,
transport-independent API+CLI descriptor и подтверждённые shared quota pools.

Отдельный security follow-up: старый Codex/Grok `_http_json` использует обычный
`urlopen` и unbounded `read()`: запрет redirects/size cap нового Go adapter туда
пока не перенесён. В этой итерации новых live Codex/Grok HTTP probes не делали.
Нужен отдельный transport-hardening с regression tests, до заявления о единой
безопасной auth boundary всех провайдеров.

## Проверка локального среза

Venv `/tmp/vuzol-stage2.jzwfpD/venv`, добавочные зависимости
`/tmp/vuzol-mcp-deps-20261022`; production venv не менялся.
Disposable container `vuzol-accounts-test-20261023`, label
`vuzol.disposable-test=accounts-20261023`, PostgreSQL 16.13, tmpfs, без persistent
volume; только `127.0.0.1:32789/vuzol_accounts_test`. Migrations до
`e8b5a61fa505`; новых migrations нет.

Из корня репозитория команды запуска:

```sh
PYTHONPATH=src:.:/tmp/vuzol-mcp-deps-20261022 PYTHONDONTWRITEBYTECODE=1 \
  /tmp/vuzol-stage2.jzwfpD/venv/bin/python -m pytest tests/unit --no-cov -q --tb=short -rs

env -u VUZOL_DATABASE_DSN_REFERENCE \
  VUZOL_TEST_DATABASE_DSN=postgresql://vuzol_accounts_test:disposable-local-only@127.0.0.1:32789/vuzol_accounts_test \
  PYTHONPATH=src:.:/tmp/vuzol-mcp-deps-20261022 PYTHONDONTWRITEBYTECODE=1 \
  /tmp/vuzol-stage2.jzwfpD/venv/bin/python -m pytest \
  tests/integration/providers tests/integration/leadership \
  tests/integration/telegram/test_projections.py tests/integration/telegram/test_delivery.py \
  tests/integration/workflows/test_workflow_lifecycle_controls.py --no-cov -q --tb=short -rs

python3 devtools/system_map.py
python3 devtools/system_map.py --check
```

Перед повторением проверить exact container/port/database. Fixture TRUNCATE-ит
выбранную БД и завершает соединения. Не запускать два integration процесса на
одной БД. DSN выше только disposable; не заменять на shared/production/default.

Результаты:

- Последняя полная integration **выборка выше**: **360 passed, 1 skipped**,
  171.86s. Это не вся integration suite. Skip — старый per-task planner,
  временно отключённый в `compile_workflow` (BACKLOG).
- Последний полный unit до четырёх заключительных guard tests: 3621 passed,
  1 skipped; после этих guards targeted config/providers/map: 203 passed.
  Финальный полный unit до OAuth-дельты: **3625 passed, 1 skipped**, 92.99s,
  11 warnings. Skip — `test_remote_out_of_scope.py`, не пройденная проверка.
- Read-only Go/plan DB boundary и dashboard: 19 passed. Restart + usage boundary
  отдельным последовательным прогоном: 4 passed (16.92s).
- Ruff на изменённых модулях/новых тестах, scoped mypy (9 source files),
  `git diff --check`, JS syntax и map validator — успешно.
  Карта: 16 семейств, 136 найденных входов, 13 очередей, 9 findings;
  uncovered/stale bindings/missing consumers — 0 в пределах AST extractor.

Не скрытые неудачные прогоны:

- Неверная команда integration остановлена: 109 failed, 95 passed —
  `VUZOL_DATABASE_DSN_REFERENCE` конфликтовал с `build_bundle(environment={})`.
  Не зачтённый прогон; assertions не менялись, исправлена команда запуска.
- Следующая выборка: 359 passed, 1 failed, 1 skipped. Единственный failure —
  30s timeout subprocess restart в `test_managed_goal_flow`. Отдельный rerun и
  повтор всей выборки без параллельных suites прошли. Это возможная timing
  нестабильность, не доказанный исправленный runtime defect; timeout не увеличен.

Warnings сохранены: unit 11 (SDK deprecations + pytest stale-temp cleanup),
integration 12 (включая SAWarning о незакрытом connection в projection test).
Чужие временные каталоги не удаляли, warnings не подавляли.

Trace offline-приёмки: реальный registry → два профиля account C → один GET через
fake transport → 3 окна → JSONB → новая DB composition → Telegram dashboard.
Отдельно реальный executor refresh Codex: Plus → persist/load → Pro → новая
card revision, без ProviderAccountLease. Проверен новый engine, **не OS restart**.

## Включение / выключение и rollback

Автоматически ничего не включено: legacy config не активирует Go collector по
имени `pi`/суффиксу аккаунта. Для изолированного теста образец config создаёт
`tests/unit/config/test_account_descriptors.py::document`: явный `accounts` block,
`auth_method=static_api_key`, `quota_collector=opencode_go`, `quota_scope_id`,
и `account_ref` в профилях. Это fixture текущего узкого adapter, НЕ инструкция
переходить с OAuth на ключ. `check` не проверяет live entitlement.

Выключить сбор у explicit descriptor: `quota_collector=none`. Visibility и
enabled/lifecycle независимы; скрытие карточки не отключает исполнителя.
Для production сначала независимое ревью и отдельное разрешение; до OAuth
приёмки этот Go adapter туда не выкатывать. Старый код игнорирует новые JSONB
поля, downgrade migration не нужен. Возврат config/release возможен только
после проверки текущих leases/pins, без удаления старых auth/state.

Карта — статическая помощь аудиту, не сертификат полной системы. Browser UI
визуально не проверен (проверен синтаксис JS); external timers/dynamic registrations
требуют дальнейшей сверки. Full integration suite, live OAuth refresh/revocation,
OS restart OAuth/collector и model/inference canary в этой итерации не выполнены.
Существующий fake-transport lead test с subprocess restart пройден отдельно
и в последней integration выборке; это не live OAuth restart certification.
