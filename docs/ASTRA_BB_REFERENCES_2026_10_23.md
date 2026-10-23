# Астре: что взять из bb и как продолжить уже сделанную работу

Дополнение от 2026-10-23 к
[основному заданию](ASTRA_ACCOUNTS_LIMITS_SCENARIO_MAP_2026_10_23.md).
Читать вместе с [текущей промежуточной сдачей](ACCOUNTS_LIMITS_PROGRESS_2026_10_23.md).
При расхождении старого задания с новым inventory сначала проверить факты;
не выполнять устаревшие шаги заново.

## Главное

Владелец привёл bb как практический ориентир: его знакомый пользуется системой
для работы, а не постоянно чинит сам оркестратор. Это пользовательский опыт,
не результат нашего собственного прогона bb. Но критерий для Вузла правильный:
согласованная задача должна двигаться до результата без ручного обслуживания
каждого перехода; если нужно решение человека — понятно, какое и почему.

Смотрим [get-bb/bb](https://github.com/get-bb/bb), закреплённый срез
`2884950f17e76f78b28c64ccf4a2e9a7c74188a8`. Исследовались исходники и тесты,
bb у нас не устанавливался и его тесты не запускались.

Не нужно начинать новый проект внедрения bb, переносить его plugin platform
или переписывать Вузол. Нужны небольшие заимствования в существующих границах.

## 1. Что у нас уже есть — не делать с нуля

На чтении рабочего дерева уже присутствуют:

- `config/accounts.py`, `AccountConfig`/`account_ref` и нормализация профилей;
- `providers/account_auth.py`, `opencode_usage.py`, `quota.py`, `subscription_plan.py`;
- `cli/accounts.py`, unit-тесты и `test_opencode_usage_boundary.py`;
- `docs/system-map/`, builder/validator и промежуточный отчёт.

Текущий collector читает явно настроенный Pi static key, обращается к
`/zen/go/v1/usage` и разбирает исторические `rollingUsage/usagePercent/resetInSec`.
OAuth в этом auth boundary намеренно отклоняется. В отчёте честно отмечено:
старый route дал 403 для B/C, live Go не принят, OAuth/refresh ещё открыты.
Это не утверждение, что вся реализация неправильная: сохранить полезные
ограничения HTTP, single-flight/cache, round-trip окон, plan provenance и тесты.

По новой сверке владельца в отчёте B сейчас без подписки; показания есть для
A/C. Прежнее требование «успешный сбор B/C» не выполнять вслепую: сначала
актуальный разрешённый inventory, привязка credentials к кабинету и service UID.
Не включать disabled A, не выключать B и не менять frozen bindings автоматически.
Проценты, сообщённые владельцем, — ориентир сверки, не значения для fixtures
или имитации успешного collector response.

## 2. P0: конкретная реализация нового OpenCode OAuth/usage

Начать с двух файлов bb:

- [opencode-usage.ts](https://github.com/get-bb/bb/blob/2884950f17e76f78b28c64ccf4a2e9a7c74188a8/packages/provider-bridge-acp/src/bridge/opencode-usage.ts)
- [opencode-usage.test.ts](https://github.com/get-bb/bb/blob/2884950f17e76f78b28c64ccf4a2e9a7c74188a8/packages/provider-bridge-acp/src/bridge/opencode-usage.test.ts)

Что в них полезно: чтение нового SQLite credential store и старого account store;
разделение OAuth и ключей; проверка issuer/org/expiry; для OAuth —
`https://opencode.ai/inference/go/v1/usage` с `x-opencode-org-id`, для ключей —
старый Zen route. Новый ответ имеет `usage.{rolling,weekly,monthly}` с `percent`
и `resetsAt`. Тесты покрывают миграцию store, чужой issuer, отсутствие org,
истёкший токен и исключение stale auth-файла после миграции на v2.

Это сильный референс, но не гарантия совместимости с нашей версией OpenCode/Pi.
Выписать применимые версии и подтвердить read-only ответ для нужного owner account.
Один HTTP 401/403 не доказывает все свойства OAuth/подписки.

Как применить к Вузлу:

1. Расширить существующий auth descriptor/resolver, а не добавить обход
   `read_go_key` с поиском credentials по home. Аккаунт и org выбираются явной
   настройкой; активный аккаунт пользовательского desktop не выбирается за нас.
2. Сохранить запрет отправки неподтверждённого токена на другой origin/product.
   Различать auth method → разрешённый endpoint → соответствующий parser.
3. Поддержать новый payload отдельным проверяемым разбором. Не смешивать поля
   двух форматов эвристикой и не считать ошибку новой схемы поводом попробовать
   другой секрет, аккаунт или PAYG endpoint.
4. Проверить service UID, rootless mapping и путь получения свежего токена.
   Сам collector bb не refresh-ит истёкший OAuth: он возвращает expired.
   Значит наш refresh owner/rotation/revoke всё ещё требует отдельного решения.
5. Перенести тестовые случаи на Python/существующий HTTP boundary: issuer/org,
   stale legacy auth, malformed store, expired token, изоляция двух аккаунтов,
   новые три окна и статусы. Секреты и исходные приватные записи не копировать.
6. Для статических legacy credentials сохранить существующий отдельный путь,
   если он ещё поддержан и явно выбран. Новый OAuth не должен означать
   обязательную массовую миграцию всех профилей ради лимитов.

Не копировать из bb поиск активного credential по всей пользовательской БД как
политику выбора аккаунта Вузла. Не переводить `cli_oauth` в новый режим без
проверки config validation, revisions, readonly sandbox seed и потребителей.

Сдача: настоящий OAuth usage для согласованного активного аккаунта, все окна
и org совпадают с кабинетом; состояние auth/ошибка объяснимы. Если login требует
владельца — подготовить безопасный ручной шаг и продолжение, не обходить login.
Не выдавать mock-тесты старого endpoint за завершённую live-приёмку нового пути.

## 3. Общий контракт квот и account identity

Референсы:

- [usage-contract.ts](https://github.com/get-bb/bb/blob/2884950f17e76f78b28c64ccf4a2e9a7c74188a8/plugins/account-pool/src/usage-contract.ts)
- [provider-usage README](https://github.com/get-bb/bb/blob/2884950f17e76f78b28c64ccf4a2e9a7c74188a8/plugins/provider-usage/README.md)
- [account pool overview](https://github.com/get-bb/bb/blob/2884950f17e76f78b28c64ccf4a2e9a7c74188a8/plugins/account-pool/PLUGIN_OVERVIEW.md)

Полезное разделение: дешёвый inventory ресурсов отдельно от измерения выбранного
ресурса; provider-issued identity отдельно от display label; время успешного
измерения отдельно от попытки. Неизвестная identity не объединяется по email,
а ошибка авторизации не превращается в нулевое потребление.

Сопоставить это с уже добавленными AccountConfig/QuotaWindow/SubscriptionPlan.
Исправлять только найденные пробелы, не заменять наши типы чужими именами.
В частности, проверить, что list/check не превращаются в скрытый HTTP refresh,
refresh одного аккаунта не опрашивает все остальные, а модельный quota scope
не становится вторым account lock.

Account Pooler bb в этом срезе ориентирован на Claude/Codex; OpenCode usage —
отдельный bridge. Не объявлять, что у них уже есть универсальный OpenCode pool,
который можно просто подключить. У нас пока не нужен новый HTTP proxy с
машинными hub tokens только ради нормализации аккаунтов.

## 4. P1: общая матрица адаптеров и безопасный replay

Референсы:

- [provider bridge protocol](https://github.com/get-bb/bb/blob/2884950f17e76f78b28c64ccf4a2e9a7c74188a8/docs/provider-bridge-protocol.md)
- [conformance scenarios](https://github.com/get-bb/bb/blob/2884950f17e76f78b28c64ccf4a2e9a7c74188a8/packages/provider-bridge-protocol/src/conformance/scenarios.ts)
- [recording replay](https://github.com/get-bb/bb/blob/2884950f17e76f78b28c64ccf4a2e9a7c74188a8/packages/provider-bridge-protocol/src/testing/first-party-replay.ts)

Идея: provider adapter разбирает родной диалект, общий runtime проверяет
семантику событий. Один набор conformance-проверок применяется ко всем bridges;
записанные обмены воспроизводятся без платного model call.

Для Вузла сначала составить матрицу существующих тестов/проб. Уже есть отдельные
provider tests, agent certification и encrypted runtime journal — использовать
их, не строить второй реестр runtime. Добавить общий parameterized contract
suite там, где сейчас каждый adapter проверяется по-разному:

- один успешный вызов, ошибка до отправки и после отправки;
- промежуточный текст против окончательного ответа, native retries;
- tool/background work ещё идёт после ответа модели;
- malformed/truncated stream, неожиданный process exit;
- timeout/cancel, повторная отмена, late result после pause;
- receipt/account ticket/cleanup и отсутствие повторного внешнего эффекта.

Не всем нашим adapters нужны native resume/fork/steer: unsupported — валидный
результат, не требование срочно реализовать feature. Полный bb JSON-RPC protocol
и plugin handshake сейчас не переносить; сначала общие поведенческие проверки
поверх существующих Python ports.

Из private journal нельзя автоматически делать публичную fixture. Только
санитизированный минимальный пример с проверкой отсутствия secrets, prompts,
приватного tool output и персональных данных. Replay parser/adapter-теста не
должен выполнять shell, inference или переотправлять реальные side effects.

Сдача среза: один раньше неоднозначный сценарий воспроизведён записью/fixture,
одна общая матрица запускается для поддержанных adapters, skips объяснены.
Дальше расширять по конкретным инцидентам, не устраивать рефакторинг всех adapters.

## 5. P1: объяснимое ожидание и решение о повторе

Референсы:

- [retry availability contract](https://github.com/get-bb/bb/blob/2884950f17e76f78b28c64ccf4a2e9a7c74188a8/plugins/account-pool/src/retry-contract.ts)
- [provider-retry overview](https://github.com/get-bb/bb/blob/2884950f17e76f78b28c64ccf4a2e9a7c74188a8/plugins/provider-retry/PLUGIN_OVERVIEW.md)
- [saved retry diagnostic](https://github.com/get-bb/bb/blob/2884950f17e76f78b28c64ccf4a2e9a7c74188a8/plugins/provider-retry/src/diagnostics.ts)

У bb различаются ready, blocked с известным временем и unavailable с причиной.
`provider-retry explain` показывает сохранённое решение, не только текущую
очередь. Auth failure и неизвестный reset не создают спекулятивный retry;
ожидание reset распределяется, чтобы задачи не просыпались одновременно.

Вузлу нужна одна проекция «почему не двигается», а не ещё один набор статусов:
причина; чего ждём; кто должен действовать; следующее время/событие проверки;
что может продолжиться автоматически; почему последнее решение было таким.
Использовать existing task/step/goal/outbox evidence и stored decisions.
Историческое объяснение не выдавать за текущую доступность аккаунта.

Применять поверх имеющихся retry/wakeup/control mechanisms. Ready от провайдера
не отменяет owner pause, approval, scope/pins или uncertain effects. Reset квоты
не разрешает повторить уже отправленный push/deploy. Эти ограничения важнее
заимствования механизма автоматического продолжения.

Сдача: на одном реальном stopped/waiting сценарии одинаковое объяснение в
операторском выводе и Telegram; без ручного SQL понятно, что делать дальше.

## 6. Менеджер и дочерняя работа: проверить ownership, не добавлять второго лида

Референсы:

- [system overview](https://github.com/get-bb/bb/blob/2884950f17e76f78b28c64ccf4a2e9a7c74188a8/docs/system-overview.md)
- [child notifications](https://github.com/get-bb/bb/blob/2884950f17e76f78b28c64ccf4a2e9a7c74188a8/apps/server/src/services/threads/child-thread-notifications.ts)
- [background work state](https://github.com/get-bb/bb/blob/2884950f17e76f78b28c64ccf4a2e9a7c74188a8/packages/agent-runtime/src/runtime-background-work-state.ts)

Обратить внимание на различие связи для отображения и lifecycle ownership,
уведомления родителю о blockers/terminal outcomes, а также различие окончания
turn и окончания запущенной дочерней/background работы.

У нас lead/wakeup/sequencer уже есть. Сравнить с ними и картой: нет ли ребёнка,
чей terminal outcome никого не будит; не пропадает ли blocker; что останавливает
pause/cancel; кто сохраняет cleanup evidence. При отсутствии пробела ничего
не переписывать. Семантику bb archive/delete не переносить на нашу owner pause.

## 7. Ограничения заимствования и порядок

Не переносить целиком:

- plugin marketplace/SDK/QuickJS workflows и новый scheduler;
- автоматический account fallback без наших frozen bindings и cost policy;
- упрощённые предположения о повторении внешних эффектов;
- desktop-wide поиск credentials как сервисный account selection;
- доверенную локальную модель публикации API на наш публичный сервер.

bb [предупреждает в README](https://github.com/get-bb/bb/blob/2884950f17e76f78b28c64ccf4a2e9a7c74188a8/README.md)
об unauthenticated API в режимах прямого удалённого доступа; это не шаблон
для нашего публичного MCP. Код bb [под MIT](https://github.com/get-bb/bb/blob/2884950f17e76f78b28c64ccf4a2e9a7c74188a8/LICENSE):
при переносе существенных фрагментов сохранять предусмотренные уведомления.

Порядок: прочитать текущую сдачу → сравнить существующий collector с новым
OAuth-референсом → довести один Go vertical slice → закрыть конкретный пробел
общей матрицы → один explainable-wait сценарий. Проверка ownership входит в
карту/аудит; код менять только по доказанной находке. Если feature уже готова —
показать соответствующий код и evidence, а не делать её второй раз.

Продовые изменения, login/refresh, импорт credentials и canary с внешними
эффектами требуют применимых разрешений из основного задания. Это дополнение
само не разрешает deploy, включение профилей, смену аккаунта или запуск paused work.
Не менять параллельно файлы, которые сейчас редактирует другой агент; сначала
сверить актуальное дерево и сохранить существующие изменения.

Короткий отчёт Астры: что уже было → что подсмотрено → какая дельта действительно
реализована → tests/live evidence → что осталось. Главный результат — меньше
участия владельца в обслуживании исполнения, а не количество новых контрактов.

## Сообщение для действующей сессии Астры

> Добавился референс bb: прочитай `docs/ASTRA_BB_REFERENCES_2026_10_23.md`.
> Текущую работу не начинай заново: collector/accounts/map уже есть, сначала
> сравни с собственной промежуточной сдачей. Приоритет — новый OpenCode OAuth
> usage path и payload из bb, затем недостающий общий adapter contract test и
> объяснение ожидания. Сохрани существующие границы, pins, pause и cleanup.
> Не внедряй bb целиком и не расширяй scope до новой платформы. Прод не меняй
> без отдельного разрешения. Покажи дельту и доказательства, не только план.
