# Задание Астре: аккаунты, лимиты OpenCode и карта сценариев Вузла

Дата: 2026-10-23. Рабочий каталог: `/home/vodkolyan/vuzol-afterword-stabilization`.

Дополнение после начала реализации:
[референсы bb и продолжение уже сделанного](ASTRA_BB_REFERENCES_2026_10_23.md).
Сначала читать также [текущую сдачу](ACCOUNTS_LIMITS_PROGRESS_2026_10_23.md):
не выполнять уже закрытые шаги повторно и не считать старый usage route
единственным вариантом для нового OAuth.

Это задание на реализацию, не отчёт о готовности. Оно не разрешает выкладку в
production, смену тарифов, включение платного перерасхода или удаление данных.

## 1. Что нужно получить

Хочется перестать угадывать, какой из нескольких путей сейчас сработает.
Нужны три конкретных результата:

1. Рабочий сборщик реальных лимитов OpenCode Go для наших аккаунтов.
2. Полная карта фактически существующих сценариев: удобный canvas, где можно
   посмотреть путь от сообщения до результата, включая legacy и остановки.
3. Единое подключение аккаунтов: один понятный источник настроек, авторизации,
   лимитов и слотов. Новый аккаунт поддерживаемого типа добавляется по инструкции,
   а не правками в пяти местах и поиском чужого `auth.json`.

Не нужно ещё раз перепридумывать оркестрацию. Основной продуктовый путь остаётся:
обсуждаем → явно поручаем работу → лид ведёт цель → существующий sequencer
исполняет пакеты → независимая проверка → разрешённая доставка результата.
Telegram и MCP — входы в существующую систему, не отдельные управляющие ядра.

## 2. Сначала разобраться с текущим состоянием

Прочитать `docs/README.md`, `REPOSITORY_HEALTH_2026_10_22.md`,
`EXECUTION_RELIABILITY_2026_10_22.md`, `PROVIDERS.md`, `MCP.md`.
Старые планы из исторического раздела — справка, не команды к исполнению.

Проверить инструкции репозитория, git status, ветку, изменения и фактические
пути запуска сервисов. На момент подготовки HEAD — `f5a3422`, но значительная
часть последних reliability/MCP/account-правок НЕ закоммичена. Нельзя принять
один HEAD за текущий baseline, затереть изменения или назвать их мусором.
Сначала сохранить обратимый baseline согласованным способом; не коммитить
секреты и чужие изменения автоматически. Реализация — в отдельной ветке.

Последняя проверенная выкладка 22 октября: `vuzol:t137-execution-visibility-r2`,
host-side source `/opt/vuzol/releases/t137-r2`. Это историческая точка сравнения,
а не утверждение, что сегодня никто ничего не обновил.

Уже есть — не делать заново:

- профили/аккаунты/модели в карточках; независимые lanes лида и исполнителя;
- account-wide admission, durable tickets, control-reserved slots;
- зашифрованный runtime journal, подтверждение cleanup и сохранение паузы;
- provenance запросов MCP и четыре MCP tools через `ProjectApplication`;
- discovery включённых CLI-профилей для лимитов, скрытие истёкших подписок,
  неизвестные/stale значения без выдуманных 100%;
- исправление timestamp старых Grok-логов и content-hash версии дашборда.

Проверить сохранность этих свойств, а не строить параллельную реализацию.

## 3. OpenCode: сначала auth-контракт, затем сборщик

### 3.1. Не смешивать OpenCode, Pi, Go, Console и OAuth

Владелец сообщил, что новый вход в OpenCode теперь через OAuth. Это обязательный
вопрос данной итерации: обеспечить поддерживаемый актуальный способ входа,
а не заставить владельца добывать старый ключ только ради удобства сборщика.

Есть расхождение с установленным состоянием: 23 октября в канонических файлах
Pi/OpenCode B и C найдены записи `opencode-go` с типом `api_key`. Проверялись
только тип и имена полей, не публиковались значения. Это факт о нашей установке,
не доказательство того, как устроен свежий OpenCode.

Официальная [инструкция Pi](https://opencode.ai/v2/docs/console/harnesses/pi/)
на проверке всё ещё описывает ключи для Go; [Console API](https://opencode.ai/v2/docs/console/api/)
тоже отдельно описывает ключи. Поэтому не объявлять глобально ни «ключей больше
нет», ни «OAuth точно не нужен». Сопоставить версию клиента, вид подключения,
workspace, тариф и реально доступные способы авторизации.

Составить короткую проверенную таблицу:

| Потребитель | Чем входит | Какая identity/workspace | Что разрешено | Кто обновляет auth |
| --- | --- | --- | --- | --- |
| Новый OpenCode/Console | Проверить OAuth/device/browser flow | Проверить | Go или иной продукт? | Один account auth owner |
| Установленный Pi `opencode-go` | Сейчас локально `api_key`; проверить целевой OAuth | B/C отдельно | Inference нужного тарифа | Тот же auth owner |
| Сборщик лимитов | Проверить поддерживаемый auth конкретного usage API | Та же область подписки | Только чтение usage | Не отдельный login/refresh |

Успешный OAuth login ещё не доказывает доступ к Go. Проверить entitlement,
workspace и маршрутизацию: нельзя получить Console/Zen credentials, показать
«Go подключён», а затем списывать деньги с pay-as-you-go баланса.
Не считать browser session cookie, OAuth access token и API key взаимозаменяемыми.
Не передавать токен на похожий endpoint методом проб и ошибок.

Если целевой OAuth ещё не поддержан установленным Pi, показать конкретный
совместимый путь/миграцию и ограничения. Не делать скрытый fallback на ключ,
другой аккаунт, платный тариф или другого исполнителя. Browser login владельца
может быть единственным ручным шагом; модель не должна обходить его или MFA.

### 3.2. Зацепка для usage API — проверена, но не является гарантией OAuth

В upstream есть [merged PR #16513](https://github.com/anomalyco/opencode/pull/16513)
для `GET https://opencode.ai/zen/go/v1/usage`. При проверке без credentials
23 октября адрес ответил 401. Это подтверждает наличие защищённого пути,
но не работу с нашими аккаунтами и не принятие OAuth access token.

В [исходнике маршрута](https://github.com/anomalyco/opencode/blob/dev/packages/console/app/src/routes/zen/go/v1/usage.ts)
есть Bearer-auth и окна usage; ссылка на ветку может устареть. Зафиксировать
актуальный commit/версию контракта. Исторический PR и нынешний ответ сервера
могут иметь разные формы JSON. Нужен безопасный authenticated read-only probe
с разрешённым способом входа, а не копирование первого примера из интернета.

Проверить сначала этот путь и актуальный официальный Console usage API.
Не начинать со скрейпинга HTML, browser-cookie exporter, Playwright-демона
или нового сервиса. Если нужен другой endpoint — записать, почему, какую auth
он принимает и как доказана привязка к нужному аккаунту.

### 3.3. Контракт данных и поведения

- Источник — серверный meter, учитывающий расход вне Вузла. Локальные токены,
  ориентировочная стоимость и успешный inference не измеряют остаток подписки.
- Поддержать все реально выдаваемые окна: rolling/5 часов, weekly, monthly.
  Месяц нельзя класть в поле `weekly` или терять при записи в БД.
- Текущий `SubscriptionLimitSnapshot` имеет только `five_hour` и `weekly`.
  Сделать минимальное расширение на список типизированных окон; проверить
  persist/load/render/revision и совместимость старых Codex/Grok-снимков.
  Если нужна миграция — additive, без редактирования применённых migrations.
- У каждого окна: стабильный ID/название, remaining/used с явной семантикой,
  reset_at из источника, состояние и область действия. Не вычитать проценты,
  пока не установлено, что именно означает поле API.
- Нулевой остаток — валидное измерение. `rate-limited` внутри успешного usage
  ответа не означает ошибку JSON. Не путать его с HTTP 429 самого collector API.
- Отдельно хранить время попытки сбора и время фактического измерения/получения
  актуального серверного ответа. Перечитывание кэша/лога не омолаживает данные.
- Дедупликация — по реальной области квоты, не просто по совпадению модели
  или пути к файлу. Слот выполнения может быть общим на аккаунт, а meter —
  общим, workspace-scoped или model-scoped. Это разные вещи.
- [Документация Go](https://opencode.ai/v2/docs/console/go) описывает три окна
  и модельно-зависимые allowance. Не вычислять из маркетинговой таблицы
  текущий процент и не выдавать общий meter API за независимые лимиты моделей.
  При расхождении документации/API честно обозначить измеренный scope.
- Ошибки auth, entitlement, permission, HTTP 429, timeout, stale, broken schema
  различаются. HTTP 403 сам по себе не доказывает отсутствие подписки.
- Чтение usage не меняет тариф, бюджеты, extra usage, маршрутизацию или paused
  задачи. Исчерпанная подписка не включает платный перерасход автоматически.

Polling встроить в существующий refresh `/update`/executor: ограниченная
параллельность, timeout, response-size limit, backoff/Retry-After, кэш и
coalescing одновременных запросов. Не блокировать async event loop и не занимать
слот исполнения модели ради HTTP-проверки лимитов.

Секрет читать через account auth boundary. Никаких ключей/tokens в исходниках,
аргументах shell, fixtures, HTML карты или Telegram. Запретить передачу
Authorization при redirect на иной origin; фиксированный allowlisted endpoint.
После ошибки одного аккаунта остальные продолжают обновляться.

### 3.4. Как принять

Offline fixtures с вычищенными ответами: три окна; разные reset; 0/100%;
fractional percent; rate-limited window; отсутствующее/некорректное поле;
401/403/429/5xx; HTML вместо JSON; timeout; oversized response; stale и clock
skew; несколько моделей одного account; разные B/C и workspace/pool scopes.
Проверить round-trip всех окон и обновление Telegram по content identity.

Live acceptance после offline-проверок: read-only сбор для каждого активного
OpenCode-аккаунта, сверка с кабинетом в близкий момент времени. Небольшая разница
из-за времени допустима, разные аккаунты и потерянный месяц — нет. Показать
данные без secrets и без оплачиваемых inference probes ради диагностики usage.
Без credentials допускается честный частичный результат, но не «сборщик готов».

## 4. Нормализация аккаунтов без нового большого фреймворка

### 4.1. Разделить четыре понятия

| Понятие | За что отвечает | Что не должно определять |
| --- | --- | --- |
| Account | Стабильная identity, auth owner, account-wide slots, lifecycle | Конкретную модель или роль |
| Profile | Runtime/adapter, модель, effort, capabilities | Новую копию того же аккаунта |
| Role assignment | Кто думает, ведёт, исполняет, проверяет в проекте | Хранение/обновление секретов |
| Quota scope | Чей meter и какое окно показано | Глобальный lock всех моделей |

`provider="pi"` сегодня описывает runtime, а OpenCode Go — сервис подписки.
Сборщик не должен определять billing provider по строке `pi` или суффиксу `-c`.

Минимальный целевой путь: один нормализованный account descriptor в конфиге
и явная ссылка из профиля. Текущий `ProviderProfileConfig` можно получать
компиляцией этого конфига, не переписывая сразу все adapters и БД.
Не заводить accounts микросервис, шину, plugin marketplace и второй scheduler.
Конкретные новые типы/таблицы вводить только под доказанную необходимость.

Account-level поля: стабильный ID, сервис подписки/тип подключения, auth method
и secret/state reference, разрешённый runtime binding, concurrency/control
reservation, lifecycle, возможности usage collector. Model/effort/capabilities
и назначения ролей остаются profile/assignment-level.

Если временно поддерживаются старые inline-поля, они только вход в одну
нормализацию. Конфликт двух источников — ошибка конфигурации, не «последний
overlay победил». Скомпилированное представление не второй редактируемый registry.

### 4.2. Одно хранилище и один владелец auth

Для сервисов — канонические account-state/secret paths вне личного home.
Не вводить fallback-сканирование `~/.codex*`, `~/.opencode*` и чужих логов
при каждом вызове. Импорт выбранного владельцем аккаунта — отдельная операция
с проверкой identity/workspace и прав, а не постоянная неявная синхронизация.

Для OAuth: один refresh owner на реальный аккаунт; short-lived refresh lock,
atomic replace и защита от одновременного refresh token rotation. После падения
процесса состояние либо старое валидное, либо новое целое, не половина JSON.
`invalid_grant`/revoked refresh → понятное `reauth_required`, не вечный цикл.
Повторный login проверяет того же владельца/workspace; подмена требует явного
решения. Refresh auth не является автоматическим retry отправленной задачи.

Проверить существующий sandbox contract: сейчас Pi получает read-only seed,
копирует auth в приватный tmpfs на invocation. Refresh только внутри временной
копии может потеряться, а бесконтрольная обратная запись из sandbox опасна.
Выбрать поддерживаемый host-side/CLI auth owner, не давать агенту запись в
канонические credentials. Учесть UID mapping rootless Docker и обновление
токена во время долгого invocation; общий «refresh cron» без этого не готов.

Для static credentials, если они ещё поддержаны: явная отдельная стратегия,
без выдуманного OAuth refresh. Display name/secret rotation не должны менять
логическую account identity. Изменение реального principal/policy требует
предусмотренного fencing/migration, а не тихой подмены под тем же ID.

### 4.3. Детерминированное поведение и наблюдаемость

- Одинаковая конфигурация даёт одинаковые account/profile bindings и порядок
  выбора; не зависеть от порядка `iterdir`, времени старта или первого auth-файла.
- Account ID не вычислять из токена, модели или отображаемого имени. Два
  аккаунта одного провайдера различаются; модели одного аккаунта делят slots.
- В API и CLI один account admission contract; provider-specific auth/usage
  остаются небольшими adapters. Новый тип provider может требовать adapter,
  новый аккаунт уже поддержанного типа — нет.
- Сохранять независимость lead/read-only control и worker при общей подписке.
  Не обещать неограниченную параллельность вопреки upstream ограничениям.
- Не снимать uncertain ticket по таймеру. Смена ID не должна обойти старую
  reservation или разрешить повторный внешний эффект.
- `enabled`, видимость дашборда, состояние login и наличие подписки различать.
  Скрытие Grok A вчера не отключило его executor/fallback roles — это нужно
  явно показать в inventory и предложить отдельное решение, не подменять смысл.
- Неизвестный usage не равен отсутствию подписки. Реальная квота провайдера,
  admission limit, внутренний token budget и денежный ceiling — разные причины
  остановки, с разными действиями владельца.
- Изменение выбора модели действует на новые вызовы. Старые pinned назначения
  нельзя «исправлять» сегодняшним default или автоматически переписывать.

Нужна одна операторская поверхность list/check/status/import/reauth/retire
в существующем CLI либо минимальном CLI-модуле. Это названия операций, не
утверждение о существующих командах. Check по умолчанию read-only и не запускает
модели. Вывод: account → profiles/roles → auth type/health → quota scopes/freshness
→ занятые slots и реальная причина недоступности, без секретов.

Сдача подключения: новый тестовый аккаунт поддерживаемого типа добавляется
одним описанием + безопасным login/import + проверкой + документированным
reload/restart. Не требует Python-правок или ручного добавления в dashboard.

### 4.4. Миграция существующих аккаунтов

Сначала dry-run inventory: configured accounts/profiles, legacy synthetic IDs
из `effective_account_id`, пути/ACL/UID, auth method, workspace binding,
назначения, fallback, active/paused runs, frozen revisions, tickets, snapshots.
Никаких secrets и полных auth documents в отчёт.

Затем одна таблица old → canonical и операторский migration path. Особое
внимание Codex B/C с приватными home paths и нескольким Pi-моделям C.
Не удалять старые файлы/БД/identity при переносе. Сначала backup, проверка прав,
auth refresh и read-only status, затем canary в изолированном проекте, rollback.
При активном/uncertain вызове — drain или явно отложенная миграция.
Не менять provider command/runtime image без проверки его certification pins.

## 5. Полная карта сценариев: факты, не ещё один идеальный план

### 5.1. Что именно построить

Локально открываемый read-only canvas со схемами, поиском, фильтрами, pan/zoom
и карточкой выбранного узла/сценария. Наверху — понятная обзорная карта, дальше
можно раскрыть конкретный путь, его владельца, остановки и ссылки на код/тесты.
Не одна огромная нечитаемая простыня из всех функций и не набор скриншотов.

Предпочтительный простой формат:

- `docs/system-map/scenarios.json` — единственный редактируемый каталог данных;
- `docs/system-map/index.html` — автономный HTML/SVG canvas с встроенными данными;
- `docs/system-map/README.md` — как открыть, обновить и проверить полноту;
- небольшой builder/validator — только если нужен для этого экспорта.

Другой репозиторный формат допустим, если так же удобно открывается и не требует
платного сервиса. Для HTML не нужен dev server, CDN, npm-фреймворк, новый backend
или БД. Данные встраивать безопасно, не через небезопасный raw HTML. Canvas
ничего не запускает, не читает credentials и не обращается к production.
Он не обязан визуализировать live-вызовы: это карта архитектуры и сценариев,
а не отдельная observability-платформа.

### 5.2. Как доказать полноту

Сначала перечислить ВСЕ фактические входы: Telegram commands, callback families,
text/voice/document handlers, MCP tools, HTTP routes, CLI entrypoints,
периодические jobs и outbox destinations. Найти их consumers/handlers и flags.
Это конечный проверяемый список, в отличие от всех возможных формулировок задач.

Для каждого входа: сценарий или явное основание исключения. Для каждой outbox
destination: кто пишет, кто читает, условия retry/dead-letter, куда идёт дальше.
Потерянный consumer/неработающий feature flag показывать как находку, не дорисовывать.
Проверять runtime configuration read-only отдельно от наличия Python-модуля.

Для каждой карточки сценария:

- стабильный ID, понятное название, пример пользовательского действия;
- вход, principal, проект/topic/session, guards и feature flags;
- шаги, кто принимает решение: owner, Jev, lead, sequencer, worker, reviewer,
  supervisor, applier; где модель, а где детерминированная политика;
- какие данные/состояния сохраняются и кто имеет право их менять;
- побочные эффекты, sandbox/network boundaries и approvals;
- async handoff, idempotency/fencing, correlation IDs и место логов;
- критерий успеха; ожидание, отказ, error, pause/cancel/resume/restart;
- конкретные source paths + symbols, существующие tests/trace evidence;
- что заменяет legacy-путь и какие данные мешают его удалению.

Не смешивать классификации в один значок. Нужны отдельные поля:
реализован/частично/не реализован; включён/выключен/не проверен в live;
canonical/legacy/experiment/операторский; доказан тестом/live/только чтением кода.
Зелёный unit-тест не доказывает живой продовый сценарий.

Цвета и условные линии помогают, но подписи обязательны: работают не только
цветом. Клик по legacy должен показывать, используется ли он реально, а не
только когда файл был написан. Planned-возможности не рисовать как существующие.

### 5.3. Обязательные семейства сценариев

1. Проектный текст/голос/документ → intake → intent → ответ или поручение.
2. `/think` и `/lead`: выбор собеседника, read-only discussion, frozen mode/model,
   явное исполнение. Исследовательская read-only делегация — пометить фактическую
   поддержку отдельно, не считать её реализованной по нашему старому обсуждению.
3. Managed goal → lead turn → план/пакет → sequencer → исполнение → review/redo
   → продолжение → acceptance результата; полномочия модели и владельца.
4. Старые прямые задачи, `/lead <goal>`, `/plan`, старые preferences, planner routes,
   horizon paths и feature-off поведение. Что дублирует canonical route?
5. MCP: ровно фактические `list_projects`, `get_project_status`, `get_result`,
   `submit_request`; OAuth principal/grant → тот же application → receipt и
   provenance в Telegram. Проверить содержание result, не судить по имени tool.
6. Общий inbox/личный ассистент, создание/import проекта, repository/topic и
   provisioning. Где уже есть работающий путь, а где пока только задумка?
7. CLI и API исполнение, разные provider adapters, readonly sandbox и coding
   sandbox, worktree, proxy/network, validation, commit/apply/publication/prod.
8. Независимое ревью, captured evidence, переделка, отсутствие доказательств,
   ручное согласование и действия с высоким риском.
9. Research/non-code задачи, параллельные поручения и зависимости. Не изображать
   существующую линейную очередь как уже работающий произвольный DAG.
10. Новый стек/инструмент: capability request → owner approval → install/build
    → доступность для следующих задач; denied/failed/repeated request.
11. Account onboarding/login/refresh/reauth/retire, model roles/fallback/pins,
    quota collectors, dashboard `/update`, различия budget/quota/ceiling.
12. Долгое исполнение: лидер отвечает при занятом worker; контроль slots,
    timeouts, cancel/pause, crash/reaper, unknown outcome, идемпотентный retry.
13. Scope edit/feedback/redo, approval callbacks и stale buttons, pause/resume
    при изменившемся конфиге, отсутствие автоматического запуска после статуса.
14. Memory/context/recovery summary и границы доступа к документам/секретам.
15. Outbox/inbox/delivery, Telegram HTML/splitting/retries/dead-letter,
    статус/история/provenance/runtime journal и возможности оператора.
16. Эксперименты/certification/canary, backup/restore, retention/disk pressure,
    deploy/rollback и remote nodes — включённость выяснить, не предполагать.

К каждой основной успешной ветке добавить хотя бы timeout, auth failure,
исчерпанную квоту, owner wait/deny, crash/restart и uncertain external effect,
если они применимы. «Здесь ждём» без события пробуждения/срока/пути выхода —
кандидат на системный тупик, который должен попасть в список находок.

### 5.4. Проверка карты и список упрощений

Validator проверяет уникальные IDs, ссылки/edges, обязательные поля и наличие
указанных файлов/symbols/test IDs насколько возможно без запуска production.
Отдельный coverage report сверяет каталог входов и outbox destinations.
Непокрытые входы перечислять явно, не сообщать «всё покрыто» по количеству схем.

Сдать 5–10 конкретных KISS/YAGNI/Pareto-находок: дубли, разные владельцы одного
решения, мёртвые consumer, unbounded waits, расхождения login/доступности,
чрезмерно сложные конфиги. Для каждой: evidence, пользовательский эффект,
небольшое действие и риск миграции. Legacy удалять только после отдельного
решения и проверки persisted данных. Этот документ не разрешает массовую чистку.

## 6. Порядок работы и критерии сдачи

1. **Baseline и inventory.** Зафиксировать текущее состояние, auth-различия,
   конечный список входов и черновую карту. Короткий отчёт с нерешёнными вопросами.
2. **Минимальный account/auth boundary.** Уточнить поддерживаемый OAuth для
   нужного Go-пути, единый resolver и refresh ownership. Не расширять права
   sandbox. Статические старые записи не удалять в процессе исследования.
3. **Вертикальный collector slice.** OpenCode B/C → реальные окна → DB → Telegram,
   с tests и read-only evidence; без обязательной предварительной переписи всех
   аккаунтов. Если контракт одного provider требует маленького descriptor —
   выделить его, а не откладывать сборщик до конца большого рефакторинга.
4. **Нормализация остальных аккаунтов.** Dry-run migration, исправление Codex
   paths/auth refresh, единый check/onboarding, испытание нового test account
   без изменения Python. Отдельные account/profile changes обратимы.
5. **Canvas и финальная сверка.** Довести карту до покрытия входов/consumers,
   добавить evidence, legacy/canonical differences и конкретный backlog уборки.
   Нельзя закончить одной красивой картинкой без source/test links.
6. **Сдача и rollout plan.** Проверенные команды, результаты/skip, changed files,
   миграции, ручные login steps, известные ограничения, rollback. Production
   меняется только при отдельном действующем разрешении владельца.

Минимальная acceptance matrix:

| Проверка | Что должно быть доказано |
| --- | --- |
| OpenCode B/C | Верные аккаунты, три окна либо честные missing windows, сверка с кабинетом |
| OAuth | Правильный Go entitlement; expiration/refresh/rotation/revocation; restart без потери auth |
| Несколько моделей C | Общий реальный account, без дублированных auth и обхода slots; корректный scope квоты |
| Lead + worker | Контроль и read-only ответ доступны при длительном worker, upstream ограничения соблюдены |
| Новый аккаунт | Config/login/check/reload, без hardcoded IDs и Python-правок |
| Ошибки | Ни 100% из неизвестности, ни бесконечного refresh/retry, ни silent PAYG fallback |
| Карта | Каждому входу/consumer есть соответствие, code/test links, status и failure exits |
| Совместимость | MCP/Telegram/пауза/pins/journal/cleanup/account fencing не регрессировали |

Работать с отдельной disposable PostgreSQL DB: integration fixtures делают
TRUNCATE и могут завершать соединения. Не подставлять production DSN.
Не возобновлять исходную paused-задачу, не делать платные вызовы ради smoke test,
не публиковать canvas с приватными runtime данными в интернет.

Сейчас не нужно объединять весь production Compose/systemd стек, переписывать
planner/lead или создавать новую систему бюджетов. Долг многослойного деплоя
отметить на карте и в follow-up; при любом rollout все затронутые процессы
должны получать согласованный конфиг, не только Telegram-контейнер.

## 7. Где начинать в коде

| Область | Основные точки |
| --- | --- |
| Config/normalization | `src/vuzol/config/{models,loader,registries,runtime,revision}.py` |
| Account admission | `src/vuzol/providers/{accounts,account_persistence,registry,health}.py` |
| Usage | `src/vuzol/providers/{subscription_limits,grok_limit_snapshot}.py`, `src/vuzol/cli/executor.py` |
| Pi/runtime/auth seed | `src/vuzol/providers/pi.py`, `src/vuzol/execution/{sandbox,codex,turn_transport}.py` |
| Identity/journal | `src/vuzol/execution/{identity,journal,reconciliation}.py` |
| Model selection | `src/vuzol/projects/{model_matrix,executor_preference}.py`, `src/vuzol/telegram/model_matrix.py` |
| Вход/режим/intent | `src/vuzol/telegram/{ingress,durable_ingress}.py`, `src/vuzol/interpretation/`, `src/vuzol/discussion/conversation.py` |
| Lead/MCP | `src/vuzol/leadership/{access,commands,service,worker}.py`, `src/vuzol/mcp/{auth,server}.py` |
| Пакет/задача | `src/vuzol/discussion/{application,sequencer,horizon}.py`, `src/vuzol/workflows/` |
| Установка/доставка | `src/vuzol/projects/{provisioning,dependency_provisioning,capability_provisioning}.py`, `src/vuzol/execution/result_apply.py`, `src/vuzol/ops/` |
| Проекции | `src/vuzol/telegram/{projections,delivery}.py`, `src/vuzol/leadership/{card,execution_view}.py` |
| Тесты | `tests/unit/providers/test_subscription*`, `tests/unit/execution/test_execution_identity.py`, `tests/integration/{providers,leadership,telegram}/` |

Это точки старта, не утверждение о полноте карты. Инвентаризацию надо вывести
из кода/конфига, а не скопировать названия директорий в красивую схему.

## Короткий стартовый промпт

> Прочитай полностью `docs/ASTRA_ACCOUNTS_LIMITS_SCENARIO_MAP_2026_10_23.md`.
> Это текущий scope: реальный OpenCode usage collector, нормализация аккаунтов
> с проверкой актуального OAuth/Go entitlement и полная фактическая карта сценариев.
> Сначала сохрани dirty baseline без потери чужих изменений, затем выполняй этапы
> небольшими проверяемыми срезами. Не переписывай оркестрацию, не упрощай защитные
> границы, не трогай production без отдельного разрешения. Не подменяй OAuth/Go
> старым ключом или Zen PAYG ради зелёного теста. Отчитывайся фактами и evidence;
> отдельно показывай, что реально проверено и что пока заблокировано.
