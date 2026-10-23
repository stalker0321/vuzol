# OpenCode OAuth usage: дельта поверх сдачи 23 октября

Продолжение после этого среза: [сервисное подключение и выкладка T138](OPENCODE_USAGE_SERVICE_2026_10_23.md).
Ниже сохранён исходный отчёт до выкладки.

Статус: **OAuth usage C подтверждён настоящим GET под UID владельца**,
включая executor → disposable PostgreSQL → render → чтение новым процессом.
Сервисное подключение и автономное обновление OAuth ещё не завершены.
Production не менялся.
Дополняет [предыдущую сдачу](ACCOUNTS_LIMITS_PROGRESS_2026_10_23.md) и
[задание с референсами bb](ASTRA_BB_REFERENCES_2026_10_23.md).

## Что уже было → какая дельта

Сохранены AccountConfig/account_ref, typed windows, тариф с provenance,
executor `/update`, JSONB persistence, HTTP bounds/cache/coalescing, static Pi
key path и карта. Нового scheduler, proxy или inference adapter нет.

- `AccountConfig.quota_oauth`: usage-only привязка к точным SQLite store,
  account, org и (v2) credential ID. Это **не переключение Pi на OAuth**.
  Явно выбранный оператором personal store разрешён только как read-only source;
  запрет personal homes для writable runtime state остаётся. Это исключает
  обязательную миграцию рабочего входа только ради чтения usage.
- `providers/opencode_oauth.py`: read-only resolver v1 account / v2 credential.
  Явная версия, migration marker, issuer/account/org/expiry/permissions checks.
  Нет home scan, выбора desktop-active account, fallback на auth.json/env/ключ.
- OAuth GET `/inference/go/v1/usage` + `x-opencode-org-id`; отдельный parser
  `usage.{rolling,weekly,monthly}.{percent,status,resetsAt}`. Static path
  `/zen/go/v1/usage` сохраняет прежний parser. Нет эвристического schema fallback.
- Auth проверяется перед cache hit: rotation меняет cache generation и позволяет
  выйти из cached 401 без рестарта. Logout/expiry не маскируются успешным кэшем.
  Auth fence после HTTP отвергает ответ токена, изменённого во время запроса.
- SQLite `mode=ro`, `query_only`, bounded busy/progress timeout; WAL читается
  штатно, не через `immutable=1`, который может скрыть rotation. Credential DB
  не меняется. SQLite может обслуживать служебный WAL/SHM; их ACL тоже важны.
- Новый source поддержан в persist/load/валидации трёх окон/render Telegram.
  Старый static snapshot не считается подтверждённым OAuth-org evidence.
- `accounts check` показывает usage identity/expiry/ACL отдельно от inference.
  `list/check/status` не делают HTTP. `accounts usage --account ID` явно читает
  только один enabled Go account; exit 0 — успех, 2 — отказ collector,
  1 — неверная конфигурация/selection. Никакого model call/auth refresh.
- Usage metadata исключена из profile revision **и** execution account binding:
  смена meter раньше могла заблокировать сохранённый run при неизменном inference
  аккаунте. Runtime identity/state path и прочие execution guards сохранены.

## Референсы и совместимость

Полностью прочитаны
[bb opencode-usage.ts](https://github.com/get-bb/bb/blob/2884950f17e76f78b28c64ccf4a2e9a7c74188a8/packages/provider-bridge-acp/src/bridge/opencode-usage.ts)
и [его тесты](https://github.com/get-bb/bb/blob/2884950f17e76f78b28c64ccf4a2e9a7c74188a8/packages/provider-bridge-acp/src/bridge/opencode-usage.test.ts).
bb не устанавливался, его tests не запускались. Python написан самостоятельно;
существенные фрагменты MIT-кода не переносились.

V2 store/device metadata сверены с OpenCode `096c2ba5551d1f8000148b1bf4d167888fda212c`:
[credential/sql.ts](https://github.com/anomalyco/opencode/blob/096c2ba5551d1f8000148b1bf4d167888fda212c/packages/core/src/credential/sql.ts),
[OAuth provider](https://github.com/anomalyco/opencode/blob/096c2ba5551d1f8000148b1bf4d167888fda212c/packages/core/src/plugin/provider/opencode.ts).

Host `opencode --version` = **1.18.32**; tag `545f51d26cc39a907d2867492d498d9607ea5fa4`:
[account repo](https://github.com/anomalyco/opencode/blob/545f51d26cc39a907d2867492d498d9607ea5fa4/packages/opencode/src/account/repo.ts),
[Console CLI](https://github.com/anomalyco/opencode/blob/545f51d26cc39a907d2867492d498d9607ea5fa4/packages/opencode/src/cli/cmd/account.ts).
Локально проверен `opencode console login --help`; **login не выполнялся**.
V2 требует marker `20260805200742_import_legacy_credentials`; v1 с ним отвечает
`auth_store_migrated`, не оживляет старые credentials.

## Инструкция подключения и границы

Ниже исходный runbook. Выполненные шаги с найденным `oc3` — в разделе
«Живой прогон»; новый login/миграция/production rollout не выполнялись.

1. Подтвердить OAuth store именно C. Pi API key для нового endpoint
   не подходит. Не искать credentials по всему home и не экспортировать cookies.
2. Если входа нет, согласовать отдельный canonical каталог и одного auth owner
   UID. OpenCode владеет login/rotation; collector имеет нужный read/traverse
   доступ к DB/WAL/SHM, model sandbox не может писать этот store. Не менять ACL
   действующего Pi state и не открывать целиком home.
3. После согласования под auth owner выполнить установленный CLI с изоляцией
   XDG. Это пример путей, **не уже созданная конфигурация**:

   ```sh
   env XDG_DATA_HOME=/var/lib/vuzol-provider-state/opencode-usage-c/data \
     XDG_CONFIG_HOME=/var/lib/vuzol-provider-state/opencode-usage-c/config \
     XDG_STATE_HOME=/var/lib/vuzol-provider-state/opencode-usage-c/state \
     XDG_CACHE_HOME=/var/lib/vuzol-provider-state/opencode-usage-c/cache \
     opencode --pure console login https://opencode.ai/console
   ```

   Browser/MFA делает владелец. Проверить actual schema, account/org и выбранную
   организацию: первый org, выбранный CLI, не обязательно наш C. Секреты в отчёт
   не копировать. Унаследованные provider env/config не использовать как fallback.
4. В **отдельном тестовом registry**, сохранив inference binding, добавить к
   проверенному элементу `accounts` (идентификаторы заменить подтверждёнными):

   ```toml
   quota_collector = "opencode_go"
   quota_scope_id = "opencode:organization:ORG_ID:account:ACCOUNT_ID"
   [accounts.quota_oauth]
   store_path = "/var/lib/vuzol-provider-state/opencode-usage-c/data/opencode/opencode.db"
   store_format = "account_v1"
   account_id = "ACCOUNT_ID"
   org_id = "ORG_ID"
   refresh_owner = "opencode_cli"
   ```

   Для v2 — `store_format="credential_v2"` и явный `credential_id`. Secret в TOML
   нет. Scope обязан точно соответствовать account/org, не email/display label.
5. Под **collector service UID**, кодом этого среза и тестовым registry:

   ```sh
   python -m vuzol.cli.accounts check --registry /path/to/reviewed-test-registry.toml
   python -m vuzol.cli.accounts usage --registry /path/to/reviewed-test-registry.toml --account opencode-go-c
   ```

   Сверить три окна и org с кабинетом. C used 0/28/58 — прежнее наблюдение
   владельца, не fixture/новое измерение. B без подписки, A disabled: не менять их.
6. Доказать **автоматический** refresh: один writer, canonical rotation,
   наблюдение новым collector, restart, remote revoke. `refresh_owner` — лишь
   декларация, не запущенный daemon/доказательство. Collector не refresh-ит.

Первое подключение descriptor к legacy inline profile всё ещё требует отдельного
dry-run/pin audit. Тесты стабильности usage metadata не разрешают массовую
миграцию legacy bindings. Отключение теста: прекратить `accounts usage`; в отдельно
проверенном registry отключить collector/visibility. Не удалять auth и не делать
скрытый возврат на старый ключ. Production deploy требует отдельного решения.

## Уточнение владельца: oc1/oc2/oc3

Владелец указал aliases: oc1=A, oc2=B, oc3=C. Прочитана только нужная definition
в `.bashrc`; aliases не запускались. C явно указывает на
`/home/vodkolyan/.opencode-profiles/3/data/opencode/opencode.db`.
Read-only SQL извлёк **только metadata** выбранного store, не ключи/tokens:
есть v1 account + account_state, официальный Console issuer, account/org;
v2 import marker отсутствует, записанный expiry **2026-11-12 19:35:05 UTC**.
Это не подтверждение сервером действительности токена/подписки.

ACL: home 750, каталог store 775, DB/WAL/SHM 644, UID/GID 1000.
Home ограничивает доступ извне; не утверждаем, что credentials доступны всем.
При этих исходных правах file boundary возвращал `auth_permissions`.
Владелец разрешил сузить **только** каталог store до 700 и DB/WAL/SHM до 600,
затем сделать read-only usage. Разрешённое изменение выполнено; UID/GID остались
1000:1000. `.bashrc`, токены, Pi state, остальные аккаунты и prod не изменялись.
Повторный login не потребовался.
Operator probe не заменяет проверку production collector UID/refresh owner.

## Живой прогон 23 октября, 01:05–01:09 UTC

Изолированный registry и scripts находятся в приватном каталоге
`/home/vodkolyan/vuzol-oauth-live-20261023.Xxq6yz` (0700). Секреты туда не
копировались. `permissions.txt` фиксирует прежние/новые права, `usage-c.toml`
содержит лишь явную identity/org/path и прежние Pi runtime facts. Это не
полная копия production профиля и **не кандидат для установки в production**.

1. `accounts check`: локально читается v1 OAuth C, expiry 12 ноября;
   inference credential не читался, network_requests=0.
2. `accounts usage --account opencode-go-c`: успешный GET нового endpoint,
   observed_at **2026-10-23 01:05:36 UTC**, source `opencode_go_oauth_v1`, Go.
3. Настоящий executor refresh повторил GET в **01:09:16 UTC**, сохранил snapshot
   в **нашу disposable БД** `vuzol_accounts_test` на `127.0.0.1:32789`.
   Outbox refresh acknowledged; число model account leases не изменилось.
4. Отдельный новый OS-процесс прочёл сохранённое измерение без HTTP/auth-read,
   восстановил три окна и отрендерил ту же карточку. Это не рестарт production.
5. Telegram sender не запускался; Telegram mutations = 0. Модельных вызовов,
   POST refresh/login, смены тарифов или PAYG fallback не было.

| Окно | Использовано | Осталось | Reset UTC |
| --- | ---: | ---: | --- |
| 5 часов | 0% | 100% | 23.10 05:44:10 |
| Неделя | 28% | 72% | 26.10 00:00:00 |
| Месяц | 58% | 42% | 13.11 18:09:57 |

Это **новое серверное измерение**, совпавшее с прежней сверкой владельца.
Не подстановка cabinet numbers в fixture. Исходный legacy-key 403 не был
обойдён сменой ключа: новый путь использует уже существующий Console OAuth.

Точные команды из корня repo (код и disposable venv, не production release):

```sh
PYTHONPATH=src:.:/tmp/vuzol-mcp-deps-20261022 PYTHONDONTWRITEBYTECODE=1 \
 /tmp/vuzol-stage2.jzwfpD/venv/bin/python -m vuzol.cli.accounts check \
 --registry /home/vodkolyan/vuzol-oauth-live-20261023.Xxq6yz/usage-c.toml
PYTHONPATH=src:.:/tmp/vuzol-mcp-deps-20261022 PYTHONDONTWRITEBYTECODE=1 \
 /tmp/vuzol-stage2.jzwfpD/venv/bin/python -m vuzol.cli.accounts usage \
 --registry /home/vodkolyan/vuzol-oauth-live-20261023.Xxq6yz/usage-c.toml --account opencode-go-c
env -u VUZOL_DATABASE_DSN_REFERENCE PYTHONPATH=src:.:/tmp/vuzol-mcp-deps-20261022 \
 PYTHONDONTWRITEBYTECODE=1 /tmp/vuzol-stage2.jzwfpD/venv/bin/python \
 /home/vodkolyan/vuzol-oauth-live-20261023.Xxq6yz/probe_flow.py
env -u VUZOL_DATABASE_DSN_REFERENCE PYTHONPATH=src:.:/tmp/vuzol-mcp-deps-20261022 \
 PYTHONDONTWRITEBYTECODE=1 /tmp/vuzol-stage2.jzwfpD/venv/bin/python \
 /home/vodkolyan/vuzol-oauth-live-20261023.Xxq6yz/probe_flow.py --read-only
```

Container проверен до записи: `vuzol-accounts-test-20261023`, label
`vuzol.disposable-test=accounts-20261023`, loopback port 32789, PG data в tmpfs.
Script дополнительно проверяет URL и `current_database()`; TRUNCATE/удалений нет.
Два первых запуска script остановились до соединения: factory требует scheme
`postgresql+psycopg`, а probe передал `postgresql`. Исправлен только DSN
одноразового script; runtime guards/assertions не ослаблялись.

### Что мешает постоянной сервисной работе

`systemctl show vuzol-executor.service`: User/Group=`vuzol-executor`,
`ProtectHome=read-only`. Реальная проверка
`sudo -n -u vuzol-executor test -r <точный oc3 opencode.db>` вернула exit 1.
Executor не получил доступ к личному home; новый код/registry не выкладывались.
Не следует просто открыть всю OpenCode DB сервису: кроме auth там может быть
история пользователя. Предстоит согласованный canonical auth ownership либо
передача ограниченного snapshot, без второй копии rotating refresh token.

В [источнике установленного OpenCode 1.18.32](https://github.com/anomalyco/opencode/blob/545f51d26cc39a907d2867492d498d9607ea5fa4/packages/opencode/src/account/account.ts)
refresh вызывается при resolve token, если осталось менее пяти минут. Это
не доказательство постоянного refresh при неиспользуемом `oc3`; показанный cache
дедуплицирует внутри процесса, а межпроцессной блокировки в этом пути не видно.
Добавлять второго writer рядом с CLI без общего ownership нельзя. Live rotation,
revocation и изменение способа хранения/служб в это разрешение не входили.

## Проверки

Из корня репозитория, только disposable venv/БД:

```sh
PYTHONPATH=src:.:/tmp/vuzol-mcp-deps-20261022 PYTHONDONTWRITEBYTECODE=1 \
 /tmp/vuzol-stage2.jzwfpD/venv/bin/python -m pytest tests/unit --no-cov -q --tb=short -rs

env -u VUZOL_DATABASE_DSN_REFERENCE \
 VUZOL_TEST_DATABASE_DSN=postgresql://vuzol_accounts_test:disposable-local-only@127.0.0.1:32789/vuzol_accounts_test \
 PYTHONPATH=src:.:/tmp/vuzol-mcp-deps-20261022 PYTHONDONTWRITEBYTECODE=1 \
 /tmp/vuzol-stage2.jzwfpD/venv/bin/python -m pytest \
 tests/integration/providers/test_opencode_usage_boundary.py \
 tests/integration/providers/test_model_assignments.py \
 tests/integration/telegram/test_projections.py --no-cov -q --tb=short -rs
```

- Последний полный unit, включая explicit personal readonly source:
  **3684 passed, 1 skipped**, 89.93s, 11 warnings. Skip —
  `test_remote_out_of_scope.py`, не пройденная проверка.
- Integration выборка выше: **32 passed**, 11.46s, 9 warnings; не вся suite.
- Targeted OAuth + operator CLI: **58 passed**, 2.54s.
- После разрешения явного personal readonly source: **177 passed**, 3.19s:
  `PYTHONPATH=src:.:/tmp/vuzol-mcp-deps-20261022 PYTHONDONTWRITEBYTECODE=1
  /tmp/vuzol-stage2.jzwfpD/venv/bin/python -m pytest
  tests/unit/providers/test_opencode_oauth.py tests/unit/config --no-cov -q --tb=short`.
- Scoped Ruff/mypy, `git diff --check`, map rebuild/validator прошли.

```sh
/tmp/vuzol-stage2.jzwfpD/venv/bin/ruff check src/vuzol/config/models.py \
 src/vuzol/config/revision.py src/vuzol/cli/accounts.py \
 src/vuzol/providers/opencode_oauth.py src/vuzol/providers/opencode_usage.py \
 src/vuzol/providers/subscription_limits.py tests/unit/providers/test_opencode_oauth.py \
 tests/unit/config/test_accounts_cli.py tests/integration/providers/test_opencode_usage_boundary.py
/tmp/vuzol-stage2.jzwfpD/venv/bin/mypy --follow-imports=silent \
 src/vuzol/config/models.py src/vuzol/config/revision.py src/vuzol/cli/accounts.py \
 src/vuzol/providers/opencode_oauth.py src/vuzol/providers/opencode_usage.py \
 src/vuzol/providers/subscription_limits.py
python devtools/system_map.py --check
git diff --check
```

Первая OAuth integration-итерация: 1 failed/3 passed — карточка не показывала
окна нового source; исправлен loader/renderer, assertions не ослаблены.
При расширении unit fixture были две ошибки сборки пустых registries; исправлена
fixture, не assertions. Warnings не скрыты: старые pytest tmp cleanup, SDK/PTB
deprecations, SQLAlchemy unchecked-in connection в существующем Telegram тесте.
Чужие tmp не удалялись. Результаты старых suites — в предыдущей сдаче.

Trace fake-transport: explicit OAuth C → точный credential/org → один GET → три
окна → executor refresh receipt → JSONB → новый engine → тот же snapshot →
Telegram Go/окна. Execution leases отсутствуют. Это reload composition,
не OS restart, не живой Telegram mutation и не authenticated live measurement.

## Открытое и сохранность

- Operator OAuth usage C и сверка кабинета закрыты. Service UID/ACL и deployment
  остаются открыты; успешный operator probe не закрывает их автоматически.
- Canonical автоматический refresh, restart/revoke/long-lived evidence.
  **Нельзя обещать «без ручного ремонта», пока подтверждён только reader.**
- QuotaWindow сохраняет контракт 0..100: overage сейчас schema error,
  не молчаливое обрезание как в bb. Расширение контракта — отдельно.
- P1 conformance/explainable wait не начинались вместо P0; F9 Codex/Grok HTTP открыт.

Ветка `agent/accounts-limits-map-20261023`, HEAD `f5a3422`. Новых коммитов,
push/deploy нет, чужой dirty tree сохранён. Перед дельтой проверен checkpoint
`/home/vodkolyan/vuzol-accounts-baseline-20261023-vj0f3l__` (939 files, Git bundle).
