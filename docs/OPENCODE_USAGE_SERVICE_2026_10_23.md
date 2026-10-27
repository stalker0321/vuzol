# OpenCode C: сервисный сбор лимитов и компактная карточка

23 октября 2026. Продовая цепочка работает: systemd exporter → очищенный файл →
executor под своим UID → PostgreSQL → Telegram. Delivery receipt подтверждён.
Релиз: `vuzol:t138-oauth-usage-r1`, host source `/opt/vuzol/releases/t138`.
Это продолжение [предыдущего отчёта](OPENCODE_OAUTH_USAGE_2026_10_23.md).

## Что изменилось

- `vuzol-opencode-usage.timer` запускает экспорт примерно раз в минуту.
  Процесс под `vodkolyan` читает ровно существующий OAuth store C. Executor
  получает только snapshot без токенов, email и истории OpenCode.
- Файл `/var/lib/vuzol/usage-export/opencode-c/usage.json` публикуется атомарно:
  каталог `vodkolyan:vuzol-executor 2750`, файл `0640`. Executor читает его,
  но по-прежнему не читает личную SQLite DB. Копий refresh token не создавали.
- `SubscriptionLimitSettings.opencode_snapshots` связывает файл с account ID,
  OAuth scope и хешем runtime account binding. Это настройка измерения вне
  execution registry: все 13 profile revisions совпали до/после выкладки.
- Consumer проверяет размер, тип/права файла, schema, три окна, scope, account,
  runtime binding, timestamps и свежесть до 15 минут. Ошибка заменяет прежний
  успех; устаревший снимок не превращается в 100%. Нет fallback на старый ключ.
- Executor сохраняет результат через прежний refresh/persist/outbox путь,
  вне слотов моделей. `/update` читает последний минутный снимок; авторизация
  и HTTP принадлежат экспортёру. Существующий executor tick — раз в 120 секунд.
- Карточка: `5h`, `7d`, `1d` для суточного Grok, `1mo` для месячного Go;
  проценты означают доступный остаток. Убраны полоски, «осталось», «сброс»,
  технические строки provenance; доказательства тарифа сохранены в БД.
  Время reset и «Обновлено» — `Europe/Amsterdam`, включая DST и перенос даты.
  Между аккаунтами пустая строка, исчерпанное окно отмечено ⛔.

## Владелец обновления OAuth

Токен обновляет установленный неизменённый OpenCode 1.18.32, командой
`--pure console orgs`. Его собственная логика обновляет token при остатке
менее пяти минут. Exporter запускает её только при необходимости; проверяет
версию CLI, единственный account в store и сохранённый срок после команды.
Успешный exit code сам по себе не означает успешный refresh.

`/usr/local/bin/opencode` теперь небольшой dispatcher из
`deploy/opencode-dispatch.py`. Только UID 1000 с XDG data C проходит через
`vuzol.cli.opencode_owner`. Прежние `oc3` функции, в том числе в открытых shell,
используют этот путь. Остальные аккаунты сразу запускают прежний native binary.
Native executable и `.bashrc` не менялись.

Operator CLI и обслуживание используют один `flock`. Native CLI наследует
descriptor замка через exec: смерть Python launcher не освобождает его раньше
CLI. Пока интерактивный C открыт, exporter может читать usage, но не запускает
второго refresh writer. Если токен при этом истечёт, карточка покажет ожидание.
Это сериализация интерактивных CLI одного auth store; параллелизм Pi-моделей
и отдельный control slot не менялись.

При изменении dispatcher или версии native CLI автоматический refresh
останавливается с понятной ошибкой. Обновление npm/OpenCode требует проверки
launcher, store schema и закреплённой версии. Прямой запуск native binary с
XDG C обходит управление: для этого store пользоваться обычным `opencode`/`oc3`.
У v2 остаётся read-only reader; автоматическое обслуживание закреплено на v1
установленного CLI, без догадок о новой схеме.

## Проверено

- Полный unit: **3706 passed, 1 skipped**, 91.84 s. Skip — прежний remote test;
  три предупреждения SDK/PTB deprecation. Затем отдельно добавлены проверки
  изменения launcher и наследования замка операторским CLI.
- Integration: **33 passed**, 13.56 s, disposable PostgreSQL `127.0.0.1:32789`,
  `vuzol_accounts_test`. Один прежний SQLAlchemy warning в Telegram test.
  Включает executor `/update` из snapshot без auth/HTTP/model slots, reload БД
  новым engine и формирование карточки.
- Ruff, mypy шести изменённых source modules, `git diff --check`; карта сценариев
  пересобрана с новыми CLI entrypoints, без непокрытых входов.
- Реальный systemd exporter успешен под UID владельца; отдельный probe
  `--verify-cli` выполнил установленный OpenCode и последующий live usage GET.
- Настоящий `vuzol-executor` UID прочитал snapshot и три окна; доступ к личной
  OpenCode DB по-прежнему отсутствует.
- В 05:49 UTC live значения C: used **0 / 28 / 58**, remaining **100 / 72 / 42**.
  Delivery receipt совпал с revision карточки. Её длина 1172 символа.
- Все шесть контейнеров и три host worker переведены на T138. Дополнительно
  перезапущены exporter и executor. Модельные leases/processes — 0, статусы
  задач и profile revisions сохранились; paused-задача не возобновлялась.

Важная граница проверки: действующий токен истекает 12 ноября. Мы проверили
настоящий запуск CLI, но не форсировали истечение/отзыв этого токена. Ротация
expired → CLI → durable token → новое чтение и ошибки refresh/revoke покрыты
синтетическими тестами; естественная production-ротация ещё не наблюдалась.
Перевода Pi inference на OAuth, новых модельных вызовов и миграции аккаунтов нет.

## Эксплуатация

Конфигурация без секретов: `/etc/vuzol/opencode-usage-c.json`.
Executor binding: `VUZOL_SUBSCRIPTION_LIMITS__OPENCODE_SNAPSHOTS` in `/etc/vuzol/executor.env`.

```sh
systemctl status vuzol-opencode-usage.timer
sudo journalctl -u vuzol-opencode-usage.service -n 20
sudo systemctl start vuzol-opencode-usage.service
```

Повторный read-only контроль БД и Telegram receipt:

```sh
docker exec -i repo-telegram-delivery-1 python - \
  < /home/vodkolyan/vuzol-release-t138/verify.py
```

Никаких SQLite migrations, удаления auth или ручного копирования токенов при
обычном обновлении лимитов не требуется. Для нового аккаунта нужны отдельная
проверенная usage identity, output binding и включение общего auth-owner lock.

## Откат

Сохранены T137 image/source, прежние systemd drop-ins и исходная ссылка
`/home/vodkolyan/vuzol-release-t138/opencode.before` (её target относительный;
не запускать её из backup-каталога). Схема БД не менялась.

Image-only rollback подготовлен и его preflight проверен:

```sh
VUZOL_DEPLOY_PREFLIGHT_ONLY=1 python3 /home/vodkolyan/vuzol-release-t138/deploy.py --rollback
```

Для фактического отката при отсутствии активных model calls: остановить три
host worker, выполнить эту команду без `VUZOL_DEPLOY_PREFLIGHT_ONLY`, переименовать
три конкретных `99-t138.conf` в `99-t138.conf.disabled`, `daemon-reload`, запустить
workers. Отключить `vuzol-opencode-usage.timer`. При полном удалении обслуживания
восстановить `/usr/local/bin/opencode` как symlink на
`../lib/node_modules/opencode-ai/bin/opencode.exe` атомарной заменой. OAuth store
и файлы данных удалять не требуется.

## Отдельные оставшиеся проблемы

В действующей карточке Codex B/C не читаются сборщиком из-за старых путей/ACL,
Grok B показывает устаревшее измерение, OpenCode B не подключён к новому OAuth
сборщику (по прежним сведениям владельца подписки нет). Это не исправлялось
переносом чужих credentials или подстановкой данных C. Видимость и execution
bindings этих аккаунтов сохранены. Старые F1/F9 и общий account migration
backlog этим релизом целиком не закрыты.
