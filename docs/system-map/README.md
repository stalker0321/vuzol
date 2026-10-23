# Карта сценариев Вузла

Открыть `index.html` непосредственно в браузере (`file://`): server, CDN,
npm, credentials и доступ к production не нужны. Есть обзор, поиск/фильтры,
pan/zoom, клавиатурный выбор узла и карточки со ссылками на исходники/тесты.

Единственный редактируемый каталог — `scenarios.json`. `template.html` содержит
только UI; `index.html` и `coverage.json` — генерируемые результаты.

Из корня репозитория:

```sh
python3 devtools/system_map.py --inventory
python3 devtools/system_map.py
python3 devtools/system_map.py --check
```

Validator проверяет файлы, symbols, IDs, edges, явные bindings и outbox consumers.
Новый обнаруженный вход без binding делает `--check` красным. Привязать его
нужно к содержательному сценарию, не удалить из inventory ради зелёного статуса.

Ограничение: это статический AST-аудит, не доказательство абсолютной полноты.
Dynamic registrations, external systemd/cron timers и отдельные deployment
flags требуют операторской сверки. Сгенерированный список нужно сопоставлять
с фактическими entrypoints; ошибки extractor исправляются тестами. Generic
outbox forwarders видны отдельными строками, не выдаются за новые очереди.

Текущий статус — первый фактический каталог, не закрытая приёмка всей системы.
Сценарии без сквозного live evidence оставлены `unverified`. Кнопка «Live-сверка»
показывает отдельно прочитанные runtime flags, deployment/DB inventory и сведения
владельца о кабинете; это не превращает сценарий в принятый на production.
Ссылки на тесты не означают запуск каждого файла в этой итерации.
Полная проверка UI в браузере и миграция frozen assignments пока не проведены.

«Очереди» показывает producers/consumers из AST и явно описанные guard/retry/DLQ/
next contracts. Отсутствующий контракт, неверный status, duplicate JSON key,
непокрытая callback action или несвежий HTML делают проверку красной.

Безопасность: JSON экранирован при встраивании, карточки строятся через
`textContent`, CSP запрещает сетевые соединения и формы. HTML ничего не запускает
в Вузле. Не добавлять raw auth, private journals или пользовательские prompts.
