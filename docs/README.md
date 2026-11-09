# Карта документации

Начинать с этого файла. Руководства описывают код в репозитории; живую установку
проверяйте по ней самой.

## Как это работает

- [Обзор продукта](../README.md).
- [Telegram](TELEGRAM.md): топики, карточки, кнопки.
- [Память руководителя проекта](LEAD_MEMORY.md): блокноты, итоги целей, ночной разбор.
- [Мониторинг](MONITOR.md): публичные источники и дайджест.
- [MCP](MCP.md), [провайдеры и профили](PROVIDERS.md), [настройка](CONFIGURATION.md).
- [Исполнение кода](CODING_RUNTIME.md), [окружение проекта](PROJECT_ENVIRONMENT.md),
  [удалённые ноды](SECOND_NODE.md), [контролируемый выход в сеть](NODE_CONTROLLED_EGRESS.md).
- [Границы ревью](REVIEW_BOUNDARIES.md), [бюджет входа ревью](REVIEW_INPUT_BUDGET.md).
- [Восстановление после сбоев](RECOVERY_POLICY.md), [сверка эффектов](EFFECT_RECONCILIATION.md).
- [Хранилище](STORAGE.md), [учёт расхода](ACCOUNTING_LEDGER.md), [тесты](TESTING.md).
- [Инварианты](ARCHITECTURE_INVARIANTS.md), [решения и их история](decisions/).

## Эксплуатация

- [Деплой, откат, бэкапы](../deploy/production/README.md): единственное описание
  того, что запущено в production.
- [Доступ applier](APPLIER_ACCESS.md), [приём секретов](SECRET_INGRESS.md).
- [Backlog](BACKLOG.md): перед исполнением пункта проверять, не закрыт ли он уже.
- [Карта сценариев](system-map/README.md): статический срез кода, не доказательство
  того, что включено в production.

Датированные отчёты, планы и передачи работы агентам удалены 9 ноября 2026;
они остаются в истории Git.
