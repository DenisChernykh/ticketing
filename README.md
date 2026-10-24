# Ticketing — система продажи билетов

Система продажи билетов с каталогом, резервированием мест, оформлением заказов, выпуском билетов и отчётностью.

## Возможности

- Отдельные сервисы продаж, выпуска билетов, аналитики и обработки платежного сценария.
- PostgreSQL, ручной SQL, Redis, RabbitMQ и Kafka.
- Бронирование, отмена, возврат, повторная доставка и перестроение отчётов.

## Инженерные акценты

- Проектирование согласованных процессов между сервисами.
- Конкурентный доступ к местам, транзакции и идемпотентность.
- Outbox, обработка событий и восстановление проекций.

## Стек

TypeScript.

## Навигация по коду

- [test/](test/) — автоматизированные проверки.
- [docs/](docs/) — архитектура и документация.

## Разработка

Инструменты: Node.js 24.18.0, pnpm@11.15.1.

```sh
pnpm install
pnpm dev
pnpm check
pnpm test
pnpm build
```

Параметры окружения перечислены в [.env.example](.env.example). Локальные значения задаются вне Git.

## Сценарии реализации

<a id="tasks"></a>

- [01-last-seat](docs/tasks/01-last-seat.md)
- [02-idempotency](docs/tasks/02-idempotency.md)
- [03-outbox](docs/tasks/03-outbox.md)
- [04-rate-limit](docs/tasks/04-rate-limit.md)
- [05-rabbitmq-worker](docs/tasks/05-rabbitmq-worker.md)
- [06-kafka-replay](docs/tasks/06-kafka-replay.md)
- [07-pagination](docs/tasks/07-pagination.md)
- [08-edit-conflicts](docs/tasks/08-edit-conflicts.md)
- [09-reservation-expiry](docs/tasks/09-reservation-expiry.md)
- [10-payments](docs/tasks/10-payments.md)
- [11-cancellation](docs/tasks/11-cancellation.md)
- [12-csv-export](docs/tasks/12-csv-export.md)
- [13-observability](docs/tasks/13-observability.md)
