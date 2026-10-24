# 13. Поиск задержки покупки между сервисами

[Все задачи](../../README.md#tasks)

**Задача.** Заказ оплачен, а билеты ещё не появились. Нужно понять, на каком этапе задержалась обработка: платёж, outbox или получатель события.

**Решение.** Передавать контекст трассировки через HTTP, фоновые задания и оба брокера. Сохранять его с устойчивой работой, чтобы связать этапы после перезапуска процесса. Метрики показывают задержки, незавершённые операции, повторы и очереди ошибок.

**Код решения:**

- [beginHttp()](../../packages/observability/src/index.ts#L95) — создаёт span входящего HTTP-запроса.
- [carrier() и traced()](../../packages/observability/src/index.ts#L36) — сохраняют контекст и связывают выполнение фоновой операции с trace.
- [PgEventWriter.append()](../../apps/sales/src/modules/messaging/infrastructure/pg-event-writer.ts#L7) — записывает trace-контекст рядом с событием outbox.
- [PgWorkMetrics.refresh()](../../apps/sales/src/modules/messaging/infrastructure/pg-work-metrics.ts#L6) — собирает показатели незавершённой работы Sales.

**Проверки:**

- [Трассировка и метрики](../../test/integration/observability.test.cjs#L6) — проверяются spans через RabbitMQ и Kafka, метрики Prometheus и работа приложения при недоступном Collector.

Адреса Jaeger, Grafana и Prometheus, а также поиск по `X-Trace-Id` описаны в [README](../../README.md#диагностика).

[Подробнее об устройстве решения](../../GUIDE.md#observability).
