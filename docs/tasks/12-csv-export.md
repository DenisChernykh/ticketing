# 12. Большой отчёт без загрузки целиком в память

[Все задачи](../../README.md#tasks)

**Задача.** Организатор скачивает большой CSV. Сервер должен передавать его постепенно и прекращать чтение БД, если клиент отключился.

**Решение.** Читать согласованный снимок PostgreSQL через SQL-курсор порциями по 500 строк. Передавать данные потоком через Analytics и Sales, учитывая скорость клиента. Отключение клиента отменяет чтение и освобождает соединение.

**Код решения:**

- [SqlOrderExports.rows()](../../apps/analytics/src/modules/reports/infrastructure/sql-order-exports.ts#L5) — задаёт SQL выгрузки с фильтром организатора и периода.
- [SqlRowStream.rows()](../../packages/core/src/streaming.ts#L21) — держит согласованный снимок, читает порциями и закрывает курсор.
- [HttpProductQueries.csv()](../../apps/sales/src/modules/integrations/infrastructure/http-product-queries.ts#L21) — передаёт поток Analytics через Sales с отменой запроса.
- [streamCsv()](../../packages/core/src/streaming.ts#L106) — связывает поток с HTTP-ответом, таймаутами и отключением клиента.

**Проверки:**

- [Потоковая выгрузка](../../test/integration/lifecycle.test.cjs#L371) — 100 000 строк, replay во время скачивания, права организатора, экранирование ячеек и закрытие чтения после разрыва соединения.

[Подробнее об устройстве решения](../../GUIDE.md#csv-export).
