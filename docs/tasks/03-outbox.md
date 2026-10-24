# 3. Доставка события после сохранения заказа

[Все задачи](../../README.md#tasks)

**Задача.** Покупка подтверждена, но процесс упал до отправки события или брокер недоступен. Выпуск билетов и отчёты должны догнать сохранённый заказ.

**Решение.** Записать изменение заказа и событие outbox одной транзакцией. Отдельный relay доставляет событие в RabbitMQ и Kafka, повторяя неудачные попытки. Каждое направление учитывается отдельно, а постоянный `messageId` позволяет получателям распознать дубликат.

В текущем приложении событие `order.confirmed` появляется при подтверждении оплаты. Само создание брони ещё не запускает выпуск билетов.

**Код решения:**

- [CheckoutService.settle()](../../apps/sales/src/modules/checkout/application/checkout.service.ts#L135) — сохраняет подтверждение заказа и вызывает запись события внутри транзакции.
- [PgEventWriter.append()](../../apps/sales/src/modules/messaging/infrastructure/pg-event-writer.ts#L7) — записывает событие и две доставки в PostgreSQL.
- [RelayService.deliverOne()](../../apps/sales/src/modules/messaging/application/relay.service.ts#L31) — публикует событие и отмечает успех только после подтверждения брокера.
- [PgOutbox.claim()](../../apps/sales/src/modules/messaging/infrastructure/pg-outbox.ts#L30) — выдаёт аренду доставки через `SKIP LOCKED`; просроченную аренду может забрать другой relay.
- [PgOutbox.retry()](../../apps/sales/src/modules/messaging/infrastructure/pg-outbox.ts#L80) — откладывает повтор с тем же идентификатором сообщения.

**Проверки:**

- [Восстановление доставки](../../test/integration/product.test.cjs#L203) — Kafka работает при недоступном RabbitMQ; после остановки и перезапуска relay билеты появляются.
- [Повторная публикация](../../test/integration/product.test.cjs#L307) — не создаёт дополнительных билетов и не увеличивает отчёты второй раз.

[Подробнее об устройстве решения](../../GUIDE.md#checkout).
