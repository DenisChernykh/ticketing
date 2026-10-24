# 10. Оплата при потерянном ответе или webhook

[Все задачи](../../README.md#tasks)

**Задача.** Провайдер списал деньги, но HTTP-ответ потерялся, webhook не пришёл или пришёл дважды. Нужно узнать результат без повторного списания.

**Решение.** Сначала сохранить платёжное задание в БД, затем выполнить HTTP-запрос в фоне. Повторы используют тот же идентификатор платежа. Подписанный webhook сохраняется с защитой от дубликатов, а независимая сверка запрашивает актуальное состояние провайдера.

**Код решения:**

- [CheckoutService.pay()](../../apps/sales/src/modules/checkout/application/checkout.service.ts#L71) — проверяет бронь и сохраняет операцию оплаты.
- [PaymentWorker.operation()](../../apps/sales/src/modules/payments/application/payment-worker.ts#L22) — выполняет устойчивое задание оплаты или возврата.
- [PaymentWorker.sync()](../../apps/sales/src/modules/payments/application/payment-worker.ts#L39) — сверяет локальный платёж с провайдером.
- [WebhooksController.receive()](../../apps/sales/src/modules/payments/presentation/webhooks.controller.ts#L19) — проверяет подпись исходного тела и время webhook.
- [PgPayments.receive()](../../apps/sales/src/modules/payments/infrastructure/pg-payments.ts#L97) — проверяет идентичность платежа, сохраняет квитанцию webhook и планирует сверку.

**Проверки:**

- [Потерянный ответ и webhook](../../test/integration/lifecycle.test.cjs#L132) — сценарии `lost_response`, `no_webhook` и `duplicate_webhook` завершаются без второго списания.
- [Подпись и повторы webhook](../../test/integration/lifecycle.test.cjs#L246) — проверяются исходные байты, время, идентификаторы и совпадение содержимого.

[Подробнее об устройстве решения](../../GUIDE.md#payments).
