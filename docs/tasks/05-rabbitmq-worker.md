# 5. Падение обработчика RabbitMQ

[Все задачи](../../README.md#tasks)

**Задача.** Worker получил сообщение, но упал во время выпуска билетов. Задание должно сохраниться, а повтор после успешной обработки не должен выпустить лишние билеты.

**Решение.** Подтверждать сообщение через ACK только после коммита результата в PostgreSQL. Неподтверждённую доставку RabbitMQ выдаст повторно. Запись об обработанном сообщении сохраняется в той же транзакции, что и билеты или уведомление.

**Код решения:**

- [RabbitWorker.handle()](../../apps/fulfillment/src/modules/delivery/infrastructure/rabbit-workers.ts#L201) — дожидается завершения обработки и только затем отправляет ACK.
- [FulfillOrder.execute()](../../apps/fulfillment/src/modules/delivery/application/fulfill-order.ts#L34) — объединяет квитанцию обработки и результат в одной транзакции.
- [PostgresDeliveryRepository.claim()](../../apps/fulfillment/src/modules/delivery/infrastructure/postgres-delivery-repository.ts#L71) — распознаёт повтор по обработчику и идентификатору сообщения.
- [declareTopology()](../../apps/fulfillment/src/modules/delivery/infrastructure/rabbit-topology.ts#L40) — создаёт устойчивые очереди билетов, уведомлений, повторов и ошибок.

**Проверки:**

- [Повтор сообщения](../../apps/fulfillment/src/modules/delivery/composition/verify.ts#L97) — не удваивает билеты; у двух независимых обработчиков свои квитанции.
- [Ошибка внутри обработки](../../apps/fulfillment/src/modules/delivery/composition/verify.ts#L147) — откатывает билеты и квитанцию, затем retry завершает работу.

Эти проверки моделируют повтор сообщения и ошибку транзакции. Отдельной принудительной остановки процесса между коммитом и ACK в текущем наборе нет.

[Подробнее об устройстве решения](../../GUIDE.md#rabbitmq).
