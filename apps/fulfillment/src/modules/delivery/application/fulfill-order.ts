import { parseOrderEvent, type OrderEvent, type OrderConfirmed } from '@ticketing/contracts';
import type { TransactionPort } from '@ticketing/core';
import type { ConsumerName } from '../domain/delivery';
import { DeliveryRepository } from './ports';

/**
 * Выполняет одну часть выдачи заказа: билеты или уведомление.
 *
 * @remarks
 * Use case не подтверждает сообщение RabbitMQ: его результат — завершённая
 * транзакция PostgreSQL. Транспортный обработчик отправляет ACK только после
 * успешного возврата отсюда. Если процесс упадёт между коммитом и ACK, RabbitMQ
 * повторит доставку, а сохранённая запись о сообщении остановит повторные изменения.
 */
export class FulfillOrder {
  constructor(
    private readonly repository: DeliveryRepository,
    private readonly transactions: TransactionPort,
  ) {}

  /**
   * Атомарно связывает факт обработки сообщения с результатом для пользователя.
   *
   * Сначала `claim` проверяет пару обработчик/сообщение. Затем сохраняется описание
   * покупки и проверяется версия состояния. Для новой ревизии создаётся результат и убирается прежняя ошибка именно этой части заказа.
   * При исключении откатываются все эти изменения, включая запись `claim`: повторная
   * попытка сможет начать работу заново, а не примет незавершённую работу за готовую.
   *
   * @param consumer - Независимая обязанность, для которой выполняется дедупликация.
   * @param message - Событие жизненного цикла; старый формат нормализуется к первой ревизии.
   * @returns Завершение после коммита либо после распознавания уже выполненного повтора.
   * @throws Ошибка хранения или конфликт идентичности; решение о retry/DLQ принимает транспорт.
   */
  execute(consumer: ConsumerName, input: OrderConfirmed | OrderEvent): Promise<void> {
    const message = parseOrderEvent(input);
    return this.transactions.run(async () => {
      // Запись о сообщении не должна пережить откат самих билетов или уведомления.
      if (!(await this.repository.claim(consumer, message))) return;
      await this.repository.registerOrder(message);
      if (!(await this.repository.advance(consumer, message))) return;
      if (consumer === 'tickets') {
        if (message.state.status === 'confirmed') await this.repository.issueTickets(message);
        else await this.repository.invalidate(message.order.id);
      } else await this.repository.createNotification(message);
      await this.repository.clearFailure(consumer, message.order.id);
    });
  }

  /**
   * Делает исчерпанную обработку видимой в API как ошибку соответствующей части заказа.
   *
   * @remarks
   * Вызывается после подтверждённой публикации в DLQ и только для корректного события.
   * Повреждённое сообщение и конфликт существующего идентификатора сюда не попадают:
   * они не должны менять статус настоящего заказа. Повторная успешная обработка
   * через `execute` снимет эту ошибку в одной транзакции с созданием результата.
   *
   * @throws Ошибка БД — вызывающий worker сохранит исходную доставку неподтверждённой.
   */
  deadLettered(consumer: ConsumerName, input: OrderConfirmed | OrderEvent, reason: string): Promise<void> {
    const message = parseOrderEvent(input);
    return this.transactions.run(async () => {
      await this.repository.registerOrder(message);
      await this.repository.recordFailure(consumer, message, reason);
    });
  }
}
