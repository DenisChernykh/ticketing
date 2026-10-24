import type { OrderEvent } from '@ticketing/contracts';
import type { ConsumerName, OrderDelivery } from '../domain/delivery';

/**
 * Хранилище результатов выпуска: приложение задаёт операции, а адаптер выбирает SQL.
 *
 * @remarks
 * Методы изменения данных сами не открывают транзакцию. `FulfillOrder` объединяет
 * их через `TransactionPort`, поэтому запись о сообщении, билеты и снятие ошибки
 * используют одно соединение и одну границу коммита. Это обязательство адаптера,
 * хотя его технический `PoolClient` здесь не виден.
 *
 * Репозиторий работает только с БД Fulfillment. Данные о продаже приходят в событии:
 * проверка существования заказа в чужой БД не входит в этот контракт.
 */
export abstract class DeliveryRepository {
  /**
   * Занимает идентификатор сообщения для одного обработчика в текущей транзакции.
   * @returns `true` для новой обработки, `false` для уже выполненного точного повтора.
   * @throws `MessageConflict` — идентификатор уже связан с другим содержимым.
   */
  abstract claim(consumer: ConsumerName, message: OrderEvent): Promise<boolean>;
  /** Сохраняет локальное описание продажи либо проверяет совпадение существующего. */
  abstract registerOrder(message: OrderEvent): Promise<void>;
  /** Создаёт отдельный билет на каждую купленную единицу. */
  abstract advance(consumer: ConsumerName, message: OrderEvent): Promise<boolean>;
  abstract invalidate(orderId: string): Promise<void>;
  abstract issueTickets(message: OrderEvent): Promise<void>;
  /** Сохраняет одно уведомление о заказе для чтения внутри приложения. */
  abstract createNotification(message: OrderEvent): Promise<void>;
  /** Убирает прошлый сбой только того обработчика, который сейчас успешно завершился. */
  abstract clearFailure(consumer: ConsumerName, orderId: string): Promise<void>;
  /** Фиксирует подтверждённую отправку в DLQ; вызывать до её публикации нельзя. */
  abstract recordFailure(consumer: ConsumerName, message: OrderEvent, reason: string): Promise<void>;
  /** Читает результаты в контексте покупателя, переданном доверенным сервисом Sales. */
  abstract readOrder(orderId: string, buyerId: string): Promise<OrderDelivery>;
  abstract ping(): Promise<void>;
}

/**
 * Наблюдаемое состояние подписки на очередь, отдельно от готовности HTTP и PostgreSQL.
 * `inFlight` — число доставок, которые этот процесс ещё обрабатывает; это не длина очереди.
 */
export interface WorkerState {
  status: 'connecting' | 'running' | 'disconnected' | 'stopped';
  inFlight: number;
}
/** Позволяет API показать состояние фоновой обработки без зависимости от AMQP-адаптера. */
export abstract class WorkerHealth {
  abstract states(): Record<ConsumerName, WorkerState>;
}
