import { traced, count, measure } from '@ticketing/observability';
import { connect, type Channel, type ChannelModel, type ConsumeMessage } from 'amqplib';
import { log, sleep } from '@ticketing/core';
import { parseOrderEvent, type OrderEvent } from '@ticketing/contracts';
import { FulfillOrder } from '../application/fulfill-order';
import { WorkerHealth, type WorkerState } from '../application/ports';
import { type ConsumerName, MessageConflict } from '../domain/delivery';
import { closeRabbitConnection } from './rabbit-close';
import { ConfirmedPublisher, consumerQueue, declareTopology } from './rabbit-topology';

const MAX_RETRIES = 3;

/**
 * Управляет одной независимой подпиской: соединением, доставками и восстановлением.
 *
 * @remarks
 * Канал получения работает с ручным ACK; отдельный confirm-канал нужен для надёжной
 * перепубликации в retry/DLQ. `prefetch` ограничивает число выданных этому получателю
 * неподтверждённых сообщений и тем самым нагрузку на БД. Он не делает обработку
 * последовательной и не гарантирует исходный порядок после повторных доставок.
 */
class RabbitWorker {
  readonly state: WorkerState = { status: 'connecting', inFlight: 0 };
  private readonly abort = new AbortController();
  private connection?: ChannelModel;
  private channel?: Channel;
  private consumerTag?: string;
  private loop?: Promise<void>;
  private pending = new Set<Promise<void>>();

  constructor(
    private readonly consumer: ConsumerName,
    private readonly url: string,
    private readonly prefix: string,
    private readonly fulfillOrder: FulfillOrder,
    private readonly prefetch: number,
  ) {}

  start(): void {
    this.loop ??= this.run();
  }

  /**
   * Останавливает новые доставки, затем дожидается уже начатых транзакций.
   *
   * `CancelOk` ждём не более секунды: при недоступном брокере сам запрос отмены тоже
   * может зависнуть. Тогда соединение закрывается, а оставшиеся сообщения брокер
   * сможет выдать заново. При обычной отмене канал остаётся открыт до завершения
   * текущих обработчиков, чтобы они успели отправить ACK после коммита.
   *
   * @remarks
   * Срок ограничивает AMQP-отмену и закрытие, а не всё завершение сервиса: ожидание
   * уже выполняющихся операций PostgreSQL имеет собственные границы в адаптере БД.
   */
  async stop(): Promise<void> {
    this.abort.abort();
    if (this.channel && this.consumerTag) {
      let timer: NodeJS.Timeout | undefined;
      let cancelled = false;
      try {
        await Promise.race([
          this.channel.cancel(this.consumerTag).then(
            () => {
              cancelled = true;
            },
            () => undefined,
          ),
          new Promise<void>((resolve) => {
            timer = setTimeout(resolve, 1000);
          }),
        ]);
      } finally {
        if (timer) clearTimeout(timer);
      }
      // Разрыв соединения прекращает доставки, даже когда подтверждение отмены не приходит.
      if (!cancelled) await closeRabbitConnection(this.connection);
    }
    // При штатной отмене сохраняем канал для ACK уже начатых транзакций.
    await Promise.allSettled([...this.pending]);
    await closeRabbitConnection(this.connection);
    await this.loop;
    this.state.status = 'stopped';
  }

  /**
   * Восстанавливает подписку после потери соединения или любого из её каналов.
   *
   * Каждое подключение заново объявляет согласованную топологию и включает prefetch.
   * Обработчик доставки захватывает конкретный канал этого подключения: ACK от старой
   * доставки нельзя отправить на новый канал с другой последовательностью delivery tags.
   * Если перепубликация не подтвердилась, оригинал возвращается через NACK либо через
   * закрытие канала; попытки подключения продолжаются с задержкой до пяти секунд.
   *
   * `pending` учитывает работу, которая уже началась, для наблюдения и завершения
   * процесса. Дедупликация в БД остаётся необходимой при пересечении старой обработки
   * и повторной доставки после восстановления соединения.
   */
  private async run(): Promise<void> {
    let failures = 0;
    while (!this.abort.signal.aborted) {
      this.state.status = 'connecting';
      let connection: ChannelModel | undefined;
      try {
        connection = await connect(this.url, {
          timeout: 5000,
          clientProperties: { connection_name: `fulfillment-${this.consumer}` },
        });
        this.connection = connection;
        connection.on('error', () => undefined); // Восстановлением управляет close; error не должен стать необработанным событием.
        const closed = new Promise<void>((resolve) => connection!.once('close', resolve));
        if (this.abort.signal.aborted) {
          await closeRabbitConnection(connection);
          break;
        }
        const channel = await connection.createChannel();
        const confirms = await connection.createConfirmChannel();
        this.channel = channel;
        for (const item of [channel, confirms]) {
          item.on('error', () => undefined);
          item.once('close', () => {
            void closeRabbitConnection(connection);
          });
        }
        await declareTopology(channel, this.prefix);
        await channel.prefetch(this.prefetch);
        const publisher = new ConfirmedPublisher(confirms);
        const result = await channel.consume(
          consumerQueue(this.prefix, this.consumer),
          (message) => {
            if (!message) {
              void closeRabbitConnection(connection);
              return;
            }
            const task = this.handle(channel, publisher, message).catch((error) => {
              log('fulfillment.delivery_transport_error', {
                consumer: this.consumer,
                error: error instanceof Error ? error.message : String(error),
              });
              // NACK относится к исходному каналу; при его потере брокер сам вернёт unacked-доставки.
              try {
                channel.nack(message, false, true);
              } catch {
                /* Канал закрыт, повторную доставку выполнит брокер. */
              }
              void closeRabbitConnection(connection);
            });
            this.pending.add(task);
            this.state.inFlight = this.pending.size;
            void task.finally(() => {
              this.pending.delete(task);
              this.state.inFlight = this.pending.size;
            });
          },
          { noAck: false },
        );
        this.consumerTag = result.consumerTag;
        this.state.status = 'running';
        failures = 0;
        log('fulfillment.worker_started', { consumer: this.consumer });
        await closed;
      } catch (error) {
        log('fulfillment.worker_disconnected', {
          consumer: this.consumer,
          error: error instanceof Error ? error.message : String(error),
        });
      } finally {
        await closeRabbitConnection(connection);
        this.connection = undefined;
        this.channel = undefined;
        this.consumerTag = undefined;
        this.state.status = 'disconnected';
      }
      failures += 1;
      if (!this.abort.signal.aborted)
        await sleep(Math.min(5000, 250 * 2 ** Math.min(failures, 5)), this.abort.signal).catch(
          () => undefined,
        );
    }
  }

  /**
   * Связывает проверку события, коммит PostgreSQL и ручное подтверждение доставки.
   *
   * Сначала проверяются JSON и версия контракта. Успешный `execute` возвращается после
   * коммита receipt и результата, и только затем отправляется ACK. Если ACK потерян,
   * повтор безопасен за счёт сохранённого receipt; общей транзакции БД и RabbitMQ здесь нет.
   *
   * Временная ошибка получает максимум три отложенных повтора после первой попытки.
   * Повреждённое событие и `MessageConflict` сразу направляются в DLQ. Некорректный
   * счётчик повторов трактуется как исчерпанный, чтобы не образовать бесконечный цикл.
   * Во всех случаях сначала подтверждается новая публикация, и лишь затем ACK-ается
   * оригинал. Ошибка этого переноса поднимается наружу: `run` сохранит возможность
   * повторной доставки исходного сообщения.
   *
   * @remarks
   * Запись `failed` создаётся после принятия DLQ только для проверенного события.
   * Если в этот момент БД недоступна, оригинал останется неподтверждённым и в DLQ
   * возможны дополнительные копии. Повреждённые сообщения и конфликты не должны
   * отмечать ошибкой заказ, сведения о котором нельзя безопасно принять.
   */
  private async handle(
    channel: Channel,
    publisher: ConfirmedPublisher,
    delivery: ConsumeMessage,
  ): Promise<void> {
    let message: OrderEvent | undefined;
    let failure: unknown;
    let malformed = false;
    try {
      message = parseOrderEvent(JSON.parse(delivery.content.toString('utf8')));
    } catch (error) {
      malformed = true;
      failure = error;
    }
    if (message) {
      try {
        await traced(
          'fulfillment.' + this.consumer,
          { orderId: message.order.id, messageId: message.messageId },
          () => this.fulfillOrder.execute(this.consumer, message!),
          delivery.properties.headers as Record<string, string>,
        );
        measure('event_processing_lag_ms', Math.max(0, Date.now() - Date.parse(message.occurredAt)), {
          consumer: this.consumer,
        });
        channel.ack(delivery);
        return;
      } catch (error) {
        failure = error;
      }
    }
    const retryValue: unknown = delivery.properties.headers?.['x-retry-count'] ?? 0;
    const attempt =
      typeof retryValue === 'number' && Number.isSafeInteger(retryValue) && retryValue >= 0
        ? retryValue
        : MAX_RETRIES;
    const terminal = malformed || failure instanceof MessageConflict || attempt >= MAX_RETRIES;
    const reason = failure instanceof Error ? failure.message : String(failure);
    const queue = consumerQueue(this.prefix, this.consumer);
    await publisher.send(`${queue}.${terminal ? 'dead' : 'retry'}`, delivery.content, {
      contentType: 'application/json',
      messageId: message?.messageId ?? delivery.properties.messageId,
      headers: {
        ...delivery.properties.headers,
        'x-retry-count': terminal ? attempt : attempt + 1,
        'x-last-error': reason.slice(0, 500),
      },
    });
    // Невалидная оболочка или конфликт идентификатора не меняют статус правильного заказа.
    if (terminal && message && !(failure instanceof MessageConflict)) {
      await this.fulfillOrder.deadLettered(this.consumer, message, reason);
    }
    channel.ack(delivery);
    count(terminal ? 'dead_letters' : 'message_retries', { consumer: this.consumer });
    log(terminal ? 'fulfillment.dead_lettered' : 'fulfillment.retry_scheduled', {
      consumer: this.consumer,
      messageId: message?.messageId,
      attempt,
      reason: reason.slice(0, 500),
    });
  }
}

/**
 * Собирает два независимых worker: выпуск билетов и сохранение уведомлений.
 * У каждого своё соединение, prefetch и цикл восстановления. Сбой одной подписки
 * не останавливает другую, а общий API показывает оба состояния раздельно.
 */
export class RabbitWorkers extends WorkerHealth {
  private readonly workers: Record<ConsumerName, RabbitWorker>;
  constructor(url: string, prefix: string, fulfillOrder: FulfillOrder) {
    super();
    const prefetch = Number(process.env.RABBIT_PREFETCH ?? 8);
    if (!Number.isInteger(prefetch) || prefetch < 1 || prefetch > 100)
      throw new Error('RABBIT_PREFETCH must be between 1 and 100');
    this.workers = {
      tickets: new RabbitWorker('tickets', url, prefix, fulfillOrder, prefetch),
      notifications: new RabbitWorker('notifications', url, prefix, fulfillOrder, prefetch),
    };
  }
  start(): void {
    this.workers.tickets.start();
    this.workers.notifications.start();
  }
  async stop(): Promise<void> {
    await Promise.all([this.workers.tickets.stop(), this.workers.notifications.stop()]);
  }
  states(): Record<ConsumerName, WorkerState> {
    return {
      tickets: { ...this.workers.tickets.state },
      notifications: { ...this.workers.notifications.state },
    };
  }
}
