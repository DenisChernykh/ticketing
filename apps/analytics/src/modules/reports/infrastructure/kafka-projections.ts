import { traced, measure, gauge } from '@ticketing/observability';
import { createHash } from 'node:crypto';
import { AppError, log } from '@ticketing/core';
import { parseOrderEvent } from '@ticketing/contracts';
import { Kafka, logLevel, type Admin, type Consumer } from 'kafkajs';
import type { ProjectionRuntime } from '../application/ports';
import { RecordSale } from '../application/reports.service';
import {
  InvalidSaleEvent,
  type Projection,
  type ReplaySelection,
  type WorkerStatus,
} from '../domain/reports';
import { SqlReportsRepository } from './sql-reports.repository';
import { RedisRanking } from './redis-ranking';

/**
 * Состояние одного управляемого consumer внутри процесса.
 * targets задаёт исключающие верхние offsets replay, reached — достигнутые
 * партиции. blocked отличает ошибочное событие от временного отказа зависимости.
 */
interface Slot extends WorkerStatus {
  consumer?: Consumer;
  blocked: boolean;
  targets?: Map<number, bigint>;
  reached: Set<number>;
}

interface KafkaSettings {
  brokers: string[];
  topic: string;
  prefix: string;
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/**
 * Нормализует JSON перед хешированием для проверки содержимого повтора.
 * Порядок полей объектов незначим, порядок элементов массива сохраняется.
 * Вход уже прошёл parseOrderEvent и содержит разрешённые поля события.
 */
function canonical(value: unknown): string {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`;
  return `{${Object.entries(value)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, item]) => `${JSON.stringify(key)}:${canonical(item)}`)
    .join(',')}}`;
}

/**
 * Владеет двумя независимыми Kafka-consumers и переключением их поколений.
 *
 * @remarks
 * Имя группы включает projection и generation. Поэтому events и daily получают
 * каждую продажу, а replay читает историю новой группой, не переиспользуя offsets
 * старой. Дедупликация SQL имеет ту же область — проекцию и поколение.
 *
 * Временные ошибки соединений восстанавливает supervisor. Нарушение контракта
 * сообщения помечает consumer как blocked: ошибка видна в /health/workers,
 * но повреждённая запись не пропускается ради продвижения отчёта.
 *
 * busy, transitions и replays координируют операции только в этом процессе.
 * Здесь нет распределённого владения replay: для одной БД Analytics должен
 * работать один такой runtime. HTTP-команда перестроения обращается именно
 * к владельцу consumers; отдельный CLI не запускает вторую копию обработчиков.
 */
export class KafkaProjections implements ProjectionRuntime {
  private readonly kafka: Kafka;
  private admin?: Admin;
  private brokerConnection?: Promise<Admin>;
  /** Текущий запуск/восстановление consumer, завершения которого ждёт replay. */
  private readonly transitions: Partial<Record<Projection, Promise<void>>> = {};
  private stopping = false;
  private loop?: Promise<void>;
  private readonly wake = new AbortController();
  /** Выбранные HTTP-командой проекции; supervisor не должен запускать их параллельно. */
  private readonly busy = new Set<Projection>();
  private readonly replays = new Set<Promise<{ projection: Projection; generation: string }>>();
  private readonly slots: Record<Projection, Slot> = {
    events: {
      projection: 'events',
      generation: null,
      state: 'starting',
      ready: false,
      error: null,
      blocked: false,
      reached: new Set(),
    },
    daily: {
      projection: 'daily',
      generation: null,
      state: 'starting',
      ready: false,
      error: null,
      blocked: false,
      reached: new Set(),
    },
  };

  constructor(
    private readonly repository: SqlReportsRepository,
    private readonly recordSale: RecordSale,
    private readonly ranking: RedisRanking,
    private readonly settings: KafkaSettings,
  ) {
    if (!settings.topic.startsWith(`${settings.prefix}.`))
      throw new Error('Analytics Kafka topic must belong to RESOURCE_PREFIX');
    this.kafka = new Kafka({
      clientId: `${settings.prefix}-analytics`,
      brokers: settings.brokers,
      logLevel: logLevel.ERROR,
      connectionTimeout: 2000,
      requestTimeout: 5000,
      retry: { retries: 2, initialRetryTime: 200, maxRetryTime: 1000 },
    });
  }

  /**
   * Готовит SQL-поколения и запускает восстановление consumers в фоне.
   * Доступная PostgreSQL обязательна; ожидание Kafka не удерживает запуск HTTP.
   */
  async start(): Promise<void> {
    await this.repository.initialize();
    this.loop = this.supervise();
  }

  /** Возвращает копии публичного состояния без consumer, locks и служебных offsets. */
  statuses(): WorkerStatus[] {
    return Object.values(this.slots).map(({ projection, generation, state, ready, error }) => ({
      projection,
      generation,
      state,
      ready,
      error,
    }));
  }

  /**
   * Запрещает новые операции, прерывает паузы и дожидается владельцев обработки.
   * Consumers и текущие replay завершаются до закрытия внешним lifecycle SQL/Redis,
   * чтобы незавершённая транзакция не потеряла соединение из-за порядка shutdown.
   */
  async stop(): Promise<void> {
    this.stopping = true;
    this.wake.abort();
    if (this.loop) await this.loop;
    await Promise.all(Object.values(this.slots).map((slot) => this.stopSlot(slot)));
    await Promise.allSettled([...this.replays]);
    if (this.admin) await this.admin.disconnect().catch(() => undefined);
  }

  /**
   * Принимает команду перестроения и резервирует выбранные проекции в процессе.
   *
   * @remarks
   * Все имена попадают в busy до первого await. Пересекающийся HTTP-запрос
   * получает 409, а supervisor пропускает эти проекции до завершения команды.
   * all запускает два самостоятельных replay и ждёт завершения обоих. Ошибка
   * одного возвращает 503, но не откатывает уже переключившуюся вторую проекцию.
   * Точный протокол одного перестроения реализован в replayOne.
   */
  async replay(selection: ReplaySelection): Promise<{ projection: Projection; generation: string }[]> {
    const projections: Projection[] = selection === 'all' ? ['events', 'daily'] : [selection];
    if (this.stopping) throw new AppError('ANALYTICS_STOPPING', 'Analytics останавливается', 503);
    if (projections.some((projection) => this.busy.has(projection))) {
      throw new AppError('REPLAY_IN_PROGRESS', 'Для выбранной проекции уже выполняется replay', 409);
    }
    // Резервируем весь выбор до первого await, пока другой HTTP-запрос не вмешался.
    for (const projection of projections) this.busy.add(projection);
    try {
      const tasks = projections.map((projection) => {
        const task = this.replayOne(projection);
        this.replays.add(task);
        void task.then(
          () => this.replays.delete(task),
          () => this.replays.delete(task),
        );
        return task;
      });
      const results = await Promise.allSettled(tasks);
      const failures = results.flatMap((result, index) =>
        result.status === 'rejected' ? [`${projections[index]}: ${messageOf(result.reason)}`] : [],
      );
      if (failures.length) throw new AppError('REPLAY_FAILED', failures.join('; '), 503);
      return results.flatMap((result) => (result.status === 'fulfilled' ? [result.value] : []));
    } finally {
      for (const projection of projections) this.busy.delete(projection);
    }
  }

  /**
   * Восстанавливает остановленные из-за временных ошибок consumers и рейтинг.
   *
   * Готовые, занятые replay и заблокированные ошибочным событием проекции
   * пропускаются. Promise каждого запуска записывается в transitions: если
   * HTTP-команда пришла в середине запуска, она дождётся его завершения перед
   * остановкой группы. Ошибка одной проекции не прекращает весь цикл.
   */
  private async supervise(): Promise<void> {
    while (!this.stopping) {
      for (const projection of ['events', 'daily'] as const) {
        const slot = this.slots[projection];
        if (this.stopping || this.busy.has(projection) || slot.ready || slot.blocked) continue;
        const transition = this.recoverSlot(slot);
        this.transitions[projection] = transition;
        try {
          await transition;
        } catch (error) {
          slot.state = 'error';
          slot.ready = false;
          slot.error = messageOf(error);
          log('analytics.worker.unavailable', { projection, error: slot.error });
        } finally {
          delete this.transitions[projection];
        }
      }
      if (!this.stopping) await this.ranking.refreshAll();
      await this.delay(2000);
    }
  }

  /**
   * Перезапускает consumer поколения, которое сейчас выбрано в PostgreSQL.
   * Проверки busy после await уступают управление пришедшему запросу replay;
   * уже начатый startSlot завершается под наблюдением promise в transitions.
   */
  private async recoverSlot(slot: Slot): Promise<void> {
    await this.repository.formatReady(slot.projection);
    await this.ensureBroker();
    if (this.stopping || this.busy.has(slot.projection)) return;
    await this.stopSlot(slot);
    if (this.stopping || this.busy.has(slot.projection)) return;
    await this.startSlot(slot, await this.repository.active(slot.projection));
  }

  /**
   * Разделяет один Admin и один выполняющийся connect между параллельными replay.
   * Неудачная попытка не остаётся в brokerConnection и допускает новый запуск.
   */
  private async ensureBroker(): Promise<Admin> {
    if (this.admin) return this.admin;
    if (this.brokerConnection) return this.brokerConnection;
    this.brokerConnection = this.connectBroker();
    try {
      return await this.brokerConnection;
    } finally {
      this.brokerConnection = undefined;
    }
  }

  /**
   * Подключается к брокеру и создаёт отсутствующий topic в namespace приложения.
   *
   * При создании задаются три партиции и хранение без ограничения времени/объёма
   * для полного replay локального стенда. У существующего topic проверяется число
   * партиций; этот метод не переписывает его retention-настройки. Потеря префикса
   * истории дополнительно выявляется по low offsets при запуске replay.
   */
  private async connectBroker(): Promise<Admin> {
    const admin = this.kafka.admin();
    try {
      await admin.connect();
      await admin.createTopics({
        waitForLeaders: true,
        topics: [
          {
            topic: this.settings.topic,
            numPartitions: 3,
            replicationFactor: 1,
            configEntries: [
              { name: 'cleanup.policy', value: 'delete' },
              { name: 'retention.ms', value: '-1' },
              { name: 'retention.bytes', value: '-1' },
            ],
          },
        ],
      });
      const metadata = await admin.fetchTopicMetadata({ topics: [this.settings.topic] });
      if (metadata.topics[0]?.partitions.length !== 3)
        throw new Error('Analytics expects exactly three topic partitions');
      this.admin = admin;
      return admin;
    } catch (error) {
      await admin.disconnect().catch(() => undefined);
      throw error;
    }
  }

  /**
   * Запускает consumer поколения и владеет порядком «SQL-коммит → Kafka offset».
   *
   * @remarks
   * Для новой группы fromBeginning читает начало истории; существующая группа
   * продолжает с подтверждённых offsets. Оба автоматических подтверждения
   * отключены. Каждое сообщение проходит проверку схемы и Kafka key=eventId,
   * получает fingerprint и передаётся в RecordSale. Только после SQL-коммита
   * resolveOffset отмечает запись обработанной, а commitOffsets сохраняет
   * позицию следующей записи. Если процесс упадёт между этими действиями,
   * повтор будет распознан SQL-receipt без увеличения сумм.
   *
   * InvalidSaleEvent блокирует проекцию до разбора причины: такой offset не
   * подтверждается. Временная ошибка БД или брокера оставляет восстановление
   * supervisor. Автоматический restart KafkaJS отключён, чтобы у consumer
   * не оказалось второго независимого владельца жизненного цикла.
   *
   * targets используется только при replay: это фиксированные исключающие
   * верхние offsets. Сообщения с offset >= target пока не обрабатываются;
   * достигшая границы партиция приостанавливается. Пустая партиция с target=0
   * считается достигнутой сразу: иначе replay ждал бы несуществующий eachBatch.
   */
  private async startSlot(slot: Slot, generation: string, targets?: Map<number, bigint>): Promise<void> {
    const consumer = this.kafka.consumer({
      groupId: `${this.settings.prefix}.analytics.${slot.projection}.${generation}`,
      sessionTimeout: 15000,
      heartbeatInterval: 3000,
      retry: { retries: 2, initialRetryTime: 200, maxRetryTime: 1000, restartOnFailure: async () => false },
    });
    slot.consumer = consumer;
    slot.generation = generation;
    slot.state = targets ? 'replaying' : 'starting';
    slot.ready = false;
    slot.error = null;
    slot.blocked = false;
    slot.targets = targets;
    // Для пустой истории партиции не придёт batch, которым можно отметить завершение.
    slot.reached = new Set(
      targets ? [...targets].filter(([, offset]) => offset === 0n).map(([partition]) => partition) : [],
    );
    consumer.on(consumer.events.CRASH, ({ payload }) => {
      if (slot.consumer !== consumer || this.stopping) return;
      slot.state = 'error';
      slot.ready = false;
      slot.error = messageOf(payload.error);
      log('analytics.worker.crashed', {
        projection: slot.projection,
        generation,
        error: slot.error,
        blocked: slot.blocked,
      });
    });
    consumer.on(consumer.events.DISCONNECT, () => {
      if (slot.consumer !== consumer || this.stopping) return;
      slot.ready = false;
      if (slot.state !== 'error' && slot.state !== 'stopped') {
        slot.state = 'error';
        slot.error = 'Kafka consumer disconnected';
      }
    });
    await consumer.connect();
    await consumer.subscribe({ topic: this.settings.topic, fromBeginning: true });
    if (this.stopping) {
      await this.stopSlot(slot);
      return;
    }
    await consumer.run({
      autoCommit: false,
      eachBatchAutoResolve: false,
      partitionsConsumedConcurrently: 1,
      eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
        for (const message of batch.messages) {
          if (!isRunning() || isStale() || this.stopping) break;
          const target = slot.targets?.get(batch.partition);
          if (target !== undefined && BigInt(message.offset) >= target) {
            this.pauseReached(slot, batch.partition);
            break;
          }
          try {
            let event;
            try {
              if (!message.value) throw new Error('An order event cannot be a tombstone');
              event = parseOrderEvent(JSON.parse(message.value.toString('utf8')));
              if (message.key?.toString('utf8') !== event.order.eventId)
                throw new Error('Kafka key must equal order.eventId');
            } catch (error) {
              throw new InvalidSaleEvent(messageOf(error));
            }
            const fingerprint = createHash('sha256').update(canonical(event)).digest('hex');
            const incoming = Object.fromEntries(
              Object.entries(message.headers ?? {}).map(([k, v]) => [
                k,
                Array.isArray(v) ? String(v[0]) : (v?.toString() ?? ''),
              ]),
            );
            await traced(
              'analytics.' + slot.projection,
              { orderId: event.order.id, messageId: event.messageId, projection: slot.projection },
              () => this.recordSale.execute(slot.projection, generation, event, fingerprint),
              incoming,
              !!slot.targets,
            );
            measure('event_processing_lag_ms', Math.max(0, Date.now() - Date.parse(event.occurredAt)), {
              consumer: slot.projection,
            });
            // COMMIT уже завершён; receipt делает повтор после сбоя безопасным.
            resolveOffset(message.offset);
            const next = (BigInt(message.offset) + 1n).toString();
            await consumer.commitOffsets([{ topic: batch.topic, partition: batch.partition, offset: next }]);
            await heartbeat();
            if (target !== undefined && BigInt(next) >= target) {
              this.pauseReached(slot, batch.partition);
              break;
            }
          } catch (error) {
            slot.blocked = error instanceof InvalidSaleEvent;
            slot.ready = false;
            slot.state = 'error';
            slot.error = messageOf(error);
            log('analytics.message.failed', {
              projection: slot.projection,
              generation,
              partition: batch.partition,
              offset: message.offset,
              blocked: slot.blocked,
              error: slot.error,
            });
            // Ошибка валидации или SQL-транзакции возникает до подтверждения записи.
            throw error;
          }
        }
      },
    });
    if (!slot.error) {
      slot.state = targets ? 'replaying' : 'running';
      slot.ready = !targets;
      log('analytics.worker.started', { projection: slot.projection, generation, replay: Boolean(targets) });
    }
  }

  /**
   * Приостанавливает партицию до переключения active generation.
   * Отмечает достижение только после pause, чтобы replay не возобновил чтение
   * раньше, чем текущий обработчик выставил остановку на зафиксированной границе.
   */
  private pauseReached(slot: Slot, partition: number): void {
    slot.consumer?.pause([{ topic: this.settings.topic, partitions: [partition] }]);
    slot.reached.add(partition);
  }

  /**
   * Останавливает обработку и отсоединяет consumer из внешнего управляющего кода.
   * Ссылка очищается до disconnect, чтобы его событие не испортило состояние
   * новой группы. Вызывать stop из собственного eachBatch нельзя: он ожидает
   * окончания того самого обработчика, который в таком случае ждал бы stop.
   */
  private async stopSlot(slot: Slot): Promise<void> {
    const consumer = slot.consumer;
    // Событие намеренного disconnect не должно менять состояние следующего consumer.
    slot.consumer = undefined;
    slot.ready = false;
    slot.state = 'stopped';
    if (consumer) {
      try {
        await consumer.stop();
      } finally {
        await consumer.disconnect().catch(() => undefined);
      }
    }
  }

  /**
   * Перестраивает одну проекцию, сохраняя доступность прежнего SQL-результата.
   *
   * @remarks
   * Сначала завершается уже начатое восстановление из transitions и останавливается
   * старая группа. Её данные остаются активными и доступными для чтения, но до
   * переключения не обновляются. Продажи и публикация новых событий продолжаются.
   *
   * Затем для каждой партиции один раз фиксируется high offset — позиция после
   * последней записи на момент снимка. Low offset должен оставаться нулевым:
   * если начало удалено, полный пересчёт нельзя выдавать за восстановленную историю.
   * Новое building-поколение получает новую consumer group и собственные receipts.
   * startSlot читает каждую партицию до её target, учитывая пустые партиции,
   * и приостанавливает достигнутые. Ожидание этой стадии ограничено 120 секундами.
   *
   * После достижения всех границ activate атомарно меняет SQL-указатель. targets
   * убирается, та же новая группа возобновляется и дочитывает продажи, пришедшие
   * после снимка offsets. Старые агрегаты не удаляются; другая проекция не затронута.
   *
   * Ошибка до успешного переключения оставляет прежний active. Незавершённое
   * поколение помечается failed, а supervisor после освобождения busy восстановит
   * поколение из БД. При потере ответа на COMMIT нельзя угадывать исход по ошибке
   * клиента: сохранённый active pointer определяет, какое поколение продолжать.
   */
  private async replayOne(projection: Projection): Promise<{ projection: Projection; generation: string }> {
    const slot = this.slots[projection];
    let generation: string | undefined;
    try {
      await this.transitions[projection]?.catch(() => undefined);
      // Управляющий HTTP-процесс вызывает stop вне eachBatch.
      await this.stopSlot(slot);
      const admin = await this.ensureBroker();
      if (this.stopping) throw new Error('Analytics stopped before replay');
      const offsets = await admin.fetchTopicOffsets(this.settings.topic);
      if (offsets.some((partition) => BigInt(partition.low) !== 0n)) {
        throw new Error('Full replay is unavailable: Kafka has already removed part of the history');
      }
      const targets = new Map(offsets.map((partition) => [partition.partition, BigInt(partition.high)]));
      generation = await this.repository.createGeneration(projection);
      await this.startSlot(slot, generation, targets);
      const deadline = Date.now() + 120_000;
      while (slot.reached.size < targets.size) {
        if (this.stopping) throw new Error('Analytics stopped during replay');
        if (slot.state === 'error') throw new Error(slot.error ?? 'Replay consumer failed');
        if (Date.now() >= deadline) throw new Error('Replay exceeded its two-minute deadline');
        await this.delay(50);
      }
      if (slot.state === 'error' || this.stopping) throw new Error(slot.error ?? 'Replay interrupted');
      // Один SQL-запрос читателя видит целое старое либо целое новое поколение.
      await this.repository.activate(projection, generation);
      slot.targets = undefined;
      slot.consumer!.resume([{ topic: this.settings.topic, partitions: [...targets.keys()] }]);
      slot.state = 'running';
      slot.ready = true;
      slot.error = null;
      if (projection === 'events') await this.ranking.refreshAll();
      log('analytics.replay.activated', {
        projection,
        generation,
        offsets: Object.fromEntries([...targets].map(([key, value]) => [key, value.toString()])),
      });
      return { projection, generation };
    } catch (error) {
      await this.stopSlot(slot).catch(() => undefined);
      if (generation)
        await this.repository.failGeneration(projection, generation, messageOf(error)).catch(() => undefined);
      slot.state = 'error';
      slot.error = messageOf(error);
      slot.blocked = false;
      log('analytics.replay.failed', { projection, generation, error: slot.error });
      // Сохранённый указатель определяет исход даже при потере ответа на COMMIT.
      // Supervisor продолжит это поколение после освобождения replay-владения.
      throw error;
    }
  }

  /** Пауза supervisor/replay, которая немедленно завершается при shutdown. */
  private async delay(ms: number): Promise<void> {
    if (this.wake.signal.aborted) return;
    await new Promise<void>((resolve) => {
      const finish = () => {
        clearTimeout(timer);
        this.wake.signal.removeEventListener('abort', finish);
        resolve();
      };
      const timer = setTimeout(finish, ms);
      this.wake.signal.addEventListener('abort', finish, { once: true });
    });
  }
}
