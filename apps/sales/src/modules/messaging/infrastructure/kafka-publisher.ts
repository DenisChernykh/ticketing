import { carrier, traced, count } from '@ticketing/observability';
import { Kafka, logLevel, type Producer } from 'kafkajs';
import type { OrderEvent } from '@ticketing/contracts';
import type { EventPublisher } from '../application/ports';

/**
 * Публикует поток подтверждённых покупок для независимых групп Analytics.
 *
 * @remarks
 * Ключ eventId направляет события одного мероприятия в одну партицию. Kafka
 * сохраняет порядок записей внутри неё; при этом порядок захвата outbox несколькими
 * relay не обязан совпадать с порядком коммитов покупок. Получатели сравнивают версии полных снимков,
 * поэтому отчёты не опираются на порядок захвата outbox. Между разными партициями общего
 * порядка нет. Идемпотентный producer помогает при сетевых повторах клиента,
 * но не заменяет дедупликацию messageId после нового запуска relay.
 */
export class KafkaPublisher implements EventPublisher {
  private producer?: Producer;
  private readonly kafka: Kafka;
  constructor(
    brokers: string[],
    private readonly topic: string,
    prefix: string,
  ) {
    this.kafka = new Kafka({
      brokers,
      clientId: `${prefix}-sales-relay`,
      logLevel: logLevel.NOTHING,
      connectionTimeout: 3000,
      requestTimeout: 8000,
      retry: { retries: 1, initialRetryTime: 300 },
    });
  }
  /**
   * Создаёт локальный topic при необходимости и ждёт подтверждения записи от всех ISR.
   *
   * @remarks
   * acks=-1 означает подтверждение всех текущих синхронных реплик. В стенде
   * replicationFactor=1, поэтому это один брокер, а не защита от потери его диска.
   * Три партиции позволяют группам распределять обработку; бесконечные retention.ms
   * и retention.bytes сохраняют историю для replay до явного удаления ресурсов.
   * У уже существующего topic createTopics не заменяет его настройки.
   *
   * При ошибке producer закрывается, а статус outbox остаётся для повтора.
   * Подтверждение Kafka не означает, что отчёты уже пересчитаны потребителями.
   */
  async publish(event: OrderEvent, incoming?: Record<string, string>) {
    return traced(
      'outbox.kafka',
      { orderId: event.order.id, messageId: event.messageId },
      async () => {
        try {
          if (!this.producer) {
            const admin = this.kafka.admin();
            try {
              await admin.connect();
              await admin.createTopics({
                waitForLeaders: true,
                topics: [
                  {
                    topic: this.topic,
                    numPartitions: 3,
                    replicationFactor: 1,
                    configEntries: [
                      { name: 'retention.ms', value: '-1' },
                      { name: 'retention.bytes', value: '-1' },
                      { name: 'cleanup.policy', value: 'delete' },
                    ],
                  },
                ],
              });
            } finally {
              await admin.disconnect().catch(() => undefined);
            }
            const producer = this.kafka.producer({
              allowAutoTopicCreation: false,
              idempotent: true,
              maxInFlightRequests: 1,
              retry: { retries: 1 },
            });
            this.producer = producer;
            await producer.connect();
          }
          await this.producer.send({
            topic: this.topic,
            acks: -1,
            timeout: 8000,
            messages: [
              {
                key: event.order.eventId,
                value: JSON.stringify(event),
                headers: { messageId: event.messageId, version: String(event.version), ...carrier() },
              },
            ],
          });
        } catch (error) {
          count('outbox_retries', { destination: 'kafka' });
          await this.close();
          throw error;
        }
      },
      incoming,
    );
  }
  async close() {
    const producer = this.producer;
    this.producer = undefined;
    if (producer) await producer.disconnect().catch(() => undefined);
  }
}
