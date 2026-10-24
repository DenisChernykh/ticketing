import { config, Database } from '@ticketing/core';
import { RelayService } from '../application/relay.service';
import { PgOutbox } from '../infrastructure/pg-outbox';
import { KafkaPublisher } from '../infrastructure/kafka-publisher';
import { RabbitPublisher } from '../infrastructure/rabbit-publisher';
type Settings = ReturnType<typeof config>;
export function relayComposition(db: Database, settings: Settings) {
  const rabbitmq = new RabbitPublisher(settings.amqpUrl, settings.prefix);
  const kafka = new KafkaPublisher(settings.kafkaBrokers, settings.kafkaTopic, settings.prefix);
  return {
    relay: new RelayService(new PgOutbox(db), { rabbitmq, kafka }),
    close: async () => {
      await Promise.allSettled([rabbitmq.close(), kafka.close()]);
    },
  };
}
