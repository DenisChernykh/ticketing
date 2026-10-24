import { resolve } from 'node:path';
import { config as dotenv } from 'dotenv';

dotenv({ path: resolve(__dirname, '../../../.env'), quiet: true });

function port(key: string, fallback: number): number {
  const value = Number(process.env[key] ?? fallback);
  if (!Number.isInteger(value) || value < 1 || value > 65535)
    throw new Error(`Invalid ${key}`);
  return value;
}

/**
 * Читает конфигурацию процессов с готовыми локальными значениями для Compose.
 *
 * @remarks
 * Отдельные URL баз закрепляют владение данными сервисов; prefix отделяет
 * ключи Redis, очереди и topics разных запусков. Проверка формата prefix
 * запрещает произвольно менять дерево имён ресурсов.
 * Все пароли и служебный токен по умолчанию предназначены для локального стенда.
 * Функция не открывает соединения и не применяет миграции.
 */
export function config() {
  const prefix = process.env.RESOURCE_PREFIX ?? 'ticketing';
  if (!/^[a-z][a-z0-9_-]{0,48}$/.test(prefix))
    throw new Error('Invalid RESOURCE_PREFIX');
  const pgPort = port('TICKETING_PG_PORT', 56432);
  const internalToken =
    process.env.INTERNAL_TOKEN ?? 'local-ticketing-internal-token';
  if (internalToken.length < 24)
    throw new Error('INTERNAL_TOKEN must contain at least 24 characters');
  return {
    paymentMockDatabaseUrl:
      process.env.PAYMENT_MOCK_DATABASE_URL ??
      `postgresql://payment_mock:payment_mock-local@127.0.0.1:${pgPort}/ticketing_payment_mock`,
    paymentUrl: process.env.PAYMENT_URL ?? 'http://127.0.0.1:3113',
    paymentMockPort: port('PAYMENT_MOCK_PORT', 3113),
    webhookUrl:
      process.env.PAYMENT_WEBHOOK_URL ??
      'http://127.0.0.1:3110/webhooks/payments',
    webhookSecret:
      process.env.PAYMENT_WEBHOOK_SECRET ??
      'local-payment-webhook-secret-education',
    salesDatabaseUrl:
      process.env.SALES_DATABASE_URL ??
      `postgresql://sales:sales-local@127.0.0.1:${pgPort}/ticketing_sales`,
    fulfillmentDatabaseUrl:
      process.env.FULFILLMENT_DATABASE_URL ??
      `postgresql://fulfillment:fulfillment-local@127.0.0.1:${pgPort}/ticketing_fulfillment`,
    analyticsDatabaseUrl:
      process.env.ANALYTICS_DATABASE_URL ??
      `postgresql://analytics:analytics-local@127.0.0.1:${pgPort}/ticketing_analytics`,
    redisUrl:
      process.env.REDIS_URL ??
      `redis://127.0.0.1:${port('TICKETING_REDIS_PORT', 57389)}`,
    amqpUrl:
      process.env.AMQP_URL ??
      `amqp://ticketing:ticketing-local@127.0.0.1:${port('TICKETING_AMQP_PORT', 56672)}`,
    kafkaBrokers: (
      process.env.KAFKA_BROKERS ??
      `127.0.0.1:${port('TICKETING_KAFKA_PORT', 60092)}`
    ).split(','),
    kafkaTopic: process.env.KAFKA_TOPIC ?? `${prefix}.sales.v1`,
    internalToken,
    fulfillmentUrl: process.env.FULFILLMENT_URL ?? 'http://127.0.0.1:3111',
    analyticsUrl: process.env.ANALYTICS_URL ?? 'http://127.0.0.1:3112',
    salesPort: port('SALES_PORT', 3110),
    fulfillmentPort: port('FULFILLMENT_PORT', 3111),
    analyticsPort: port('ANALYTICS_PORT', 3112),
    prefix,
  };
}
