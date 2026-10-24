import { config } from '@ticketing/core';

/**
 * Передаёт команду уже работающему владельцу consumers через внутренний HTTP API.
 * CLI не создаёт Kafka-группу и не пишет в таблицы проекций: только runtime
 * Analytics может согласовать остановку старой группы, replay и продолжение.
 * Ожидание HTTP ограничено 150 секундами; его ошибка сама по себе не доказывает,
 * что сервер не успел переключить поколение — итог нужно читать на сервере.
 */
async function main(): Promise<void> {
  const projection = process.argv[2] ?? 'all';
  if (!['events', 'daily', 'all'].includes(projection))
    throw new Error('Использование: replay events|daily|all');
  const environment = config();
  const response = await fetch(`${environment.analyticsUrl}/internal/replay`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-Internal-Token': environment.internalToken,
    },
    body: JSON.stringify({ projection }),
    signal: AbortSignal.timeout(150_000),
  });
  const body = await response.text();
  if (!response.ok) throw new Error(`Replay HTTP ${response.status}: ${body}`);
  console.log(JSON.stringify(JSON.parse(body), null, 2));
}

void main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
