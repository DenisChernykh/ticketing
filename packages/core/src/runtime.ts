import { traceFields } from '@ticketing/observability';
import { timingSafeEqual } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { AppError } from './errors';

export function log(event: string, fields: Record<string, unknown> = {}): void {
  process.stdout.write(
    `${JSON.stringify({ time: new Date().toISOString(), event, ...traceFields(), ...fields })}\n`,
  );
}

/**
 * Допускает к внутреннему HTTP API только запрос со служебным токеном.
 *
 * @remarks
 * После этой проверки получатель доверяет переданному Sales контексту владельца,
 * но всё равно ограничивает им SQL-выборку. Токен подтверждает доверенный сервис,
 * а не права конкретного покупателя: пользовательские права проверяются в Sales.
 * Сначала сравнивается длина буферов, затем содержимое через timingSafeEqual.
 *
 * @throws AppError со статусом 401 при отсутствующем или неверном токене.
 */
export function assertInternalToken(value: unknown, expected: string): void {
  if (typeof value !== 'string')
    throw new AppError(
      'internal_unauthorized',
      'Требуется служебная авторизация',
      401,
    );
  const actual = Buffer.from(value),
    wanted = Buffer.from(expected);
  if (actual.length !== wanted.length || !timingSafeEqual(actual, wanted)) {
    throw new AppError(
      'internal_unauthorized',
      'Требуется служебная авторизация',
      401,
    );
  }
}

export async function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  try {
    await delay(ms, undefined, { signal });
  } catch (error) {
    if (!signal?.aborted) throw error;
  }
}
