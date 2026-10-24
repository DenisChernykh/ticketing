import { AppError } from '@ticketing/core';
import type { EventInput } from '../domain/event';
export const eventSchema = {
  type: 'object' as const,
  properties: {
    title: { type: 'string', example: 'Backend Meetup' },
    venue: { type: 'string', example: 'Екатеринбург, зал 1' },
    startsAt: { type: 'string', format: 'date-time', example: '2030-10-15T14:00:00.000Z' },
    capacity: { type: 'integer', minimum: 1, maximum: 1000000, example: 100 },
    unitPrice: {
      type: 'integer',
      minimum: 1,
      maximum: 100000000,
      example: 150000,
      description: 'Цена в копейках, RUB',
    },
  },
};
/**
 * Преобразует непроверенный JSON в разрешённые поля мероприятия.
 *
 * @remarks
 * Неизвестные поля отвергаются, поэтому organizerId нельзя подставить в тело:
 * владелец берётся из сессии. Деньги принимаются целыми копейками, дата — с явным
 * часовым поясом и нормализуется в UTC. PATCH может передать часть полей, но не
 * пустой объект. Проверку остатка и владения эта функция не выполняет: для них
 * требуется актуальная строка PostgreSQL и транзакция.
 */
export function eventInput(
  value: unknown,
  partial = false,
): EventInput | (Partial<EventInput> & { version: number }) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) fail();
  const body = value as Record<string, unknown>;
  const fields = ['title', 'venue', 'startsAt', 'capacity', 'unitPrice', ...(partial ? ['version'] : [])];
  if (
    !Object.keys(body).length ||
    Object.keys(body).some((key) => !fields.includes(key)) ||
    (!partial && fields.some((key) => !(key in body)))
  )
    fail();
  const result: Partial<EventInput> = {};
  for (const field of ['title', 'venue'] as const) {
    if (!(field in body)) continue;
    const v = body[field];
    if (typeof v !== 'string' || v.trim().length < 1 || v.length > (field === 'title' ? 200 : 300)) fail();
    result[field] = (v as string).trim();
  }
  if ('startsAt' in body) {
    const value = body.startsAt;
    if (
      typeof value !== 'string' ||
      !/^\d{4}-\d\d-\d\dT.*(?:Z|[+-]\d\d:\d\d)$/.test(value) ||
      !Number.isFinite(Date.parse(value))
    )
      fail();
    result.startsAt = new Date(value as string).toISOString();
  }
  for (const field of ['capacity', 'unitPrice'] as const) {
    if (!(field in body)) continue;
    const v = body[field];
    if (
      typeof v !== 'number' ||
      !Number.isInteger(v) ||
      v < 1 ||
      v > (field === 'capacity' ? 1000000 : 100000000)
    )
      fail();
    result[field] = v as number;
  }
  if (partial) {
    if (!Number.isSafeInteger(body.version) || Number(body.version) < 1 || Object.keys(body).length < 2)
      fail();
    return { ...result, version: Number(body.version) };
  }
  return result as EventInput;
}
function fail(): never {
  throw new AppError(
    'VALIDATION_FAILED',
    'Проверьте название, место, дату, вместимость и цену мероприятия',
    400,
  );
}
export function uuid(value: string): string {
  if (!/^[a-f0-9]{8}-[a-f0-9]{4}-[1-8][a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/i.test(value))
    throw new AppError('VALIDATION_FAILED', 'Ожидается UUID', 400);
  return value;
}
