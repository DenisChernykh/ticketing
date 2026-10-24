import { AppError } from '@ticketing/core';
import { isUuid } from '@ticketing/contracts';
import type { Purchase } from '../domain/order';

/** Проверяет транспортный идентификатор до обращения к квоте или данным заказа. */
export function orderId(value: unknown): string {
  if (!isUuid(value)) throw new AppError('VALIDATION_FAILED', 'Ожидается UUID', 400);
  return value;
}

/**
 * Полностью разбирает тело покупки и обязательный заголовок Idempotency-Key.
 *
 * @remarks
 * Здесь проверяется форма запроса, а не наличие мест или результат прежней покупки.
 * Все проверки завершаются до расходования лимита: неверный UUID, количество,
 * неизвестное поле и неверный ключ одинаково дают 400 без обращения к квоте.
 * Владельца в теле нет: контроллер получает его из проверенного principal.
 *
 * @returns Новая структура только с разрешёнными и проверенными полями.
 * @throws AppError со статусом 400 при нарушении HTTP-контракта.
 */
export function purchaseInput(body: unknown, rawKey: unknown): { input: Purchase; key: string } {
  const data = body as Record<string, unknown> | null;
  if (!data || typeof data !== 'object' || Array.isArray(data) || Object.keys(data).some(key => !['eventId', 'quantity'].includes(key))
    || typeof data.eventId !== 'string' || typeof data.quantity !== 'number' || !Number.isInteger(data.quantity) || data.quantity < 1 || data.quantity > 100)
    throw new AppError('VALIDATION_FAILED', 'Передайте eventId и целое quantity от 1 до 100', 400);
  if (typeof rawKey !== 'string' || !/^[\x21-\x7e]{1,128}$/.test(rawKey))
    throw new AppError('VALIDATION_FAILED', 'Передайте Idempotency-Key (1–128 печатных ASCII-символов без пробелов)', 400);
  return { input: { eventId: orderId(data.eventId), quantity: data.quantity }, key: rawKey };
}
