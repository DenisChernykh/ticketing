import { AppError } from '@ticketing/core';

export interface LoginInput { email: string; password: string; }

/**
 * Строго разбирает тело входа до вызова сценария: только email/password,
 * ограниченные строки и допустимый формат email. Проверка предшествует
 * нормализации, поэтому пробелы в email по-прежнему отвергаются. Пароль
 * не обрезается и не меняет регистр; email приводится к нижнему регистру.
 */
export function loginInput(body: unknown): LoginInput {
  const data = body as Record<string, unknown> | null;
  if (!data || typeof data !== 'object' || Array.isArray(data) || typeof data.email !== 'string' || typeof data.password !== 'string'
    || data.email.length > 254 || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(data.email) || data.password.length < 1 || data.password.length > 256
    || Object.keys(data).some(key => !['email', 'password'].includes(key))) throw new AppError('VALIDATION_FAILED', 'Передайте email и password', 400);
  return { email: data.email.trim().toLowerCase(), password: data.password };
}
