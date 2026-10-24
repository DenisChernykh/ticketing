import { AppError } from '@ticketing/core';
import type { Principal } from '../domain/identity';

export interface HttpRequest {
  headers: Record<string, string | string[] | undefined>;
  protocol: string; method?: string; ip?: string; socket: { remoteAddress?: string };
  principal?: Principal;
}
export interface HttpResponse { setHeader(name: string, value: string): void; }
export function header(request: HttpRequest, name: string): string | undefined {
  const value = request.headers[name.toLowerCase()];
  return typeof value === 'string' ? value : undefined;
}
/**
 * Отклоняет браузерный запрос изменения данных, пришедший с другого сайта.
 *
 * @remarks
 * Origin сравнивается с протоколом и Host текущего запроса; явный `null` также
 * не совпадёт. Отсутствующий Origin разрешён для обычных HTTP-клиентов, но это
 * не отменяет CSRF для операций с сессией. Настройка доверия обратному прокси
 * здесь не выполняется: локальный стенд обращается к API напрямую.
 */
export function assertOrigin(request: HttpRequest) {
  const origin = header(request, 'origin');
  if (header(request, 'sec-fetch-site') === 'cross-site') throw new AppError('CSRF_REJECTED', 'Cross-site запрос запрещён', 403);
  if (origin && origin !== `${request.protocol}://${header(request, 'host')}`) throw new AppError('CSRF_REJECTED', 'Origin запроса не совпадает с приложением', 403);
}
export function sessionCookie(request: HttpRequest): string | undefined {
  return header(request, 'cookie')?.split(';').map(part => part.trim()).find(part => part.startsWith('ticketing_session='))?.slice('ticketing_session='.length);
}
/**
 * Выдаёт HttpOnly cookie либо удаляет её через Max-Age=0 при выходе.
 * SameSite=Lax дополняет CSRF, а Secure включается настройкой для HTTPS.
 * За фактическое завершение сессии отвечает удаление Redis-записи: очистка
 * cookie без серверного отзыва не остановила бы использование её прежней копии.
 */
export function writeCookie(response: HttpResponse, token: string, ttl: number, secure: boolean) {
  response.setHeader('Set-Cookie', `ticketing_session=${token}; Path=/; HttpOnly; SameSite=Lax; Max-Age=${ttl}${secure ? '; Secure' : ''}`);
}
