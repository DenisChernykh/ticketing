import { applyDecorators, createParamDecorator, Inject, Injectable, SetMetadata, UseGuards,
  type CanActivate, type ExecutionContext } from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import { AppError } from '@ticketing/core';
import { AuthService } from '../application/auth.service';
import type { Principal, Role } from '../domain/identity';
import { assertOrigin, header, type HttpRequest, sessionCookie } from './request';

const SESSION_POLICY = Symbol('SESSION_POLICY');
const SAFE_METHODS = new Set(['GET', 'HEAD', 'OPTIONS']);
interface SessionPolicy { role?: Role; }

/**
 * Объявляет политику сессии на классе контроллера или отдельном маршруте.
 * Политика метода имеет приоритет перед политикой класса. Проверку выполняет
 * Nest guard до обработчика: изменяющие запросы автоматически требуют Origin/CSRF,
 * поэтому контроллеру не нужно помнить отдельный флаг mutation.
 */
export function RequireSession(role?: Role) {
  return applyDecorators(SetMetadata(SESSION_POLICY, { role } satisfies SessionPolicy), UseGuards(SessionGuard));
}

/**
 * Проверяет сессию и актуальную роль, затем сохраняет доверенный principal в запросе.
 *
 * @remarks
 * Не использует заранее заполненный request.principal как доказательство входа.
 * Пользователь перечитывается AuthService из PostgreSQL; недоступность Redis
 * остаётся ошибкой 503. Для GET/HEAD/OPTIONS достаточно сессии, остальные методы
 * также проходят Origin и CSRF. Владение конкретным заказом проверяет сценарий.
 */
@Injectable()
export class SessionGuard implements CanActivate {
  constructor(@Inject(AuthService) private readonly auth: AuthService, @Inject(Reflector) private readonly reflector: Reflector) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const request = context.switchToHttp().getRequest<HttpRequest>();
    request.principal = undefined;
    const policy = this.reflector.getAllAndOverride<SessionPolicy>(SESSION_POLICY, [context.getHandler(), context.getClass()]);
    if (!policy) throw new AppError('AUTHENTICATION_REQUIRED', 'Для маршрута не задана политика сессии', 401);
    const identity = await this.auth.authenticate(sessionCookie(request), policy.role);
    if (!SAFE_METHODS.has((request.method ?? '').toUpperCase())) {
      assertOrigin(request);
      this.auth.assertCsrf(identity, header(request, 'x-csrf-token'));
    }
    request.principal = identity;
    return true;
  }
}

/** Защищает вход от чужого Origin, когда действующей сессии и CSRF-токена ещё нет. */
@Injectable()
export class OriginGuard implements CanActivate {
  canActivate(context: ExecutionContext): boolean {
    assertOrigin(context.switchToHttp().getRequest<HttpRequest>());
    return true;
  }
}

/**
 * Передаёт контроллеру только principal, уже установленный SessionGuard.
 * При пропущенном guard не возвращает undefined и не собирает личность из тела
 * или заголовков: закрывает доступ с 401, делая ошибку настройки явной.
 */
export const CurrentPrincipal = createParamDecorator((_data: unknown, context: ExecutionContext): Principal => {
  const identity = context.switchToHttp().getRequest<HttpRequest>().principal;
  if (!identity) throw new AppError('AUTHENTICATION_REQUIRED', 'Сессия запроса не проверена', 401);
  return identity;
});
