import { randomBytes, timingSafeEqual } from 'node:crypto';
import { AppError } from '@ticketing/core';
import type { Principal, Role } from '../domain/identity';
import type { IdentityRepository, PasswordVerifier, SessionStore } from './ports';

/**
 * Связывает вход, сессии и проверку прав, сохраняя Redis и PostgreSQL за портами.
 *
 * @remarks
 * Токен сессии доказывает факт входа, роль из свежей выборки — право выполнить
 * операцию, CSRF-токен — владение значением, которое браузер другого сайта не может
 * прочитать. Эти проверки решают разные задачи и не заменяют друг друга.
 */
export class AuthService {
  constructor(private readonly users: IdentityRepository, private readonly sessions: SessionStore,
    private readonly passwords: PasswordVerifier, private readonly loginLimit: number,
    private readonly windowSeconds: number, private readonly orderLimit: number) {}

  /**
   * Проверяет пароль и создаёт новую серверную сессию с отдельным CSRF-токеном.
   *
   * @remarks
   * До затратной проверки пароля расходуются два общих для API лимита: по адресу
   * клиента и по email. Поэтому смена email не обходит лимит одного источника,
   * а смена источника не снимает лимит одного аккаунта. Неизвестный пользователь
   * проходит сопоставимую по стоимости проверку; ответ не раскрывает наличие email.
   * Старые сессии здесь не отзываются: у пользователя может быть несколько входов.
   *
   * @param email - Уже нормализованный и проверенный HTTP-слоем адрес.
   * @param networkAddress - Адрес соединения, а не произвольный заголовок клиента.
   * @returns Пользователь и два токена; sessionToken контроллер помещает только в cookie.
   * @throws AppError при неверных данных, исчерпанном лимите или недоступном Redis.
   */
  async login(email: string, password: string, networkAddress: string) {
    // Два измерения лимита не позволяют обходить защиту простой сменой email.
    await this.limit('login:ip', networkAddress, this.loginLimit);
    await this.limit('login:email', email, this.loginLimit);
    const user = await this.users.findCredential(email);
    const valid = await this.passwords.verify(password, user?.passwordHash ?? '');
    if (!user?.isActive || !valid) throw new AppError('INVALID_CREDENTIALS', 'Неверный email или пароль', 401);
    const csrfToken = randomBytes(32).toString('base64url');
    const sessionToken = await this.sessions.create({ userId: user.id, csrfToken });
    return { sessionToken, csrfToken, user: { id: user.id, email: user.email, role: user.role } };
  }

  /**
   * Восстанавливает пользователя из сессии и проверяет его текущую роль.
   *
   * @remarks
   * Чтение не продлевает TTL. Даже при сохранившейся записи Redis отключённый
   * аккаунт теряет доступ сразу после изменения PostgreSQL. Такая сессия удаляется.
   * Проверка роли относится к типу операции; владение конкретным заказом или
   * мероприятием дополнительно проверяется в соответствующем модуле.
   *
   * @throws AppError с 401 для отсутствующей сессии, 403 для неподходящей роли,
   * либо 503, если Redis не позволяет достоверно проверить сессию.
   */
  async authenticate(token: string | undefined, role?: Role): Promise<Principal> {
    if (!token || !/^[A-Za-z0-9_-]{43}$/.test(token)) throw new AppError('AUTHENTICATION_REQUIRED', 'Войдите в приложение', 401);
    const session = await this.sessions.get(token);
    if (!session) throw new AppError('AUTHENTICATION_REQUIRED', 'Сессия истекла', 401);
    // В сессии нет копии роли: права и отключение аккаунта читаются из PostgreSQL.
    const user = await this.users.findActiveUser(session.userId);
    if (!user) { await this.sessions.delete(token); throw new AppError('AUTHENTICATION_REQUIRED', 'Аккаунт недоступен', 401); }
    if (role && user.role !== role) throw new AppError('FORBIDDEN', 'Недостаточно прав', 403);
    return { user, csrfToken: session.csrfToken, sessionToken: token };
  }

  /**
   * Сверяет явно переданный заголовок с секретом текущей сессии.
   *
   * @remarks
   * Браузер прикладывает cookie автоматически, поэтому одной cookie для изменения
   * данных недостаточно. Проверка длины нужна до timingSafeEqual: сам Node отвергает
   * буферы разной длины. Проверку Origin выполняет HTTP-слой отдельно.
   *
   * @throws AppError с 403 при отсутствии или несовпадении CSRF-токена.
   */
  assertCsrf(principal: Principal, actual: string | undefined): void {
    const expected = Buffer.from(principal.csrfToken);
    const received = Buffer.from(actual ?? '');
    if (expected.length !== received.length || !timingSafeEqual(expected, received)) {
      throw new AppError('CSRF_REJECTED', 'Передайте X-CSRF-Token из /auth/csrf', 403);
    }
  }

  /** Отзывает именно текущую сессию на всех API; остальные входы пользователя сохраняются. */
  async logout(principal: Principal) { await this.sessions.delete(principal.sessionToken); }
  async limitOrder(buyerId: string) { await this.limit('order', buyerId, this.orderLimit); }
  /** Отказ Redis не превращается в разрешение запроса: обязательный лимит закрывается с 503. */
  private async limit(scope: string, key: string, maximum: number) {
    if (!await this.sessions.consumeLimit(scope, key, maximum, this.windowSeconds)) {
      throw new AppError('RATE_LIMITED', 'Слишком много запросов; дождитесь окончания окна лимита', 429);
    }
  }
}
