import { createHash, randomBytes } from 'node:crypto';
import { AppError, RedisConnection } from '@ticketing/core';
import type { SessionStore } from '../application/ports';
import type { Session } from '../domain/identity';

// INCR и первый EXPIRE выполняются одним Lua-скриптом: между ними нельзя потерять процесс.
const LIMIT_SCRIPT = `local n = redis.call('INCR', KEYS[1]); if n == 1 then redis.call('EXPIRE', KEYS[1], ARGV[1]); end; return n`;
/**
 * Общая для экземпляров Sales реализация сессий и лимитов.
 *
 * @remarks
 * Клиенту выдаётся случайный токен; в имени Redis-ключа хранится его SHA-256.
 * Это не позволяет превратить одно лишь имя ключа в готовую cookie. Значение
 * содержит идентификатор пользователя и CSRF-токен, поэтому Redis остаётся
 * доверенным хранилищем. Его политика памяти должна сохранять сессии, а не
 * вытеснять их ради кеша; при ошибке памяти запись завершается отказом.
 */
export class RedisSessions implements SessionStore {
  constructor(private readonly redis: RedisConnection, private readonly prefix: string, private readonly ttl: number) {}
  private key(token: string) { return `${this.prefix}:session:${createHash('sha256').update(token).digest('hex')}`; }
  /** Превращает сбой обязательного хранилища в 503 без раскрытия адреса Redis или его данных. */
  private async available<T>(operation: () => Promise<T>): Promise<T> {
    try { return await operation(); } catch { throw new AppError('DEPENDENCY_UNAVAILABLE', 'Хранилище сессий и лимитов недоступно', 503); }
  }
  /**
   * Атомарно сохраняет сессию вместе с абсолютным TTL.
   * Токен возвращается только после успешного SET: клиент не получает вход,
   * который сервер ещё не способен проверить. GET далее не обновляет срок жизни.
   */
  async create(session: Session): Promise<string> {
    const token = randomBytes(32).toString('base64url');
    await this.available(() => this.redis.client.set(this.key(token), JSON.stringify(session), { EX: this.ttl }));
    return token;
  }
  /** Отсутствующая запись или невалидный JSON означают отсутствие сессии; сбой Redis остаётся 503. */
  async get(token: string): Promise<Session | undefined> {
    const data = await this.available(() => this.redis.client.get(this.key(token)));
    if (!data) return undefined;
    try { return JSON.parse(data) as Session; } catch { return undefined; }
  }
  async delete(token: string): Promise<void> { await this.available(() => this.redis.client.del(this.key(token))); }
  /**
   * Расходует одну попытку в общем счётчике и сообщает, допускается ли запрос.
   *
   * @remarks
   * Окно отсчитывается от первой попытки. Следующие попытки, включая отклонённые,
   * увеличивают счётчик, но не продлевают TTL. Это фиксированное окно, а не
   * скользящий лимит: близко к его границе возможны две соседние допустимые серии.
   * Хеш ключа исключает email и сетевой адрес из читаемых имён Redis-ключей.
   * Lua обеспечивает единый результат для одновременно работающих API.
   */
  async consumeLimit(scope: string, key: string, maximum: number, window: number): Promise<boolean> {
    const digest = createHash('sha256').update(key).digest('hex');
    const count = await this.available(() => this.redis.client.eval(LIMIT_SCRIPT, {
      keys: [`${this.prefix}:limit:${scope}:${digest}`], arguments: [String(window)],
    }));
    return Number(count) <= maximum;
  }
}
