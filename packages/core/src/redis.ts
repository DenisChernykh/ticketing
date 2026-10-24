import { createClient, RedisClientType } from 'redis';
import { log } from './runtime';

/**
 * Управляет подключением Redis, общим для адаптеров одного процесса.
 *
 * @remarks
 * Offline queue отключена: команды при отсутствии готового соединения получают
 * ошибку вместо ожидания восстановления. Решение об ошибке принимает адаптер:
 * сессии и обязательные лимиты возвращают 503, кеши переходят к PostgreSQL.
 * Сам этот класс не хранит правила кеширования или авторизации.
 */
export class RedisConnection {
  readonly client: RedisClientType;
  constructor(url: string, name: string) {
    this.client = createClient({ url, name, disableOfflineQueue: true,
      socket: { connectTimeout: 2000, reconnectStrategy: (attempt: number) => Math.min(200 * (attempt + 1), 2000) } });
    this.client.on('error', (error: Error) => log('redis.connection_error', { name, error: error.message }));
  }
  /**
   * Запускает подключение с ограниченным ожиданием первого соединения.
   *
   * @remarks
   * Через 2,5 секунды ожидание завершается ошибкой, но переподключение продолжает
   * работать в фоне. Повторный вызов при isOpen не ждёт isReady; успешный возврат
   * не заменяет обработку ошибок последующих Redis-команд.
   */
  async connect(): Promise<void> {
    if (this.client.isReady) return;
    const connecting = this.client.isOpen ? Promise.resolve() : this.client.connect();
    // Фоновое переподключение сохраняется; публичный каталог пока может читать SQL.
    let timeout: NodeJS.Timeout | undefined;
    try { await Promise.race([connecting, new Promise<never>((_, reject) => {
      timeout = setTimeout(() => reject(new Error('Redis startup timeout')), 2500);
    })]); } finally { if (timeout) clearTimeout(timeout); }
  }
  /** Закрывает сокет и прекращает переподключения при остановке процесса. */
  async close(): Promise<void> { if (this.client.isOpen) this.client.destroy(); }
}
