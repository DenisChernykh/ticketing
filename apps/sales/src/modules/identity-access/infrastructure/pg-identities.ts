import { Database } from '@ticketing/core';
import type { IdentityRepository } from '../application/ports';
import type { Credential, User } from '../domain/identity';

/**
 * Читает учётные данные только из базы Sales с параметризованными запросами.
 * Активность не фильтруется в findCredential: сценарий входа проверяет пароль,
 * затем возвращает одинаковую ошибку для отключённого и неизвестного аккаунта.
 */
export class PgIdentities implements IdentityRepository {
  constructor(private readonly db: Database) {}
  async findCredential(email: string): Promise<Credential | undefined> {
    return (await this.db.query<Credential>('SELECT id, email, role, password_hash AS "passwordHash", is_active AS "isActive" FROM users WHERE email = $1', [email])).rows[0];
  }
  async findActiveUser(id: string): Promise<User | undefined> {
    return (await this.db.query<User>('SELECT id, email, role FROM users WHERE id = $1 AND is_active = true', [id])).rows[0];
  }
}
