import type { Credential, Session, User } from '../domain/identity';

/**
 * Даёт сценарию входа учётные данные, а проверке сессии — актуальное состояние аккаунта.
 *
 * @remarks
 * Разделение выборок не позволяет случайно вернуть passwordHash из `/auth/me`.
 * Роль и доступность пользователя запрашиваются заново при каждом защищённом запросе:
 * сохранённая сессия сама по себе не подтверждает действующие права.
 */
export interface IdentityRepository {
  findCredential(email: string): Promise<Credential | undefined>;
  findActiveUser(id: string): Promise<User | undefined>;
}
/**
 * Хранит отзываемые сессии и атомарно учитывает запросы всех экземпляров API.
 *
 * @remarks
 * Сценарии не знают команд Redis. Адаптер отвечает за срок жизни, генерацию
 * непрозрачного токена и отсутствие гонки между увеличением счётчика и установкой TTL.
 * Недоступное хранилище должно отклонить операцию: отсутствие сессии и невозможность
 * проверить сессию — разные ситуации, соответственно 401 и 503 на HTTP-границе.
 */
export interface SessionStore {
  create(session: Session): Promise<string>;
  get(token: string): Promise<Session | undefined>;
  delete(token: string): Promise<void>;
  consumeLimit(scope: string, key: string, maximum: number, windowSeconds: number): Promise<boolean>;
}
/** Проверяет пароль без передачи алгоритма хеширования и формата хранения в сценарий входа. */
export interface PasswordVerifier { verify(password: string, encoded: string): Promise<boolean>; }
