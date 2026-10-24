import { randomBytes, scrypt, timingSafeEqual } from 'node:crypto';
import type { PasswordVerifier } from '../application/ports';

/** Выполняет scrypt асинхронно, чтобы затратное вычисление не занимало основной цикл Node.js. */
function derive(password: string, salt: string): Promise<Buffer> {
  return new Promise((resolve, reject) => scrypt(password, salt, 64, { N: 16384, r: 8, p: 1 },
    (error, key) => error ? reject(error) : resolve(key)));
}
/**
 * Создаёт строку для password_hash при подготовке демонстрационных аккаунтов.
 *
 * @remarks
 * Случайная соль делает хеши одинаковых паролей различными. Параметры scrypt
 * зафиксированы в derive; формат строки содержит алгоритм, соль и результат,
 * но не параметры стоимости. Их изменение потребовало бы продумать совместимость
 * уже сохранённых хешей, а не только поменять константу.
 */
export async function hashPassword(password: string): Promise<string> {
  const salt = randomBytes(16).toString('hex');
  return `scrypt$${salt}$${(await derive(password, salt)).toString('hex')}`;
}
/** Проверка пароля с безопасным сравнением и затратным вычислением даже для неизвестного email. */
export class ScryptPasswords implements PasswordVerifier {
  async verify(password: string, encoded: string): Promise<boolean> {
    const [, salt, hash] = /^scrypt\$([a-f0-9]{32})\$([a-f0-9]{128})$/.exec(encoded) ?? [];
    // Для неизвестного аккаунта тоже выполняется затратный scrypt, затем проверка завершается false.
    const actual = await derive(password, salt ?? '0'.repeat(32));
    const expected = hash ? Buffer.from(hash, 'hex') : Buffer.alloc(64);
    return timingSafeEqual(actual, expected) && !!hash;
  }
}
