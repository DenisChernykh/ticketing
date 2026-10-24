const { readFile, readdir } = require('node:fs/promises');
const { join, resolve } = require('node:path');
const { createHash } = require('node:crypto');
const { Database, config } = require('@ticketing/core');

/**
 * Применяет SQL-файлы одного сервиса и фиксирует их checksum в его собственной БД.
 *
 * Файл и запись в schema_migrations коммитятся вместе. Транзакционная advisory lock
 * не даёт двум migrator одновременно решить, что одна версия ещё не применена.
 * Уже выполненный файл с прежним checksum пропускается, с новым — вызывает ошибку:
 * изменение схемы нужно оформлять следующей миграцией, не редактированием истории.
 * Каждая версия имеет отдельную транзакцию; общей транзакции трёх сервисов нет.
 *
 * @param {string} service Имя каталога миграций: sales, fulfillment или analytics.
 * @param {string} url Подключение к принадлежащей этому сервису базе.
 */
async function migrateService(service, url) {
  const directory = resolve(__dirname, `../apps/${service}/migrations`);
  const files = (await readdir(directory)).filter((file) => /^\d+.*\.sql$/.test(file)).sort();
  const db = new Database(url, `${service}-migrator`);
  try {
    for (const file of files) {
      const sql = await readFile(join(directory, file), 'utf8');
      const checksum = createHash('sha256').update(sql).digest('hex');
      await db.run(async () => {
        await db.query("SELECT pg_advisory_xact_lock(hashtext('ticketing-migrations'))");
        await db.query(`CREATE TABLE IF NOT EXISTS schema_migrations (
          name text PRIMARY KEY, checksum text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now())`);
        const existing = (await db.query('SELECT checksum FROM schema_migrations WHERE name=$1', [file]))
          .rows[0];
        if (existing) {
          if (existing.checksum !== checksum)
            throw new Error(`Applied migration changed: ${service}/${file}`);
          return;
        }
        await db.query(sql);
        await db.query('INSERT INTO schema_migrations(name,checksum) VALUES($1,$2)', [file, checksum]);
        process.stdout.write(`migrated ${service}/${file}\n`);
      });
    }
  } finally {
    await db.close();
  }
}

async function migrateAll(settings = config()) {
  for (const [service, key] of [
    ['sales', 'sales'],
    ['fulfillment', 'fulfillment'],
    ['analytics', 'analytics'],
    ['payment-mock', 'paymentMock'],
  ])
    await migrateService(service, settings[`${key}DatabaseUrl`]);
}
module.exports = { migrateAll, migrateService };
if (require.main === module)
  migrateAll().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
