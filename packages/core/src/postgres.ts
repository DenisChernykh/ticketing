import { AsyncLocalStorage } from 'node:async_hooks';
import { Pool, PoolClient, QueryResult, QueryResultRow } from 'pg';
import { TransactionPort } from './errors';
import { log } from './runtime';

type Transaction = { client: PoolClient; failed: boolean };

/**
 * Выполняет явный SQL и связывает запросы одной операции с одним соединением.
 *
 * @remarks
 * Application знает только TransactionPort. Инфраструктурные репозитории получают
 * тот же экземпляр Database: внутри run их query используют PoolClient из
 * AsyncLocalStorage, вне run — обычное соединение пула. Поэтому транзакция может
 * охватывать несколько репозиториев без передачи драйвера через бизнес-слой.
 * Область этой гарантии — один экземпляр Database и одна PostgreSQL-база.
 * Redis и брокеры в эту транзакцию не входят; для публикации используется outbox.
 */
export class Database implements TransactionPort {
  private readonly pool: Pool;
  private readonly transaction = new AsyncLocalStorage<Transaction>();
  private closing?: Promise<void>;

  constructor(connectionString: string, applicationName: string) {
    this.pool = new Pool({ connectionString, application_name: applicationName,
      max: 20, connectionTimeoutMillis: 3000, idleTimeoutMillis: 10000, query_timeout: 17000,
      options: '-c statement_timeout=15000 -c lock_timeout=10000 -c idle_in_transaction_session_timeout=20000' });
    this.pool.on('error', (error: Error) => log('postgres.connection_error', { applicationName, error: error.message }));
  }

  /**
   * Выполняет параметризованный запрос на соединении текущей транзакции или пула.
   *
   * @remarks
   * SQL-ошибка делает текущую транзакцию непригодной для коммита. Флаг failed
   * сохраняет это состояние даже если вызывающий код перехватил исключение:
   * run не должен вернуть успешный результат после фактического отката PostgreSQL.
   * Тип Row описывает ожидаемый результат для TypeScript, но не валидирует строки.
   *
   * @param sql - SQL с позиционными параметрами $1, $2 и далее.
   * @param values - Значения параметров; пользовательские данные не вставляются в SQL.
   * @returns Строки и метаданные драйвера, включая rowCount для условных изменений.
   */
  async query<Row extends QueryResultRow = any>(sql: string, values?: any[]): Promise<QueryResult<Row>> {
    if (this.closing) throw new Error('Database is closing');
    const active = this.transaction.getStore();
    try { return await (active ? active.client.query<Row>(sql, values) : this.pool.query<Row>(sql, values)); }
    catch (error) {
      // После SQL-ошибки COMMIT может ответить ROLLBACK без нового исключения.
      // Сохраняем ошибочное состояние, чтобы run не сообщил об успешном коммите.
      if (active) active.failed = true;
      throw error;
    }
  }

  /**
   * Коммитит всю операцию либо откатывает её изменения на одном PoolClient.
   *
   * @remarks
   * Внешний run берёт соединение, начинает READ COMMITTED и освобождает его
   * в finally. Вложенный run присоединяется к той же транзакции: отдельного
   * коммита и SAVEPOINT нет. Ошибка вложенной операции помечает всю транзакцию
   * как rollback-only, даже если внешний код затем поймает исключение.
   *
   * READ COMMITTED сам по себе не защищает составную проверку остатка.
   * Блокировки строк и условия UPDATE задаются SQL-репозиториями.
   * Все действия operation должны быть await-нуты до её завершения;
   * запущенная в фоне работа не продлевает время жизни транзакции.
   * Если даже ROLLBACK не удался, соединение уничтожается вместо возврата в пул.
   * Потеря соединения во время COMMIT может оставить исход неопределённым:
   * сервер мог сохранить данные до потери ответа. Ошибка run сама по себе
   * не доказывает откат; покупка разрешает эту ситуацию повтором с прежним ключом.
   *
   * @param operation - Операция с запросами через этот экземпляр Database.
   * @returns Для внешнего вызова — результат после успешного COMMIT; для вложенного —
   * результат callback до коммита, за который всё ещё отвечает внешний run.
   * @throws Ошибка операции, соединения, коммита или состояния rollback-only.
   */
  async run<T>(operation: () => Promise<T>): Promise<T> {
    const existing = this.transaction.getStore();
    if (existing) {
      try { return await operation(); }
      catch (error) { existing.failed = true; throw error; }
    }
    if (this.closing) throw new Error('Database is closing');
    const client = await this.pool.connect();
    const context: Transaction = { client, failed: false };
    let destroy = false;
    try {
      await client.query('BEGIN ISOLATION LEVEL READ COMMITTED');
      const result = await this.transaction.run(context, operation);
      if (context.failed) throw new Error('Transaction is rollback-only after a nested failure');
      await client.query('COMMIT');
      return result;
    } catch (error) {
      try { await client.query('ROLLBACK'); } catch { destroy = true; }
      throw error;
    } finally { client.release(destroy); }
  }

  /** Закрывает пул один раз; новые query и внешние run после этого запрещены. */
  close(): Promise<void> { return this.closing ??= this.pool.end(); }
}
