/**
 * Ожидаемая ошибка операции, которую HTTP-фильтр переводит в problem+json.
 * code служит стабильным машинным идентификатором, message — пояснением клиенту.
 * В message нельзя передавать SQL, секреты или внутренние подробности драйвера.
 */
export class AppError extends Error {
  constructor(readonly code: string, message: string, readonly status = 400) {
    super(message); this.name = 'AppError';
  }
}

/**
 * Позволяет use case задать атомарную границу, не импортируя PostgreSQL-драйвер.
 *
 * @remarks
 * Инфраструктурная реализация связывает репозитории с одной транзакцией.
 * Это не распределённая транзакция: сетевые вызовы, Redis и брокеры не откатываются.
 * В этом приложении отправка событий вынесена в relay, а use case пишет outbox
 * вместе с бизнес-данными. Вложенный run присоединяется к внешней транзакции.
 */
export interface TransactionPort { run<T>(operation: () => Promise<T>): Promise<T> }
