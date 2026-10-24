import type { ChannelModel } from 'amqplib';

/**
 * Завершает AMQP-соединение, не позволяя протокольному закрытию зависнуть навсегда.
 *
 * @remarks
 * В используемой версии amqplib потеря сокета в ожидании `CloseOk` может вызвать
 * событие `close`, но оставить Promise от `close()` незавершённым. Поэтому здесь
 * конкурируют три сигнала: Promise, событие и срок ожидания. Любой исход убирает
 * таймер и обработчик события; повторный сигнал ничего не меняет.
 *
 * При истечении срока или ошибке закрытия уничтожается нижележащий поток. Это
 * локальная особенность адаптера закреплённой версии amqplib, а не публичный контракт
 * бизнес-кода. Неподтверждённые доставки после разрыва вернутся брокеру; такая очистка
 * не доказывает, что обработка сообщения успела завершиться.
 *
 * @param connection - Соединение для освобождения; отсутствие соединения допустимо.
 * @param deadlineMs - Максимальное ожидание AMQP-закрытия, по умолчанию одна секунда.
 * @returns Завершение очистки без проброса ошибок уже закрытого сокета.
 */
export function closeRabbitConnection(connection: ChannelModel | undefined, deadlineMs = 1000): Promise<void> {
  if (!connection) return Promise.resolve();
  return new Promise(resolve => {
    let finished = false;
    const finish = () => {
      if (finished) return;
      finished = true;
      clearTimeout(timer);
      connection.off('close', finish);
      resolve();
    };
    const destroy = () => {
      // Только адаптер знает о внутреннем stream: публичный close() не даёт своего deadline.
      const stream = (connection.connection as unknown as { stream?: { destroy(): void } }).stream;
      try { stream?.destroy(); } catch { /* Ошибка уже закрытого сокета не должна остановить очистку. */ }
      finish();
    };
    const timer = setTimeout(destroy, deadlineMs);
    connection.once('close', finish);
    try { void connection.close().then(finish, destroy); }
    catch { destroy(); }
  });
}
