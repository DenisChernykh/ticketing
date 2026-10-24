# 4. Общий лимит запросов

[Все задачи](../../README.md#tasks)

**Задача.** Работают два экземпляра API. Если каждый считает запросы в своей памяти, пользователь может получить двойной лимит.

**Решение.** Хранить общий счётчик в Redis. Lua-скрипт атомарно увеличивает его и устанавливает TTL при первом запросе. Следующие запросы не продлевают окно. Превышение лимита возвращает `429`, недоступность обязательного хранилища — `503`.

**Код решения:**

- [RedisSessions.consumeLimit()](../../apps/sales/src/modules/identity-access/infrastructure/redis-sessions.ts#L52) — вызывает Lua-скрипт для общего ключа и сравнивает результат с лимитом.
- [LIMIT_SCRIPT](../../apps/sales/src/modules/identity-access/infrastructure/redis-sessions.ts#L7) — выполняет `INCR` и первый `EXPIRE` атомарно.
- [AuthService.limit()](../../apps/sales/src/modules/identity-access/application/auth.service.ts#L91) — применяет лимит и формирует HTTP-ошибку при превышении.

**Проверки:**

- [Два API и одно окно](../../test/integration/redis.test.cjs#L96) — из десяти конкурентных покупок проходят пять, ещё пять получают `429`; после TTL доступна новая покупка.
- [Отказ Redis](../../test/integration/redis.test.cjs#L188) — защищённые операции получают `503`, а публичный каталог остаётся доступен через SQL.

[Подробнее об устройстве решения](../../GUIDE.md#redis).
