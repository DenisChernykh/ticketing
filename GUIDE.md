# Подробности реализации

Выбрать любую из 13 учебных задач и открыть её короткое решение можно в [каталоге README](README.md#tasks). Здесь собраны подробные объяснения механизмов, ссылки на связанные файлы и проверки, включая авторизацию, SQL и применения Redis.

Запуск и HTTP-примеры — в [README](README.md), владение данными и границы модулей — в [архитектуре](docs/architecture.md).

## Разделы справочника

- [Вход, права и HTTP API](#identity)
- [Транзакции, идемпотентность и outbox](#checkout)
- [Каталог и SQL-запросы](#sql)
- [Kafka: независимые проекции и replay](#kafka)
- [RabbitMQ: билеты, уведомления и повторы](#rabbitmq)
- [Redis: сессии, лимиты, кеш и рейтинг](#redis)
- [Пагинация по курсору](#pagination)
- [Защита от конфликтующих изменений](#edit-conflicts)
- [Бронирование и фоновое освобождение мест](#reservation-expiry)
- [Оплата, webhook и сверка результата](#payments)
- [Отмена заказа и возврат денег](#cancellation)
- [Потоковая выгрузка CSV](#csv-export)
- [Трассировка, метрики и диагностика](#observability)
- [Где работает фон](#background)
- [Как проверять решения](#verification)

<a id="identity"></a>

## Вход, права и HTTP API

Покупатель и организатор входят в приложение, получают доступ к своим операциям и данным. Неверный или недопустимый запрос завершается понятной HTTP-ошибкой.

| Подзадача | Основное решение | Проверка |
| --- | --- | --- |
| Выполнить вход, сохранить сессию между запросами и отозвать её при выходе | [AuthService](apps/sales/src/modules/identity-access/application/auth.service.ts), [RedisSessions](apps/sales/src/modules/identity-access/infrastructure/redis-sessions.ts), [проверка пароля](apps/sales/src/modules/identity-access/infrastructure/password.ts) | [Redis](test/integration/redis.test.cjs): два API видят одну сессию, logout и истечение TTL |
| Разделить права покупателя и организатора, скрыть чужие заказы и мероприятия | [guards](apps/sales/src/modules/identity-access/presentation/access.ts), [OrdersService](apps/sales/src/modules/orders/application/orders.service.ts), [PgCatalog](apps/sales/src/modules/catalog/infrastructure/pg-catalog.ts) | [HTTP-границы](test/integration/http-boundaries.test.cjs), [продукт](test/integration/product.test.cjs): чужая роль, заказ и отчёт не дают доступа |
| Защитить изменение данных и logout от CSRF | [AuthService.assertCsrf](apps/sales/src/modules/identity-access/application/auth.service.ts), [проверка Origin и guards](apps/sales/src/modules/identity-access/presentation/access.ts) | [HTTP-границы](test/integration/http-boundaries.test.cjs): неверные CSRF и Origin отклоняются |
| Проверить HTTP-ввод и вернуть понятную ошибку со стабильным кодом | [валидация заказа](apps/sales/src/modules/orders/presentation/order.validation.ts), [валидация мероприятия](apps/sales/src/modules/catalog/presentation/event.validation.ts), [ProblemFilter и Swagger](packages/core/src/http.ts) | [HTTP-границы](test/integration/http-boundaries.test.cjs), [unit-проверки](apps/sales/src/tests/security.test.ts): неверные поля, суммы и даты; отсутствие записи и расхода квоты |

`AuthService` проверяет пароль и создаёт серверную сессию в Redis. Cookie содержит случайный токен; Redis хранит данные с абсолютным TTL. Чтение сессии не продлевает её жизнь, logout отзывает текущую сессию на всех API. Роль и активность пользователя проверяются по PostgreSQL, а владение конкретным ресурсом — в модуле заказа, каталога или отчёта.

Для изменения данных одной cookie недостаточно: запрос должен пройти проверки CSRF и Origin. Контроллеры проверяют тело и параметры до прикладной операции. Ошибки возвращаются через `ProblemFilter` с кодом и requestId. Лимиты входа и заказов общие для процессов: Lua-скрипт атомарно выполняет `INCR` и первый `EXPIRE`. Отказ обязательного хранилища сессий даёт `503`.

<a id="checkout"></a>

## Транзакции, идемпотентность и outbox

Короткие решения: [1. Последнее место](docs/tasks/01-last-seat.md); [2. Повтор запроса без второго заказа](docs/tasks/02-idempotency.md); [3. Доставка события после сохранения заказа](docs/tasks/03-outbox.md).

Покупка сохраняется одной локальной транзакцией. Конкуренция за последнее место, повтор запроса и сбой брокера не приводят к лишнему заказу или потере события.

| Подзадача | Основное решение | Проверка |
| --- | --- | --- |
| Отдать последнее место одному покупателю и откатить всю операцию при ошибке | [CheckoutService.purchase](apps/sales/src/modules/checkout/application/checkout.service.ts), [PgInventory](apps/sales/src/modules/catalog/infrastructure/pg-inventory.ts), [Database.run](packages/core/src/postgres.ts) | [Продукт](test/integration/product.test.cjs): конкуренция за последнее место и искусственная ошибка SQL |
| Повторить создание заказа с тем же ключом без второй покупки | [CheckoutService.purchase](apps/sales/src/modules/checkout/application/checkout.service.ts), [PgOrders.claimKey / completeKey](apps/sales/src/modules/orders/infrastructure/pg-orders.ts) | [Продукт](test/integration/product.test.cjs): одновременные повторы одного ключа и конфликт другого тела |
| Сохранить изменение заказа и событие для брокеров одной транзакцией | [CheckoutService](apps/sales/src/modules/checkout/application/checkout.service.ts), [PgEventWriter](apps/sales/src/modules/messaging/infrastructure/pg-event-writer.ts) | [Продукт](test/integration/product.test.cjs): записи outbox и доставка подтверждённой покупки; [Lifecycle](test/integration/lifecycle.test.cjs): отдельные события подтверждения, отмены и возврата |
| Доставить outbox в RabbitMQ и Kafka независимо, пережить отказ брокера и повтор публикации | [RelayService](apps/sales/src/modules/messaging/application/relay.service.ts), [PgOutbox](apps/sales/src/modules/messaging/infrastructure/pg-outbox.ts), [publishers](apps/sales/src/modules/messaging/composition/messaging.ts) | [Продукт](test/integration/product.test.cjs): остановка одного направления, восстановление, повтор после коммита |
| Сохранить границы модулей и сервисов при добавлении новых сценариев | [композиция Sales](apps/sales/src/composition/sales.ts), [прикладные порты Checkout](apps/sales/src/modules/checkout/application/checkout.service.ts), [архитектура](docs/architecture.md) | [check-architecture](scripts/check-architecture.cjs): допустимые импорты слоёв, публичные границы модулей, запрет драйверов в бизнес-коде |

Блокировка строки мероприятия защищает последнее место от двух покупателей. Идемпотентность решает другую задачу: конкурентные запросы одного покупателя с одинаковым ключом получают один сохранённый результат. Другой состав покупки с прежним ключом вызывает конфликт. Запись заказа, изменение остатка и сохранение результата ключа входят в одну транзакцию; ошибка откатывает их вместе.

[Database.run](packages/core/src/postgres.ts) передаёт контекст транзакции SQL-адаптерам без PoolClient в прикладных методах. Checkout координирует владельцев данных через порты. [PgOrderQueries](apps/sales/src/modules/orders/infrastructure/pg-order-queries.ts) и [PgCatalogQueries](apps/sales/src/modules/catalog/infrastructure/pg-catalog-queries.ts) отдельно собирают ответы для чтения: JOIN допустим внутри базы Sales и не даёт права записывать чужие таблицы.

[PgEventWriter](apps/sales/src/modules/messaging/infrastructure/pg-event-writer.ts) сохраняет снимок и две доставки в транзакции Checkout. [RelayService](apps/sales/src/modules/messaging/application/relay.service.ts) доставляет RabbitMQ и Kafka независимо. Аренды, `SKIP LOCKED`, подтверждение владельца аренды и повтор с прежним messageId сохраняются.

Модули Sales используют публичные прикладные порты, а состав зависимостей задаёт composition. SQL, HTTP, Redis и брокеры остаются в адаптерах; Checkout связывает локальные операции одной транзакцией. Payment mock, Fulfillment и Analytics владеют своими базами. Архитектурная проверка дополняет поведенческие тесты: она запрещает недопустимые зависимости ещё до запуска приложения.

Ограниченный срок брони и оплата развивают этот сценарий в задачах [9](docs/tasks/09-reservation-expiry.md) и [10](docs/tasks/10-payments.md). Здесь основа — блокировки, идемпотентность, локальная транзакция и outbox.

<a id="sql"></a>

## Каталог и SQL-запросы

Организатор управляет мероприятиями, покупатель читает свои заказы, а отчёты показывают продажи. JOIN, агрегаты и оконные функции собирают ответы с учётом владельца данных.

| Подзадача | Основное решение | Проверка |
| --- | --- | --- |
| Создать и изменить мероприятие, сохранив цены и реквизиты прежних покупок | [CatalogService](apps/sales/src/modules/catalog/application/catalog.service.ts), [PgCatalog](apps/sales/src/modules/catalog/infrastructure/pg-catalog.ts), [CheckoutService.purchase](apps/sales/src/modules/checkout/application/checkout.service.ts) | [Продукт](test/integration/product.test.cjs): создание и PATCH. Вручную: после PATCH сравнить `eventTitle` и `currentEventTitle` в списке заказов |
| Собрать ответ из нескольких таблиц через JOIN и ограничить его владельцем | [PgOrderQueries](apps/sales/src/modules/orders/infrastructure/pg-order-queries.ts), [PgCatalogQueries](apps/sales/src/modules/catalog/infrastructure/pg-catalog-queries.ts) | [Продукт](test/integration/product.test.cjs): список своих заказов, мероприятие без продаж и изоляция пользователей |
| Посчитать продажи, рейтинг и накопленную выручку через SQL-агрегаты и оконные функции | [SqlReportsRepository.events / daily](apps/analytics/src/modules/reports/infrastructure/sql-reports.repository.ts) | [Проверка Analytics](apps/analytics/scripts/verify.cjs): суммы, оконные отчёты и изоляция организаторов; [Lifecycle](test/integration/lifecycle.test.cjs): влияние возврата |

Отчёты читаются из базы Analytics. `events` применяет ранжирование SQL к агрегатам мероприятий, `daily` использует оконную сумму для накопленной выручки. Фильтр организатора применяется в SQL. Это отдельные проекции с собственными Kafka-группами и квитанциями обработки: работа одной не заменяет обработку другой.

SQL-запросы Sales соединяют таблицы только внутри своей базы. Заказ хранит снимок цены и реквизитов покупки; текущее название мероприятия читается отдельно. SQL отчётов принадлежит Analytics, а доставка данных для этих отчётов разобрана в [задаче 6](docs/tasks/06-kafka-replay.md).

<a id="kafka"></a>

## Kafka: независимые проекции и replay

Короткие решения: [6. Независимые отчёты и пересчёт истории Kafka](docs/tasks/06-kafka-replay.md).

Из истории покупок строятся две независимые проекции. Повторная доставка не увеличивает результат второй раз, а пересборка отчёта сохраняет продажи, поступающие во время replay.

| Подзадача | Основное решение | Проверка |
| --- | --- | --- |
| Построить две независимые проекции Kafka и не посчитать повторное событие дважды | [KafkaProjections](apps/analytics/src/modules/reports/infrastructure/kafka-projections.ts), [SqlReportsRepository.apply](apps/analytics/src/modules/reports/infrastructure/sql-reports.repository.ts) | [Проверка Analytics](apps/analytics/scripts/verify.cjs): независимые группы, дубликаты и неподтверждённое ошибочное событие; [Lifecycle](test/integration/lifecycle.test.cjs): обратный порядок ревизий |
| Пересобрать отчёты из истории Kafka, продолжая принимать продажи | [KafkaProjections.replay](apps/analytics/src/modules/reports/infrastructure/kafka-projections.ts), [поколения проекций](apps/analytics/src/modules/reports/infrastructure/sql-reports.repository.ts) | [Продукт](test/integration/product.test.cjs), [проверка Analytics](apps/analytics/scripts/verify.cjs): продажи во время replay, пустые партиции, сохранение прежнего отчёта при ошибке |

Replay строит новое поколение до зафиксированных offsets, затем переключает активный указатель. При ошибке прежний отчёт остаётся доступным. `all` выполняет два независимых перестроения. Требуются полная история Kafka и один управляющий процесс Analytics; подробности — в [README Analytics](apps/analytics/README.md).

Квитанция обработки и изменение проекции коммитятся до подтверждения Kafka offset. Каждая проекция использует собственную группу и поколение; остановка или пересборка одной проекции не заменяет обработку другой.

<a id="rabbitmq"></a>

## RabbitMQ: билеты, уведомления и повторы

Короткие решения: [5. Падение обработчика RabbitMQ](docs/tasks/05-rabbitmq-worker.md).

Подтверждённая покупка запускает независимый выпуск билетов и создание уведомления. Временный сбой приводит к повтору, ошибочное сообщение — в DLQ, а redrive возвращает исправленную работу в обработку.

| Подзадача | Основное решение | Проверка |
| --- | --- | --- |
| Выпустить билеты и создать уведомление независимыми обработчиками RabbitMQ без дубликатов | [FulfillOrder](apps/fulfillment/src/modules/delivery/application/fulfill-order.ts), [PostgresDeliveryRepository](apps/fulfillment/src/modules/delivery/infrastructure/postgres-delivery-repository.ts) | [Проверка Fulfillment](apps/fulfillment/src/modules/delivery/composition/verify.ts): дубликаты, проверка владельца, rollback и повтор |
| Повторить временно неудачную обработку, отправить ошибочное сообщение в DLQ и выполнить redrive | [RabbitWorkers](apps/fulfillment/src/modules/delivery/infrastructure/rabbit-workers.ts), [топология retry/DLQ](apps/fulfillment/src/modules/delivery/infrastructure/rabbit-topology.ts), [redrive](apps/fulfillment/src/modules/delivery/infrastructure/redrive.ts) | [Проверка Fulfillment](apps/fulfillment/src/modules/delivery/composition/verify.ts): некорректный JSON, конфликт messageId, исчерпание повторов и возврат в обработку |

[FulfillOrder](apps/fulfillment/src/modules/delivery/application/fulfill-order.ts) связывает квитанцию, проверку версии и эффект одной транзакцией. У обработчиков билетов и уведомлений независимый прогресс. Отмена делает билеты недействительными; уведомления различаются по назначению. ACK отправляется после коммита. Retry, DLQ и [redrive](apps/fulfillment/src/modules/delivery/infrastructure/redrive.ts) поддерживают оба формата событий.

<a id="redis"></a>

## Redis: сессии, лимиты, кеш и рейтинг

Короткие решения: [4. Общий лимит запросов](docs/tasks/04-rate-limit.md).

Несколько API используют общие сессии и лимиты. Каталог и рейтинг кешируются, восстанавливаются после потери ключей и сохраняют предусмотренное поведение при отказе Redis.

| Подзадача | Основное решение | Проверка |
| --- | --- | --- |
| Ограничить частоту входа и создания заказов сразу на нескольких API | [AuthService.limit](apps/sales/src/modules/identity-access/application/auth.service.ts), [RedisSessions.consumeLimit](apps/sales/src/modules/identity-access/infrastructure/redis-sessions.ts) | [Redis](test/integration/redis.test.cjs): конкурентные запросы используют общий лимит, TTL открывает новое окно |
| Кешировать каталог, обновлять кеш после записи и читать SQL при отказе Redis | [CatalogService](apps/sales/src/modules/catalog/application/catalog.service.ts), [RedisCatalog](apps/sales/src/modules/catalog/infrastructure/redis-catalog.ts) | [Redis](test/integration/redis.test.cjs): общий кеш, TTL, PATCH на другом API, отказ и восстановление |
| Хранить популярные мероприятия в Redis sorted set и восстановить рейтинг после потери кеша | [RedisRanking](apps/analytics/src/modules/reports/infrastructure/redis-ranking.ts), [ReportsService](apps/analytics/src/modules/reports/application/reports.service.ts) | [Проверка Analytics](apps/analytics/scripts/verify.cjs): удаление ключей, отказ Redis, SQL fallback и восстановление |

Сессии из [раздела авторизации](#identity) также хранятся в Redis: TTL ограничивает их жизнь, logout виден всем API. Сессии и лимиты требуют доступного хранилища; при его отказе защищённая операция получает `503`.

Каталог использует кеш с TTL и ревизией содержимого в PostgreSQL. Изменение мероприятия или остатка увеличивает ревизию в той же транзакции; старые ключи перестают быть актуальными для других API. При отказе Redis публичный каталог продолжает читать SQL. Здесь кеш необязателен, в отличие от хранилища сессий и лимитов.

Redis sorted set хранит рейтинг для выдачи, а источником данных остаётся PostgreSQL. Ревизия и состав кеша проверяются относительно SQL-снимка. Потерянный рейтинг восстанавливается; при недоступности Redis ответ строится из SQL. Поколение входит в ключ кеша, поэтому replay не смешивает старый рейтинг с новым.

<a id="pagination"></a>

## Пагинация по курсору

Короткие решения: [7. Пагинация по курсору](docs/tasks/07-pagination.md).

Каталог, мероприятия организатора и заказы читаются страницами. Курсор задаёт границу продолжения и учитывает владельца и изменение порядка каталога.

| Подзадача | Основное решение | Проверка |
| --- | --- | --- |
| Читать каталог и заказы по курсору без смещения страниц при новых записях | [PgOrderQueries.list](apps/sales/src/modules/orders/infrastructure/pg-order-queries.ts), [PgCatalogQueries.page](apps/sales/src/modules/catalog/infrastructure/pg-catalog-queries.ts) | [Lifecycle](test/integration/lifecycle.test.cjs): новые заказы между страницами, чужой и устаревший курсоры |

Заказы сортируются по неизменным `created_at DESC, id DESC`. Курсор сохраняет исходную точность PostgreSQL, поэтому строки с одинаковой датой различаются по id. Новый заказ перед первой страницей не сдвигает уже пройденную границу. Это не снимок всех строк на момент первого запроса.

Дата мероприятия может измениться. Поэтому у каталога отдельно существуют ревизия содержимого для кеша и ревизия порядка для курсора. Создание мероприятия или изменение даты требует нового обхода; изменение цены или остатка не меняет порядок. Кеш хранит страницы, учитывая ревизию, лимит и курсор.

<a id="edit-conflicts"></a>

## Защита от конфликтующих изменений

Короткие решения: [8. Защита от перезаписи изменений](docs/tasks/08-edit-conflicts.md).

Две вкладки не могут незаметно перезаписать изменения друг друга. PATCH требует ожидаемую version, а конфликт возвращает `409`.

| Подзадача | Основное решение | Проверка |
| --- | --- | --- |
| Не затереть изменение мероприятия из другой вкладки | [PgCatalog.update](apps/sales/src/modules/catalog/infrastructure/pg-catalog.ts) | [Lifecycle](test/integration/lifecycle.test.cjs): два PATCH с одной version; один `200`, другой `409` |

`version` мероприятия относится к редактированию организатором. Владелец и ожидаемая версия проверяются под блокировкой строки; вместимость сравнивается с занятыми местами. Версия увеличивается вместе с записью. Движение мест обновляет кеш, сохраняя версию редактирования.

<a id="reservation-expiry"></a>

## Бронирование и фоновое освобождение мест

Короткие решения: [9. Освобождение просроченной брони](docs/tasks/09-reservation-expiry.md).

Создание заказа резервирует места на десять минут, максимум до начала мероприятия. Просроченная бронь освобождается ровно один раз, в том числе при нескольких worker.

| Подзадача | Основное решение | Проверка |
| --- | --- | --- |
| Забронировать места на ограниченный срок и освободить их фоновым worker | [CheckoutService.purchase / expire](apps/sales/src/modules/checkout/application/checkout.service.ts), [Sales worker](apps/sales/src/bootstrap/worker.ts) | [Lifecycle](test/integration/lifecycle.test.cjs): истечение, попытка оплаты просроченной брони и два worker |

Checkout связывает публичные порты заказов, каталога, платежей и записи событий. Одна PostgreSQL-транзакция сохраняет остаток, бронь, локальный платёж и результат идемпотентности. Цена и реквизиты мероприятия фиксируются в заказе. Репозиторий заказа больше не изменяет таблицы каталога.

В [Sales worker](apps/sales/src/bootstrap/worker.ts) независимо работают истечение броней, платёжные операции, сверка и сбор эксплуатационных метрик. Сроки берутся из PostgreSQL. Worker обрабатывает каждый заказ отдельной транзакцией; повторная проверка под блокировкой исключает двойное освобождение мест. Срок также проверяется при оплате и обработке её результата.

<a id="payments"></a>

## Оплата, webhook и сверка результата

Короткие решения: [10. Оплата при потерянном ответе или webhook](docs/tasks/10-payments.md).

Оплата выполняется через отдельный локальный имитатор. Потерянный HTTP-ответ, повтор webhook или его отсутствие разрешаются повтором прежней операции и сверкой состояния провайдера.

| Подзадача | Основное решение | Проверка |
| --- | --- | --- |
| Провести оплату, если HTTP-ответ потерян или webhook не пришёл | [PaymentWorker.operation / sync](apps/sales/src/modules/payments/application/payment-worker.ts), [PgPayments](apps/sales/src/modules/payments/infrastructure/pg-payments.ts), [Payment mock](apps/payment-mock/src/modules/provider/infrastructure/pg-provider.ts) | [Lifecycle](test/integration/lifecycle.test.cjs): потерянный ответ, отсутствие и повтор webhook, проверка HMAC |

Оплата не входит в транзакцию PostgreSQL. [PgPayments.claim](apps/sales/src/modules/payments/infrastructure/pg-payments.ts) выдаёт временную аренду задания, затем HTTP-адаптер обращается к провайдеру. После неизвестного исхода повторяется тот же id. Сверка продолжает опрашивать незавершённые платежи, даже если заказ уже отменён. Позднее списание приводит к компенсации, а не к восстановлению билетов.

[Payment mock](apps/payment-mock/src/modules/provider/infrastructure/pg-provider.ts) сохраняет платежи и webhook в собственной базе. [WebhooksController](apps/sales/src/modules/payments/presentation/webhooks.controller.ts) проверяет HMAC по raw body, допустимый возраст подписи, идентификатор и соответствие платежа заказу. Повтор с тем же содержимым безопасен; тот же eventId с другим содержимым получает конфликт. После коммита квитанции начинается сверка актуального состояния провайдера.

<a id="cancellation"></a>

## Отмена заказа и возврат денег

Короткие решения: [11. Отмена заказа и возврат денег](docs/tasks/11-cancellation.md).

До начала мероприятия покупатель может отменить заказ. Места освобождаются при отмене, деньги возвращаются асинхронно. Позднее списание и события в обратном порядке не восстанавливают отменённую покупку.

| Подзадача | Основное решение | Проверка |
| --- | --- | --- |
| Отменить заказ и вернуть деньги, в том числе после позднего списания | [CheckoutService.cancel / settle](apps/sales/src/modules/checkout/application/checkout.service.ts) | [Lifecycle](test/integration/lifecycle.test.cjs): позднее списание, временный отказ возврата, повтор отмены, события в обратном порядке |
| Обновить схему и старые данные без сброса покупок, повторить прерванное обновление | [migrate](scripts/migrate.cjs), [provision](scripts/provision.cjs), [upgrade-state](scripts/upgrade-state.cjs) | [Upgrade](test/integration/upgrade.test.cjs): переход с миграций `001`, сохранение билетов, повтор импорта и возврат старой покупки |

[parseOrderEvent](packages/contracts/src/index.ts) принимает исторический `order.confirmed v1` и новые события `v2`. `version` задаёт формат сообщения; `orderRevision` задаёт последовательность состояний конкретного заказа. Это разные версии.

Новый снимок содержит неизменные данные покупки и известные даты подтверждения, отмены и возврата. Поэтому получатель может применить возврат, пришедший раньше подтверждения. Старое состояние после этого не восстанавливает продажу. Отпечаток бизнес-события не зависит от транспортных trace-заголовков.

[SqlReportsRepository.apply](apps/analytics/src/modules/reports/infrastructure/sql-reports.repository.ts) хранит состояние каждого заказа внутри поколения и меняет агрегаты на разницу старого и нового вклада. В `events` число билетов относится к действующим заказам. В `daily` сохраняется день исходной продажи, а завершённый возврат учитывается отдельно в день возврата. Поля выручки не смешиваются с количеством действующих мест.

SQL-миграции применяются последовательно, под advisory lock, с сохранением checksum. Уже применённые файлы не редактируются. Отдельный `upgrade:state` при остановленных приложениях переносит старые учебные платежи и перестраивает старый формат отчётов. Идентификаторы импорта сохраняются, поэтому прерывание и повтор команды не создают второй платёж.

<a id="csv-export"></a>

## Потоковая выгрузка CSV

Короткие решения: [12. Большой отчёт без загрузки целиком в память](docs/tasks/12-csv-export.md).

Организатор скачивает большой отчёт через Analytics и Sales без загрузки всей выгрузки в память. Поток сохраняет согласованный снимок и прекращает чтение при отключении клиента.

| Подзадача | Основное решение | Проверка |
| --- | --- | --- |
| Скачать большой CSV без загрузки всего отчёта в память | [SqlOrderExports](apps/analytics/src/modules/reports/infrastructure/sql-order-exports.ts), [SqlRowStream / streamCsv](packages/core/src/streaming.ts) | [Lifecycle](test/integration/lifecycle.test.cjs): 100000 строк, смена поколения при replay, права, экранирование формул и разрыв соединения |

CSV читает строки через отдельный пул из двух соединений и SQL-курсор порциями по 500. Пока клиент не готов принять следующую часть, цепочка не запрашивает неограниченный объём. Снимок фиксирует поколение и строки для всей выгрузки. Таймаут первого ответа — 5 секунд, простоя — 30 секунд, всей выгрузки — 5 минут. После начала передачи ошибка обрывает поток; до начала остаётся HTTP-ошибкой.

<a id="observability"></a>

## Трассировка, метрики и диагностика

Короткие решения: [13. Поиск задержки покупки между сервисами](docs/tasks/13-observability.md).

Покупку можно проследить через HTTP, платёжное задание, webhook и оба брокера. Метрики показывают задержки и незавершённую работу, а health-проверки помогают отличить отказ сервиса от ожидания результата.

| Подзадача | Основное решение | Проверка |
| --- | --- | --- |
| Различать ожидание фонового результата, ошибку обработки и недоступность сервиса | [ProductQueriesService](apps/sales/src/modules/integrations/application/product-queries.service.ts), [HttpProductQueries](apps/sales/src/modules/integrations/infrastructure/http-product-queries.ts), [readiness](apps/sales/src/modules/integrations/infrastructure/readiness.ts) | [Unit-проверки запросов](apps/sales/src/tests/product-queries.test.ts), [состояния доставки](apps/fulfillment/src/modules/delivery/domain/delivery.test.ts): `pending`, `completed`, `failed` и отдельный `503`. Вручную: `/health/ready` и `/health/workers` |
| Проследить покупку между процессами и измерить задержки фоновых задач | [observability](packages/observability/src/index.ts), [Collector](infra/observability/collector.yaml), [метрики работы Sales](apps/sales/src/modules/messaging/infrastructure/pg-work-metrics.ts) | [Observability](test/integration/observability.test.cjs): spans через оба брокера, метрики Prometheus и недоступный Collector |

Контекст traces сохраняется в outbox, платёжных заданиях и webhook. Обычная обработка продолжает исходный trace; повтор webhook и replay создают связанный trace. Бизнес-слои получают функции выполнения в контексте через композицию и не импортируют OpenTelemetry SDK. На dashboard используются метрики с ограниченным набором меток; orderId остаётся в traces и логах.

Работающий HTTP-процесс и готовый фоновый обработчик — разные состояния. `/health/live` отвечает за доступность процесса, `/health/ready` — за необходимые зависимости, `/health/workers` у получателей — за состояние обработки. У заказа отдельно видны ожидаемые, выполненные и неудачные фоновые эффекты. Потеря связи с Fulfillment возвращает `503`, поскольку из сетевой ошибки нельзя узнать, выпущены ли билеты.

<a id="background"></a>

## Где работает фон

Фоновая обработка охватывает несколько самостоятельных механизмов:

- Sales relay забирает сохранённые события outbox и доставляет каждое направление независимо.
- Обработчики Fulfillment читают RabbitMQ, выпускают билеты, создают уведомления и выполняют повторы через retry-очереди.
- Обработчики Analytics читают Kafka отдельными группами, обновляют проекции и восстанавливают Redis-рейтинг. Replay запускает пересборку из истории.
- Sales worker освобождает просроченные брони, выполняет платёжные задания, сверяет незавершённые платежи и собирает метрики.
- Payment mock завершает отложенные платежи и возвраты, доставляет подписанные webhook с повторами.

Состояние работы, требующей восстановления после сбоя, хранится в базе или брокере. Таймеры процессов будят обработчики; они не заменяют устойчивую запись задания. Проверка состояния и повтор после перезапуска нужны даже при наличии аренды, поскольку процесс может завершиться между внешним эффектом и его локальным подтверждением.

<a id="verification"></a>

## Как проверять решения

`pnpm check` выполняет сборку, проверку архитектурных границ и unit-тесты. `pnpm test` дополнительно запускает интеграционные сценарии и отдельные проверки Fulfillment и Analytics на локальной инфраструктуре. В таблицах указаны конкретные проверки; ручные сценарии отмечены отдельно.
