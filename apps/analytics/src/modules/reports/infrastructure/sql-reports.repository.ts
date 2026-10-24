import { parseOrderEvent, type OrderEvent } from '@ticketing/contracts';
import { randomUUID } from 'node:crypto';
import { Database } from '@ticketing/core';
import type { ReportsRepository } from '../application/ports';
import {
  InvalidSaleEvent,
  type DailyReport,
  type EventReport,
  type Projection,
  type RankingSnapshot,
  type Sale,
} from '../domain/reports';

/**
 * Владеет SQL-состоянием двух проекций и их поколений в БД Analytics.
 *
 * @remarks
 * Все запросы чтения выбирают поколение через projection_active. Replay пишет
 * в отдельные строки с другим generation и становится видимым только после
 * переключения указателя. Таблицы старых поколений остаются для диагностики.
 * Прикладной код не получает соединение pg: Database.run связывает запросы
 * одной транзакции с одним клиентом.
 */
export class SqlReportsRepository implements ReportsRepository {
  constructor(private readonly database: Database) {}

  /**
   * Создаёт первоначальные активные поколения и отмечает прерванный replay.
   *
   * @remarks
   * Advisory lock сериализует именно эту стартовую транзакцию. Это не блокировка
   * владельца runtime на всё время работы: сервис рассчитан на один процесс
   * Analytics. Поэтому оставшееся после перезапуска building считается работой
   * прежнего процесса и переводится в failed; уже активные данные сохраняются.
   */
  async initialize(): Promise<void> {
    await this.database.run(async () => {
      await this.database.query("SELECT pg_advisory_xact_lock(hashtext('analytics-initialize'))");
      // При модели с одним runtime незавершённый replay принадлежал прежнему процессу.
      await this.database.query(
        "UPDATE projection_generations SET status = 'failed', failure = 'Process restarted before activation' WHERE status = 'building'",
      );
      for (const projection of ['events', 'daily'] as const) {
        const existing = await this.database.query(
          'SELECT generation FROM projection_active WHERE projection = $1',
          [projection],
        );
        if (existing.rowCount) continue;
        const generation = randomUUID();
        await this.database.query(
          "INSERT INTO projection_generations (projection, generation, status, activated_at) VALUES ($1, $2, 'active', now())",
          [projection, generation],
        );
        await this.database.query('INSERT INTO projection_active (projection, generation) VALUES ($1, $2)', [
          projection,
          generation,
        ]);
      }
    });
  }

  /**
   * Читает долговременный указатель поколения, чтобы восстановить его же группу.
   * Он остаётся источником истины и при потере ответа на COMMIT переключения.
   */
  async active(projection: Projection): Promise<string> {
    const result = await this.database.query<{ generation: string }>(
      'SELECT generation FROM projection_active WHERE projection = $1',
      [projection],
    );
    if (!result.rows[0]) throw new Error(`Missing active ${projection} projection`);
    return result.rows[0].generation;
  }

  /**
   * Выделяет UUID нового building-поколения без изменения публичных отчётов.
   * Этот UUID также входит в имя новой consumer group и область дедупликации.
   */
  async createGeneration(projection: Projection): Promise<string> {
    const generation = randomUUID();
    await this.database.query(
      "INSERT INTO projection_generations (projection, generation, status) VALUES ($1, $2, 'building')",
      [projection, generation],
    );
    return generation;
  }

  /**
   * Атомарно делает полностью построенное поколение видимым читателям.
   *
   * @remarks
   * Вызывающий runtime уже должен достичь зафиксированных offsets всех партиций.
   * Метод не проверяет Kafka: он блокирует строку указателя, проверяет building,
   * переводит предыдущее поколение в retired и меняет active в одной транзакции.
   * Один SQL-запрос отчёта поэтому видит целиком старое либо новое поколение.
   * Переключения events и daily независимы даже при команде replay all.
   */
  async activate(projection: Projection, generation: string): Promise<void> {
    await this.database.run(async () => {
      await this.database.query('SELECT generation FROM projection_active WHERE projection = $1 FOR UPDATE', [
        projection,
      ]);
      const next = await this.database.query(
        "UPDATE projection_generations SET status = 'active', activated_at = now() WHERE projection = $1 AND generation = $2 AND status = 'building' RETURNING generation",
        [projection, generation],
      );
      if (next.rowCount !== 1) throw new Error('Replay generation cannot be activated');
      await this.database.query(
        "UPDATE projection_generations SET status = 'retired' WHERE projection = $1 AND status = 'active' AND generation <> $2",
        [projection, generation],
      );
      await this.database.query('UPDATE projection_active SET generation = $2 WHERE projection = $1', [
        projection,
        generation,
      ]);
    });
  }

  /**
   * Сохраняет причину ошибки незавершённого поколения, не меняя active pointer.
   * Условие status=building защищает уже активированное поколение, если ответ
   * на его успешный COMMIT не дошёл до вызывающего процесса.
   */
  async failGeneration(projection: Projection, generation: string, failure: string): Promise<void> {
    await this.database.query(
      "UPDATE projection_generations SET status = 'failed', failure = $3 WHERE projection = $1 AND generation = $2 AND status = 'building'",
      [projection, generation, failure.slice(0, 1000)],
    );
  }

  /**
   * Атомарно сохраняет событие и изменяет вклад заказа в конкретном поколении.
   * Квитанция защищает messageId и пару orderId/revision от подмены. Advisory lock
   * сериализует первые и последующие ревизии одного заказа, включая конкурирующие
   * вставки. Устаревшее состояние получает квитанцию, но не меняет итог.
   *
   * Снимок новой ревизии содержит всю известную историю переходов. Поэтому возврат
   * можно применить раньше подтверждения: gross и refund восстанавливаются вместе.
   * events считает действующие билеты; daily хранит продажу в день подтверждения,
   * возврат — в день его завершения. Изменения вычисляются как новый вклад минус старый.
   * В этой транзакции сохраняются квитанция, снимок, агрегаты и revision поколения.
   * Kafka offset подтверждается вызывающим адаптером только после коммита.
   */
  async apply(projection: Projection, generation: string, sale: Sale, fingerprint: string): Promise<void> {
    await this.database.run(async () => {
      // Serialize revisions of one order, including concurrent first insertions.
      await this.database.query('SELECT pg_advisory_xact_lock(hashtextextended($1,0))', [
        projection + generation + sale.order.id,
      ]);
      const receipt = await this.database.query(
        `INSERT INTO message_receipts(projection,generation,message_id,order_id,order_revision,fingerprint) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING RETURNING message_id`,
        [projection, generation, sale.messageId, sale.order.id, sale.orderRevision, fingerprint],
      );
      if (!receipt.rowCount) {
        const rows = (
          await this.database.query(
            `SELECT message_id,fingerprint FROM message_receipts WHERE projection=$1 AND generation=$2 AND (message_id=$3 OR (order_id=$4 AND order_revision=$5))`,
            [projection, generation, sale.messageId, sale.order.id, sale.orderRevision],
          )
        ).rows;
        if (rows.length !== 1 || rows[0].message_id !== sale.messageId || rows[0].fingerprint !== fingerprint)
          throw new InvalidSaleEvent('Conflicting message or order revision');
        return;
      }
      const prior = (
        await this.database.query(
          'SELECT revision,snapshot FROM projection_orders WHERE projection=$1 AND generation=$2 AND order_id=$3',
          [projection, generation, sale.order.id],
        )
      ).rows[0];
      if (prior && prior.revision >= sale.orderRevision) return;
      const old: OrderEvent | undefined = prior ? parseOrderEvent(prior.snapshot) : undefined;
      if (old && JSON.stringify(old.order) !== JSON.stringify(sale.order))
        throw new InvalidSaleEvent('Order snapshot changed');
      const contribution = (event: OrderEvent | undefined) => {
        const confirmed = !!event?.state.confirmedAt,
          active = confirmed && event?.state.status === 'confirmed',
          gross = confirmed ? event!.order.totalPrice : 0,
          refund = confirmed && event?.state.refundedAt ? event.order.totalPrice : 0;
        return {
          orders: active ? 1 : 0,
          tickets: active ? event!.order.quantity : 0,
          gross,
          refund,
          revenue: gross - refund,
        };
      };
      if (projection === 'events') {
        const before = contribution(old),
          after = contribution(sale);
        if (old?.state.confirmedAt || sale.state.confirmedAt) {
          const changed = await this.database.query(
            `INSERT INTO event_sales(generation,organizer_id,event_id,title,orders,tickets,revenue,gross_revenue,refunds) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT(generation,event_id) DO UPDATE SET orders=event_sales.orders+EXCLUDED.orders,tickets=event_sales.tickets+EXCLUDED.tickets,revenue=event_sales.revenue+EXCLUDED.revenue,gross_revenue=event_sales.gross_revenue+EXCLUDED.gross_revenue,refunds=event_sales.refunds+EXCLUDED.refunds WHERE event_sales.organizer_id=EXCLUDED.organizer_id RETURNING event_id`,
            [
              generation,
              sale.order.organizerId,
              sale.order.eventId,
              sale.order.eventTitle,
              after.orders - before.orders,
              after.tickets - before.tickets,
              after.revenue - before.revenue,
              after.gross - before.gross,
              after.refund - before.refund,
            ],
          );
          if (!changed.rowCount) throw new InvalidSaleEvent('Event owner changed');
        }
      } else {
        const days = new Map<string, { orders: number; tickets: number; gross: number; refund: number }>();
        const add = (event: OrderEvent | undefined, sign: number) => {
          if (!event?.state.confirmedAt) return;
          const change = (time: string, o: number, t: number, g: number, r: number) => {
            const day = new Date(time).toISOString().slice(0, 10),
              v = days.get(day) ?? { orders: 0, tickets: 0, gross: 0, refund: 0 };
            v.orders += o * sign;
            v.tickets += t * sign;
            v.gross += g * sign;
            v.refund += r * sign;
            days.set(day, v);
          };
          change(event.state.confirmedAt, 1, event.order.quantity, event.order.totalPrice, 0);
          if (event.state.refundedAt) change(event.state.refundedAt, 0, 0, 0, event.order.totalPrice);
        };
        add(old, -1);
        add(sale, 1);
        for (const [day, d] of days) {
          if (!d.orders && !d.tickets && !d.gross && !d.refund) continue;
          await this.database.query(
            `INSERT INTO daily_revenue(generation,organizer_id,day,orders,tickets,revenue,gross_revenue,refunds) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(generation,organizer_id,day) DO UPDATE SET orders=daily_revenue.orders+EXCLUDED.orders,tickets=daily_revenue.tickets+EXCLUDED.tickets,revenue=daily_revenue.revenue+EXCLUDED.revenue,gross_revenue=daily_revenue.gross_revenue+EXCLUDED.gross_revenue,refunds=daily_revenue.refunds+EXCLUDED.refunds`,
            [
              generation,
              sale.order.organizerId,
              day,
              d.orders,
              d.tickets,
              d.gross - d.refund,
              d.gross,
              d.refund,
            ],
          );
        }
      }
      await this.database.query(
        `INSERT INTO projection_orders(projection,generation,order_id,organizer_id,event_id,confirmed_at,revision,snapshot) VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb) ON CONFLICT(projection,generation,order_id) DO UPDATE SET revision=EXCLUDED.revision,snapshot=EXCLUDED.snapshot,confirmed_at=EXCLUDED.confirmed_at`,
        [
          projection,
          generation,
          sale.order.id,
          sale.order.organizerId,
          sale.order.eventId,
          sale.state.confirmedAt,
          sale.orderRevision,
          JSON.stringify(sale),
        ],
      );
      await this.database.query(
        'UPDATE projection_generations SET revision=revision+1 WHERE projection=$1 AND generation=$2',
        [projection, generation],
      );
    });
  }

  /**
   * Строит SQL-рейтинг проданных билетов внутри одного организатора.
   *
   * @remarks
   * Первый CTE выбирает только активное поколение и organizerId, второй назначает
   * ROW_NUMBER по tickets DESC и event_id DESC. При равных продажах места разные,
   * но порядок воспроизводим и совпадает с обратным чтением Redis sorted set.
   *
   * Здесь есть только мероприятия, по которым получены продажи; title взят
   * из сохранённого события, а не из текущего каталога Sales. Для каталога,
   * включающего нулевые продажи, используется LEFT JOIN в PgCatalog.listOrganizer
   * внутри Sales. Соединение таблиц разных сервисов не требуется.
   */
  async events(organizerId: string): Promise<EventReport[]> {
    const result = await this.database.query<EventReport>(
      `
      WITH organizer_sales AS (
        SELECT s.* FROM event_sales s
        INNER JOIN projection_active a ON a.projection = 'events' AND a.generation = s.generation
        WHERE s.organizer_id = $1
      ), ranked AS (
        SELECT event_id AS "eventId", title, tickets::float8 AS tickets, revenue::float8 AS revenue,gross_revenue::float8 AS "grossRevenue",refunds::float8 AS refunds,
          ROW_NUMBER() OVER (ORDER BY tickets DESC, event_id DESC)::int AS rank
        FROM organizer_sales
      ) SELECT * FROM ranked ORDER BY rank
    `,
      [organizerId],
    );
    return result.rows;
  }

  /**
   * Возвращает дневные итоги и накопленную выручку одного организатора.
   *
   * WHERE ограничивает организатора до вычисления окна SUM OVER. Рамка ROWS
   * от начала до текущей строки делает cumulativeRevenue суммой выручки всех
   * предыдущих дней и текущего дня. Дни без продаж не дополняются нулевыми
   * строками; денежные суммы остаются копейками, а day возвращается датой UTC.
   */
  async daily(organizerId: string): Promise<DailyReport[]> {
    const result = await this.database.query<DailyReport>(
      `
      SELECT to_char(d.day, 'YYYY-MM-DD') AS day, d.orders::float8 AS orders,
        d.tickets::float8 AS tickets, d.revenue::float8 AS revenue,d.gross_revenue::float8 AS "grossRevenue",d.refunds::float8 AS refunds,
        (SUM(d.revenue) OVER (ORDER BY d.day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))::float8 AS "cumulativeRevenue"
      FROM daily_revenue d
      INNER JOIN projection_active a ON a.projection = 'daily' AND a.generation = d.generation
      WHERE d.organizer_id = $1 ORDER BY d.day
    `,
      [organizerId],
    );
    return result.rows;
  }

  /**
   * Получает поколение, revision и полный рейтинг одним SQL-снимком.
   *
   * @remarks
   * Если читать указатель, revision и строки отдельными запросами, между ними
   * может пройти новая продажа или переключение replay. Один запрос связывает
   * значения одним снимком PostgreSQL и даёт Redis-адаптеру проверяемую версию.
   * CTE active выбирает поколение, ranked ограничивает organizerId и нумерует
   * строки, jsonb_agg сохраняет их порядок. COALESCE возвращает пустой массив,
   * даже когда у организатора ещё нет продаж, сохраняя generation и revision.
   */
  async rankingSnapshot(organizerId: string): Promise<RankingSnapshot> {
    const result = await this.database.query<RankingSnapshot>(
      `
      WITH active AS (
        SELECT g.generation, g.revision FROM projection_active a
        INNER JOIN projection_generations g USING (projection, generation)
        WHERE a.projection = 'events'
      ), ranked AS (
        SELECT s.event_id AS "eventId", s.title, s.tickets::float8 AS tickets,
          ROW_NUMBER() OVER (ORDER BY s.tickets DESC, s.event_id DESC)::int AS rank
        FROM event_sales s INNER JOIN active a USING (generation)
        WHERE s.organizer_id = $1
      )
      SELECT a.generation, a.revision::text AS revision,
        COALESCE((SELECT jsonb_agg(r ORDER BY r.rank) FROM ranked r), '[]'::jsonb) AS items
      FROM active a
    `,
      [organizerId],
    );
    if (!result.rows[0]) throw new Error('The events projection is not initialized');
    return result.rows[0];
  }

  /** Перечисляет организаторов активного поколения для полного ремонта Redis. */
  async organizers(): Promise<string[]> {
    const result = await this.database.query<{ organizer_id: string }>(`
      SELECT DISTINCT s.organizer_id FROM event_sales s
      INNER JOIN projection_active a ON a.projection = 'events' AND a.generation = s.generation
    `);
    return result.rows.map((row) => row.organizer_id);
  }

  /** Проверяет доступность собственной PostgreSQL без зависимости от брокера. */
  async formatReady(projection: Projection) {
    const row = (
      await this.database.query(
        `SELECT g.format_version FROM projection_active a JOIN projection_generations g USING(projection,generation) WHERE a.projection=$1`,
        [projection],
      )
    ).rows[0];
    if (row?.format_version !== 2) throw new Error('PROJECTION_UPGRADE_REQUIRED: run upgrade:state');
  }
  async health(): Promise<void> {
    await this.database.query('SELECT 1');
  }
}
