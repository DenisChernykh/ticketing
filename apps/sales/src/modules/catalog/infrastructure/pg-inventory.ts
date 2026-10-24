import { AppError, Database } from '@ticketing/core';
import type { Inventory, InventorySnapshot } from '../application/inventory';
/** Called only inside the checkout transaction; catalog owns every inventory write. */
export class PgInventory implements Inventory {
  constructor(private readonly db: Database) {}
  async lock(id: string) {
    await this.db.query('SELECT id FROM events WHERE id=$1 FOR UPDATE', [id]);
  }
  async hold(id: string, quantity: number, now: string): Promise<InventorySnapshot> {
    const e = (
      await this.db.query(
        `SELECT id,organizer_id AS "organizerId",title,venue,starts_at AS "startsAt",unit_price AS "unitPrice",currency,available FROM events WHERE id=$1 FOR UPDATE`,
        [id],
      )
    ).rows[0];
    if (!e) throw new AppError('EVENT_NOT_FOUND', 'Мероприятие не найдено', 404);
    if (new Date(e.startsAt).getTime() <= Date.parse(now))
      throw new AppError('EVENT_STARTED', 'Продажа закончена', 409);
    if (e.available < quantity) throw new AppError('NOT_ENOUGH_TICKETS', 'Недостаточно мест', 409);
    await this.db.query('UPDATE events SET available=available-$2,updated_at=now() WHERE id=$1', [
      id,
      quantity,
    ]);
    await this.db.query('UPDATE catalog_revision SET revision=revision+1 WHERE id=true');
    return { ...e, startsAt: new Date(e.startsAt).toISOString() };
  }
  async release(id: string, quantity: number) {
    await this.db.query('UPDATE events SET available=available+$2,updated_at=now() WHERE id=$1', [
      id,
      quantity,
    ]);
    await this.db.query('UPDATE catalog_revision SET revision=revision+1 WHERE id=true');
  }
}
