const { Database, config } = require('@ticketing/core');
const { hashPassword } = require('../apps/sales/dist/modules/identity-access/infrastructure/password.js');

const DEMO = {
  password: 'local-password',
  organizer: { id: '10000000-0000-4000-8000-000000000001', email: 'organizer@example.test', role: 'organizer' },
  otherOrganizer: { id: '10000000-0000-4000-8000-000000000002', email: 'other-organizer@example.test', role: 'organizer' },
  buyer: { id: '20000000-0000-4000-8000-000000000001', email: 'buyer@example.test', role: 'buyer' },
  otherBuyer: { id: '20000000-0000-4000-8000-000000000002', email: 'other-buyer@example.test', role: 'buyer' },
  eventId: '30000000-0000-4000-8000-000000000001',
  emptyEventId: '30000000-0000-4000-8000-000000000002',
};

async function seed(url = config().salesDatabaseUrl) {
  const db = new Database(url, 'sales-seed');
  try {
    const hash = await hashPassword(DEMO.password);
    await db.run(async () => {
      for (const user of [DEMO.organizer, DEMO.otherOrganizer, DEMO.buyer, DEMO.otherBuyer]) {
        await db.query(`INSERT INTO users(id,email,password_hash,role) VALUES($1,$2,$3,$4)
          ON CONFLICT(id) DO NOTHING`, [user.id, user.email, hash, user.role]);
      }
      for (const event of [
        { id: DEMO.eventId, title: 'Вечер живой музыки', capacity: 100, price: 250000 },
        { id: DEMO.emptyEventId, title: 'Лекция о космосе', capacity: 50, price: 80000 },
      ]) await db.query(`INSERT INTO events(id,organizer_id,title,venue,starts_at,capacity,available,unit_price)
        VALUES($1,$2,$3,'Большой зал','2027-12-20T15:00:00Z',$4,$4,$5) ON CONFLICT(id) DO NOTHING`,
        [event.id, DEMO.organizer.id, event.title, event.capacity, event.price]);
      await db.query('UPDATE catalog_revision SET revision=revision+1 WHERE id=true');
    });
  } finally { await db.close(); }
  return DEMO;
}
module.exports = { seed, DEMO };
if (require.main === module) seed().then(() => console.log('Demo ready: organizer@example.test / buyer@example.test; password: local-password'))
  .catch((error) => { console.error(error); process.exitCode = 1; });
