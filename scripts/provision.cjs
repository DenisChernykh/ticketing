const { Pool } = require('pg');
const { config } = require('@ticketing/core');
const services = [
  ['sales', 'sales'],
  ['fulfillment', 'fulfillment'],
  ['analytics', 'analytics'],
  ['payment_mock', 'paymentMock'],
];
async function provision(settings = config()) {
  const url = new URL(settings.salesDatabaseUrl);
  url.username = 'ticketing_admin';
  url.password = 'ticketing-admin-local';
  url.pathname = '/postgres';
  const db = new Pool({ connectionString: process.env.TEST_ADMIN_DATABASE_URL ?? url.toString() });
  try {
    for (const [role, key] of services) {
      if (!(await db.query('SELECT 1 FROM pg_roles WHERE rolname=$1', [role])).rowCount)
        await db.query(`CREATE ROLE "${role}" LOGIN PASSWORD '${role}-local'`);
      const name = new URL(settings[key + 'DatabaseUrl']).pathname.slice(1);
      if (!/^[a-z0-9_]+$/.test(name)) throw new Error('Invalid database name');
      if (!(await db.query('SELECT 1 FROM pg_database WHERE datname=$1', [name])).rowCount)
        await db.query(`CREATE DATABASE "${name}" OWNER "${role}"`);
      await db.query(`REVOKE CONNECT ON DATABASE "${name}" FROM PUBLIC`);
      await db.query(`GRANT CONNECT ON DATABASE "${name}" TO "${role}"`);
    }
  } finally {
    await db.end();
  }
}
module.exports = { provision };
if (require.main === module)
  provision().catch((e) => {
    console.error(e);
    process.exitCode = 1;
  });
