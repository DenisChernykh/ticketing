import 'reflect-metadata';
import { log, startHttp } from '@ticketing/core';
import { createSalesApi } from '../composition/sales';

async function main() {
  const runtime = await createSalesApi();
  try { await startHttp(runtime.module, runtime.port, 'Ticketing — Sales'); }
  catch (error) { await runtime.close(); throw error; }
}
void main().catch(() => { log('sales.api.failed'); process.exitCode = 1; });
