import { startHttp, log } from '@ticketing/core';
import { providerComposition } from '../modules/provider/composition/provider';
async function main() {
  const runtime = providerComposition();
  await startHttp(runtime.module, runtime.port, 'payment-mock');
  runtime.start();
}
void main().catch((error) => {
  log('provider.failed', { error: String(error) });
  process.exitCode = 1;
});
