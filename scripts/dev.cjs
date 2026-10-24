const { spawn } = require('node:child_process');
const { resolve } = require('node:path');
const { config } = require('@ticketing/core');

const entries = {
  sales: 'apps/sales/dist/bootstrap/api.js',
  relay: 'apps/sales/dist/bootstrap/relay.js',
  worker: 'apps/sales/dist/bootstrap/worker.js',
  'payment-mock': 'apps/payment-mock/dist/bootstrap/main.js',
  fulfillment: 'apps/fulfillment/dist/bootstrap/main.js',
  analytics: 'apps/analytics/dist/bootstrap/main.js',
};
const children = [];
let stopping = false;
function stop(code) {
  if (stopping) return;
  stopping = true;
  process.exitCode = code;
  for (const child of children) if (child.exitCode === null) child.kill('SIGTERM');
  const deadline = setTimeout(() => {
    for (const child of children) if (child.exitCode === null) child.kill('SIGKILL');
  }, 10000);
  deadline.unref();
}
for (const [name, file] of Object.entries(entries)) {
  const child = spawn(process.execPath, [resolve(__dirname, '..', file)], {
    cwd: resolve(__dirname, '..'),
    env: process.env,
    stdio: 'inherit',
  });
  children.push(child);
  child.on('error', (error) => {
    console.error(`${name}: ${error.message}`);
    stop(1);
  });
  child.on('exit', (code, signal) => {
    if (!stopping) {
      console.error(`${name} exited: ${code ?? signal}`);
      stop(code || 1);
    }
  });
}
process.on('SIGINT', () => stop(0));
process.on('SIGTERM', () => stop(0));
const settings = config();
console.log(`Swagger: http://127.0.0.1:${settings.salesPort}/docs`);
