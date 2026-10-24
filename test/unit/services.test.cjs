const { readdirSync } = require('node:fs');
const { join, resolve } = require('node:path');
function load(directory) {
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    const file = join(directory, entry.name);
    if (entry.isDirectory()) load(file);
    else if (file.endsWith('.test.js')) require(file);
  }
}
for (const service of ['sales', 'fulfillment', 'analytics', 'payment-mock'])
  load(resolve(__dirname, `../../apps/${service}/dist`));
