const { test } = require('node:test');
const assert = require('node:assert/strict');
const { randomUUID } = require('node:crypto');
const { parseOrderConfirmed, rabbitNames } = require('@ticketing/contracts');

const valid = () => ({ messageId: randomUUID(), type: 'order.confirmed', version: 1,
  occurredAt: new Date().toISOString(), order: { id: randomUUID(), buyerId: randomUUID(), organizerId: randomUUID(),
    eventId: randomUUID(), eventTitle: 'Концерт', venue: 'Зал', startsAt: new Date().toISOString(),
    quantity: 2, unitPrice: 50000, totalPrice: 100000, currency: 'RUB' } });
test('consumer rejects unsupported versions and inconsistent money before persistence', () => {
  assert.throws(() => parseOrderConfirmed({ ...valid(), version: 2 }));
  const message = valid(); message.order.totalPrice = 1;
  assert.throws(() => parseOrderConfirmed(message));
});
test('wire parsing preserves stable IDs and strips unknown fields', () => {
  const message = valid();
  assert.deepEqual(parseOrderConfirmed({ ...message, private: 'ignored' }), message);
});
test('resource namespace cannot select another routing tree', () => {
  assert.throws(() => rabbitNames('../other'));
  assert.equal(rabbitNames('ticketing_test').tickets, 'ticketing_test.tickets');
});
