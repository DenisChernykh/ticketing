import { equal } from 'node:assert/strict';
import { test } from 'node:test';
import { deliveryStatus } from './delivery';

test('independent handlers keep an order pending until both have committed', () => {
  equal(deliveryStatus(2, 0, 0, []), 'pending');
  equal(deliveryStatus(2, 2, 0, []), 'pending');
  equal(deliveryStatus(2, 0, 1, []), 'pending');
  equal(deliveryStatus(2, 2, 1, []), 'completed');
});

test('only an unresolved handler failure makes the order failed', () => {
  equal(deliveryStatus(2, 0, 1, ['tickets']), 'failed');
  equal(deliveryStatus(2, 2, 0, ['tickets']), 'pending');
  equal(deliveryStatus(2, 2, 1, ['tickets', 'notifications']), 'completed');
});
