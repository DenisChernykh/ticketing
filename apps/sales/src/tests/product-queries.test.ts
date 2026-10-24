import { test } from 'node:test';
import assert from 'node:assert/strict';
import { AppError } from '@ticketing/core';
import { ProductQueriesService } from '../modules/integrations/application/product-queries.service';
import type { OrderOwnership, ProductQueries } from '../modules/integrations/application/ports';
import type { FulfillmentResult } from '../modules/integrations/domain/delivery';
import { ProductQueryUuidPipe, ReportLimitPipe } from '../modules/integrations/presentation/product-query.validation';

const orderId = '11111111-1111-4111-8111-111111111111';
const buyerId = '22222222-2222-4222-8222-222222222222';

test('a denied order owner never causes a Fulfillment request and retains the original failure', async () => {
  const denied = new AppError('ORDER_NOT_FOUND', 'Заказ не найден', 404);
  let requests = 0;
  const queries: ProductQueries = {
    fulfillment: async () => { requests++; return { status: 'pending', tickets: [], notifications: [] }; },
    report: async () => [],
  };
  const ownership: OrderOwnership = { assertOwnedBy: async (order, buyer) => {
    assert.equal(order, orderId);
    assert.equal(buyer, buyerId);
    throw denied;
  } };

  await assert.rejects(new ProductQueriesService(queries, ownership).fulfillment(orderId, buyerId),
    (error: unknown) => error === denied);
  assert.equal(requests, 0);
});

test('Fulfillment waits for ownership and receives exactly the same order and buyer', async () => {
  let allow!: () => void;
  const permission = new Promise<void>((resolve) => { allow = resolve; });
  const calls: [string, string, string][] = [];
  const result: FulfillmentResult = { status: 'completed', tickets: [{ id: 'ticket' }], notifications: [{ id: 'notice' }] };
  const ownership: OrderOwnership = { assertOwnedBy: async (order, buyer) => {
    calls.push(['ownership', order, buyer]);
    await permission;
  } };
  const queries: ProductQueries = {
    fulfillment: async (order, buyer) => { calls.push(['fulfillment', order, buyer]); return result; },
    report: async () => [],
  };

  const pending = new ProductQueriesService(queries, ownership).fulfillment(orderId, buyerId);
  assert.deepEqual(calls, [['ownership', orderId, buyerId]]);
  allow();
  assert.equal(await pending, result);
  assert.deepEqual(calls, [['ownership', orderId, buyerId], ['fulfillment', orderId, buyerId]]);
});

test('a downstream failure is preserved after successful ownership instead of becoming pending', async () => {
  const unavailable = new AppError('DEPENDENCY_UNAVAILABLE', 'Сервис недоступен', 503);
  let checked = false;
  const ownership: OrderOwnership = { assertOwnedBy: async () => { checked = true; } };
  const queries: ProductQueries = {
    fulfillment: async () => { assert.equal(checked, true); throw unavailable; },
    report: async () => [],
  };

  await assert.rejects(new ProductQueriesService(queries, ownership).fulfillment(orderId, buyerId),
    (error: unknown) => error === unavailable);
});

test('report limit parsing preserves default and range without accepting coerced query values', () => {
  const pipe = new ReportLimitPipe();
  assert.equal(pipe.transform(undefined), 10);
  for (const [raw, expected] of [['1', 1], ['10', 10], ['100', 100], ['001', 1]] as const) {
    assert.equal(pipe.transform(raw), expected);
  }
  for (const raw of ['', '0', '101', '-1', '+1', '1.5', '1e1', ' 10', '10 ', '10\n', ['10'], null, 10]) {
    assert.throws(() => pipe.transform(raw), (error: unknown) => error instanceof AppError
      && error.code === 'VALIDATION_FAILED' && error.status === 400);
  }
});

test('order path validation accepts UUID and retains the public validation failure contract', () => {
  const pipe = new ProductQueryUuidPipe();
  assert.equal(pipe.transform(orderId), orderId);
  for (const value of [undefined, '', 'not-an-order-id', [orderId]]) {
    assert.throws(() => pipe.transform(value), (error: unknown) => error instanceof AppError
      && error.code === 'VALIDATION_FAILED' && error.status === 400);
  }
});
