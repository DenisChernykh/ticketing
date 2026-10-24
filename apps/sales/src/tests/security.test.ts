import { test } from 'node:test';
import assert from 'node:assert/strict';
import { AppError } from '@ticketing/core';
import { AuthService } from '../modules/identity-access/application/auth.service';
import type { IdentityRepository, SessionStore } from '../modules/identity-access/application/ports';
import { assertOrigin, type HttpRequest } from '../modules/identity-access/presentation/request';
import { hashPassword, ScryptPasswords } from '../modules/identity-access/infrastructure/password';
import { eventInput } from '../modules/catalog/presentation/event.validation';

test('passwords use a fresh salt and reject wrong, missing, and malformed credentials', async () => {
  const verifier = new ScryptPasswords();
  const first = await hashPassword('local-password');
  const second = await hashPassword('local-password');
  assert.notEqual(first, second);
  assert.equal(await verifier.verify('local-password', first), true);
  assert.equal(await verifier.verify('wrong', first), false);
  assert.equal(await verifier.verify('local-password', ''), false);
  assert.equal(await verifier.verify('local-password', 'scrypt$broken$value'), false);
});

test('the session cannot preserve a revoked account or a stale role', async () => {
  let active = true,
    deleted = false;
  const repository: IdentityRepository = {
    findCredential: async () => undefined,
    findActiveUser: async () =>
      active ? { id: 'buyer', email: 'buyer@example.test', role: 'buyer' } : undefined,
  };
  const sessions: SessionStore = {
    create: async () => 'unused',
    get: async () => ({ userId: 'buyer', csrfToken: 'csrf' }),
    delete: async () => {
      deleted = true;
    },
    consumeLimit: async () => true,
  };
  const auth = new AuthService(repository, sessions, new ScryptPasswords(), 10, 60, 100);
  await assert.rejects(
    auth.authenticate('a'.repeat(43), 'organizer'),
    (error: unknown) => error instanceof AppError && error.status === 403,
  );
  active = false;
  await assert.rejects(
    auth.authenticate('a'.repeat(43)),
    (error: unknown) => error instanceof AppError && error.status === 401,
  );
  assert.equal(deleted, true);
});

test('browser mutations reject a foreign origin, including an explicit null origin', () => {
  const request = (origin?: string, site?: string): HttpRequest => ({
    headers: { host: '127.0.0.1:3110', origin, 'sec-fetch-site': site },
    protocol: 'http',
    socket: {},
  });
  assert.doesNotThrow(() => assertOrigin(request('http://127.0.0.1:3110')));
  assert.doesNotThrow(() => assertOrigin(request())); // Non-browser clients still need a CSRF header.
  assert.throws(
    () => assertOrigin(request('http://evil.test')),
    (error: unknown) => error instanceof AppError && error.status === 403,
  );
  assert.throws(
    () => assertOrigin(request('null')),
    (error: unknown) => error instanceof AppError && error.status === 403,
  );
  assert.throws(
    () => assertOrigin(request(undefined, 'cross-site')),
    (error: unknown) => error instanceof AppError && error.status === 403,
  );
});

test('event writes reject unknown fields, fractional money, and timezone-free dates', () => {
  const valid = {
    title: 'Meetup',
    venue: 'Hall',
    startsAt: '2030-01-01T12:00:00Z',
    capacity: 100,
    unitPrice: 50000,
  };
  assert.equal(eventInput(valid).startsAt, '2030-01-01T12:00:00.000Z');
  assert.throws(() => eventInput({ ...valid, organizerId: 'foreign' }));
  assert.throws(() => eventInput({ ...valid, unitPrice: 1.5 }));
  assert.throws(() => eventInput({ ...valid, startsAt: '2030-01-01T12:00:00' }));
  assert.throws(() => eventInput({}));
  assert.throws(() => eventInput({}, true));
  assert.throws(() => eventInput({ capacity: 2 }, true));
  assert.deepEqual(eventInput({ capacity: 2, version: 1 }, true), { capacity: 2, version: 1 });
});
