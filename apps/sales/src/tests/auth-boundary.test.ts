import 'reflect-metadata';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import type { ExecutionContext } from '@nestjs/common';
import { ROUTE_ARGS_METADATA } from '@nestjs/common/constants';
import { Reflector } from '@nestjs/core';
import { AppError } from '@ticketing/core';
import { AuthService } from '../modules/identity-access/application/auth.service';
import type { IdentityRepository, SessionStore } from '../modules/identity-access/application/ports';
import type { Principal, Role } from '../modules/identity-access/domain/identity';
import { CurrentPrincipal, OriginGuard, RequireSession, SessionGuard } from '../modules/identity-access/presentation/access';
import { loginInput } from '../modules/identity-access/presentation/auth.validation';
import type { HttpRequest } from '../modules/identity-access/presentation/request';

@RequireSession('buyer')
class ProtectedRoutes {
  read(@CurrentPrincipal() identity: Principal) { return identity; }
  @RequireSession('organizer') organizer() {}
}

function context(request: HttpRequest, method: 'read' | 'organizer' = 'read'): ExecutionContext {
  return {
    switchToHttp: () => ({ getRequest: () => request }),
    getHandler: () => ProtectedRoutes.prototype[method], getClass: () => ProtectedRoutes,
  } as unknown as ExecutionContext;
}

function fixture() {
  const state = { active: true, role: 'buyer' as Role, reads: 0, redisAvailable: true };
  const users: IdentityRepository = {
    findCredential: async () => undefined,
    findActiveUser: async () => {
      state.reads++;
      return state.active ? { id: 'buyer', email: 'buyer@example.test', role: state.role } : undefined;
    },
  };
  const sessions: SessionStore = {
    create: async () => 'unused', delete: async () => {}, consumeLimit: async () => true,
    get: async () => {
      if (!state.redisAvailable) throw new AppError('DEPENDENCY_UNAVAILABLE', 'Redis недоступен', 503);
      return { userId: 'buyer', csrfToken: 'csrf' };
    },
  };
  const auth = new AuthService(users, sessions, { verify: async () => false }, 10, 60, 100);
  const guard = new SessionGuard(auth, new Reflector());
  const request = (method = 'GET'): HttpRequest => ({
    method, headers: { host: '127.0.0.1:3110', cookie: `ticketing_session=${'a'.repeat(43)}` }, protocol: 'http', socket: {},
  });
  return { state, guard, request };
}

const status = (expected: number) => (error: unknown) => error instanceof AppError && error.status === expected;

test('session guard authenticates once and rereads account state on each request', async () => {
  const f = fixture();
  const first = f.request();
  assert.equal(await f.guard.canActivate(context(first)), true);
  assert.equal(f.state.reads, 1);
  assert.equal(first.principal?.user.role, 'buyer');
  f.state.active = false;
  const second = f.request();
  second.principal = first.principal;
  await assert.rejects(f.guard.canActivate(context(second)), status(401));
  assert.equal(f.state.reads, 2);
  assert.equal(second.principal, undefined);
});

test('method role overrides class role and a missing cookie remains unauthorized', async () => {
  const f = fixture();
  await assert.rejects(f.guard.canActivate(context(f.request(), 'organizer')), status(403));
  f.state.role = 'organizer';
  assert.equal(await f.guard.canActivate(context(f.request(), 'organizer')), true);
  const anonymous = f.request(); delete anonymous.headers.cookie;
  await assert.rejects(f.guard.canActivate(context(anonymous)), status(401));
});

test('unsafe methods require CSRF and same origin; safe methods keep session-only access', async () => {
  const f = fixture();
  for (const method of ['GET', 'HEAD', 'OPTIONS']) assert.equal(await f.guard.canActivate(context(f.request(method))), true);
  for (const method of ['POST', 'PATCH', 'PUT', 'DELETE']) {
    const missing = f.request(method);
    await assert.rejects(f.guard.canActivate(context(missing)), status(403));
    assert.equal(missing.principal, undefined);
    const crossSite = f.request(method);
    crossSite.headers['x-csrf-token'] = 'csrf'; crossSite.headers.origin = 'http://elsewhere.test';
    await assert.rejects(f.guard.canActivate(context(crossSite)), status(403));
    const valid = f.request(method);
    valid.headers['x-csrf-token'] = 'csrf'; valid.headers.origin = 'http://127.0.0.1:3110';
    assert.equal(await f.guard.canActivate(context(valid)), true);
  }
  const unknownMethod = f.request(); delete unknownMethod.method;
  await assert.rejects(f.guard.canActivate(context(unknownMethod)), status(403));
});

test('Redis failure propagates as 503 and login origin protection does not require a session', async () => {
  const f = fixture();
  f.state.redisAvailable = false;
  await assert.rejects(f.guard.canActivate(context(f.request())), status(503));
  const request = f.request('POST'); delete request.headers.cookie;
  const origin = new OriginGuard();
  assert.equal(origin.canActivate(context(request)), true);
  request.headers.origin = 'null';
  assert.throws(() => origin.canActivate(context(request)), status(403));
});

test('CurrentPrincipal refuses a request without a completed session guard', async () => {
  const metadata = Reflect.getMetadata(ROUTE_ARGS_METADATA, ProtectedRoutes, 'read') as Record<string, {
    factory(data: unknown, context: ExecutionContext): Principal;
  }>;
  const factory = Object.values(metadata)[0].factory;
  const f = fixture(), request = f.request();
  assert.throws(() => factory(undefined, context(request)), status(401));
  await f.guard.canActivate(context(request));
  assert.equal(factory(undefined, context(request)), request.principal);
});

test('login parser retains strict field, length and whitespace rules', () => {
  assert.deepEqual(loginInput({ email: 'BUYER@EXAMPLE.TEST', password: ' unchanged ' }), {
    email: 'buyer@example.test', password: ' unchanged ',
  });
  for (const value of [null, [], {}, { email: 'buyer@example.test', password: '' },
    { email: ' buyer@example.test', password: 'valid' }, { email: 'buyer@example.test ', password: 'valid' },
    { email: 'buyer@example.test', password: 'x'.repeat(257) },
    { email: 'buyer@example.test', password: 'valid', role: 'organizer' },
    { email: 12, password: 'valid' }, { email: 'buyer@example.test', password: 12 },
  ]) assert.throws(() => loginInput(value), status(400));
});
