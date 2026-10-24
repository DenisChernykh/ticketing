import { Database, RedisConnection } from '@ticketing/core';
import { AuthService } from '../application/auth.service';
import { PgIdentities } from '../infrastructure/pg-identities';
import { RedisSessions } from '../infrastructure/redis-sessions';
import { ScryptPasswords } from '../infrastructure/password';
import { AuthController, SESSION_OPTIONS } from '../presentation/auth.controller';
import { OriginGuard, SessionGuard } from '../presentation/access';

export function identityComposition(db: Database, redis: RedisConnection, options: {
  prefix: string; ttl: number; secure: boolean; loginLimit: number; windowSeconds: number; orderLimit: number;
}) {
  const auth = new AuthService(new PgIdentities(db), new RedisSessions(redis, options.prefix, options.ttl), new ScryptPasswords(), options.loginLimit, options.windowSeconds, options.orderLimit);
  return { auth, controllers: [AuthController], providers: [
    { provide: AuthService, useValue: auth }, { provide: SESSION_OPTIONS, useValue: options },
    SessionGuard, OriginGuard,
  ] };
}
