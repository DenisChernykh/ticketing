import { PgCatalogQueries } from '../infrastructure/pg-catalog-queries';
import { PgInventory } from '../infrastructure/pg-inventory';
import { Database, RedisConnection } from '@ticketing/core';
import { CatalogService } from '../application/catalog.service';
import { PgCatalog } from '../infrastructure/pg-catalog';
import { RedisCatalog } from '../infrastructure/redis-catalog';
import { CatalogController } from '../presentation/catalog.controller';
export function catalogComposition(db: Database, redis: RedisConnection, prefix: string, ttl: number) {
  const service = new CatalogService(
    new PgCatalogQueries(db),
    new RedisCatalog(redis, prefix, ttl),
    new PgCatalog(db),
  );
  return {
    inventory: new PgInventory(db),
    controllers: [CatalogController],
    providers: [{ provide: CatalogService, useValue: service }],
  };
}
