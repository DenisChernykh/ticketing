import { SqlOrderExports } from '../infrastructure/sql-order-exports';
import {
  Injectable,
  Module,
  OnApplicationBootstrap,
  OnApplicationShutdown,
  OnModuleDestroy,
} from '@nestjs/common';
import { config, Database, log, RedisConnection, SqlRowStream } from '@ticketing/core';
import { RecordSale, ReportsService } from '../application/reports.service';
import { KafkaProjections } from '../infrastructure/kafka-projections';
import { RedisRanking } from '../infrastructure/redis-ranking';
import { SqlReportsRepository } from '../infrastructure/sql-reports.repository';
import {
  HealthController,
  INTERNAL_TOKEN,
  InternalGuard,
  ReportsController,
} from '../presentation/reports.controller';

/** Связывает запуск и остановку адаптеров; правила обработки продаж находятся в них. */
@Injectable()
class AnalyticsLifecycle implements OnApplicationBootstrap, OnApplicationShutdown, OnModuleDestroy {
  constructor(
    private readonly runtime: KafkaProjections,
    private readonly redis: RedisConnection,
    private readonly database: Database,
    private readonly exports: SqlRowStream,
  ) {}

  /**
   * Допускает отказ необязательного Redis, затем готовит SQL и фоновый runtime.
   * Подключение Kafka восстанавливается самим runtime независимо от HTTP-чтения.
   */
  async onApplicationBootstrap(): Promise<void> {
    try {
      await this.redis.connect();
    } catch (error) {
      log('analytics.redis.unavailable', { error: error instanceof Error ? error.message : String(error) });
    }
    // HTTP-чтению нужна собственная БД; восстановление Kafka выполняется в фоне.
    await this.runtime.start();
  }

  /** Сначала дожидается consumers и replay, пока их SQL/Redis ещё открыты. */
  async onModuleDestroy(): Promise<void> {
    await this.runtime.stop();
  }

  /** Закрывает соединения после завершения операций фонового runtime. */
  async onApplicationShutdown(): Promise<void> {
    await this.redis.close();
    await this.exports.close();
    await this.database.close();
  }
}

/**
 * Подключает прикладные порты к SQL, Redis и Kafka и регистрирует HTTP-контроллеры.
 * Здесь выбираются зависимости; транзакции, дедупликация и replay реализованы
 * в соответствующих адаптерах, а не в Nest-провайдерах этого модуля.
 */
@Module({
  controllers: [ReportsController, HealthController],
  providers: [
    {
      provide: Database,
      useFactory: () => new Database(config().analyticsDatabaseUrl, 'ticketing-analytics'),
    },
    {
      provide: RedisConnection,
      useFactory: () => new RedisConnection(config().redisUrl, 'ticketing-analytics-ranking'),
    },
    {
      provide: SqlReportsRepository,
      inject: [Database],
      useFactory: (database: Database) => new SqlReportsRepository(database),
    },
    { provide: SqlRowStream, useFactory: () => new SqlRowStream(config().analyticsDatabaseUrl) },
    {
      provide: SqlOrderExports,
      inject: [SqlRowStream],
      useFactory: (stream: SqlRowStream) => new SqlOrderExports(stream),
    },
    {
      provide: RecordSale,
      inject: [SqlReportsRepository],
      useFactory: (repository: SqlReportsRepository) => new RecordSale(repository),
    },
    {
      provide: RedisRanking,
      inject: [SqlReportsRepository, RedisConnection],
      useFactory: (repository: SqlReportsRepository, redis: RedisConnection) =>
        new RedisRanking(repository, redis, config().prefix),
    },
    {
      provide: KafkaProjections,
      inject: [SqlReportsRepository, RecordSale, RedisRanking],
      useFactory: (repository: SqlReportsRepository, record: RecordSale, ranking: RedisRanking) => {
        const environment = config();
        return new KafkaProjections(repository, record, ranking, {
          brokers: environment.kafkaBrokers,
          topic: environment.kafkaTopic,
          prefix: environment.prefix,
        });
      },
    },
    {
      provide: ReportsService,
      inject: [SqlReportsRepository, RedisRanking, KafkaProjections, SqlOrderExports],
      useFactory: (
        repository: SqlReportsRepository,
        ranking: RedisRanking,
        runtime: KafkaProjections,
        exports: SqlOrderExports,
      ) => new ReportsService(repository, ranking, runtime, exports),
    },
    { provide: INTERNAL_TOKEN, useFactory: () => config().internalToken },
    InternalGuard,
    AnalyticsLifecycle,
  ],
})
export class AnalyticsModule {}
