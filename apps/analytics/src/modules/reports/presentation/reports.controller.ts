import {
  Body,
  CanActivate,
  Controller,
  ExecutionContext,
  Get,
  HttpCode,
  Inject,
  Injectable,
  Post,
  Query,
  UseGuards,
  Res,
} from '@nestjs/common';
import { ApiBody, ApiOperation, ApiQuery, ApiSecurity, ApiTags } from '@nestjs/swagger';
import { assertInternalToken, streamCsv, exportFilter, csvCell, type StreamResponse } from '@ticketing/core';
import { ReportsService } from '../application/reports.service';

export const INTERNAL_TOKEN = Symbol('ANALYTICS_INTERNAL_TOKEN');

/**
 * Проверяет доверие к межсервисному HTTP-вызову через внутренний токен.
 * Пользовательскую сессию и роль проверяет Sales; переданный им organizerId
 * затем ограничивает SQL-запрос. Сам токен не является сессией организатора.
 */
@Injectable()
export class InternalGuard implements CanActivate {
  constructor(@Inject(INTERNAL_TOKEN) private readonly token: string) {}

  canActivate(context: ExecutionContext): boolean {
    const request = context.switchToHttp().getRequest<{ headers: Record<string, unknown> }>();
    assertInternalToken(request.headers['x-internal-token'], this.token);
    return true;
  }
}

/** Транспортный вход: проверка токена и передача параметров в ReportsService. */
@ApiTags('Internal reports')
@ApiSecurity('internal')
@UseGuards(InternalGuard)
@Controller('internal')
export class ReportsController {
  constructor(private readonly reports: ReportsService) {}

  @Get('reports/orders.csv')
  @ApiOperation({ summary: 'Потоковый отчёт по заказам' })
  async csv(@Query() query: Record<string, unknown>, @Res() response: StreamResponse) {
    const filter = exportFilter(query),
      rows = this.reports;
    await streamCsv(response, async (signal) => {
      const source = rows.exportRows(query.organizerId, filter, signal);
      return (async function* () {
        const fields = [
          'orderId',
          'eventId',
          'eventTitle',
          'confirmedAt',
          'quantity',
          'totalPrice',
          'status',
          'cancelledAt',
          'refundedAt',
          'refundedAmount',
        ];
        // Read first row before headers so initial database errors remain HTTP errors.
        const iterator = source[Symbol.asyncIterator]();
        try {
          let next = await iterator.next();
          yield fields.join(',') + '\r\n';
          while (!next.done) {
            yield fields
              .map((k) =>
                csvCell(
                  next.value[k] instanceof Date ? (next.value[k] as Date).toISOString() : next.value[k],
                ),
              )
              .join(',') + '\r\n';
            next = await iterator.next();
          }
        } finally {
          await iterator.return?.();
        }
      })();
    });
  }
  @Get('reports/events')
  @ApiOperation({ summary: 'Продажи мероприятий организатора и рейтинг по количеству билетов' })
  @ApiQuery({ name: 'organizerId', format: 'uuid', required: true })
  events(@Query('organizerId') organizerId: unknown) {
    return this.reports.events(organizerId);
  }

  @Get('reports/daily')
  @ApiOperation({ summary: 'Продажи по дням UTC и накопленная выручка' })
  @ApiQuery({ name: 'organizerId', format: 'uuid', required: true })
  daily(@Query('organizerId') organizerId: unknown) {
    return this.reports.daily(organizerId);
  }

  @Get('reports/popular')
  @ApiOperation({ summary: 'Популярные мероприятия: Redis sorted set с восстановлением из PostgreSQL' })
  @ApiQuery({ name: 'organizerId', format: 'uuid', required: true })
  @ApiQuery({ name: 'limit', required: false, type: Number, minimum: 1, maximum: 100 })
  popular(@Query('organizerId') organizerId: unknown, @Query('limit') limit: unknown) {
    return this.reports.popular(organizerId, limit);
  }

  @Post('replay')
  @HttpCode(200)
  @ApiOperation({ summary: 'Перестроить выбранные проекции в новых поколениях и продолжить live-чтение' })
  @ApiBody({
    schema: {
      type: 'object',
      required: ['projection'],
      properties: {
        projection: { type: 'string', enum: ['events', 'daily', 'all'] },
      },
    },
  })
  replay(@Body() body: unknown) {
    const projection =
      body && typeof body === 'object' && !Array.isArray(body)
        ? (body as Record<string, unknown>).projection
        : undefined;
    return this.reports.replay(projection);
  }
}

@ApiTags('Health')
@Controller('health')
export class HealthController {
  constructor(private readonly reports: ReportsService) {}

  @Get('live') live() {
    return { status: 'alive' };
  }
  @Get('ready') ready() {
    return this.reports.ready();
  }
  @Get('workers') workers() {
    return this.reports.workers();
  }
}
