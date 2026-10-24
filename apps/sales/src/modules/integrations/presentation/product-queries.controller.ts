import { streamCsv, exportFilter, type StreamResponse } from '@ticketing/core';
import { Controller, Get, Inject, Param, Query, Res } from '@nestjs/common';
import { ApiCookieAuth, ApiOperation, ApiQuery, ApiTags } from '@nestjs/swagger';
import { CurrentPrincipal, RequireSession, type Principal } from '../../identity-access';
import { ProductQueriesService } from '../application/product-queries.service';
import { ProductQueryUuidPipe, ReportLimitPipe } from './product-query.validation';

/**
 * Делает внутренние чтения доступными только владельцу данных через публичный Sales API.
 *
 * @remarks
 * RequireSession проверяет сессию и роль, pipes разбирают параметры HTTP.
 * Контроллер передаёт id из подтверждённого principal в один прикладной вызов.
 * Проверка владения заказом находится в ProductQueriesService, служебный токен
 * добавляет HTTP-адаптер. organizerId из строки запроса здесь не принимается.
 */
@ApiTags('Билеты и отчёты')
@ApiCookieAuth('session')
@Controller()
export class ProductQueriesController {
  constructor(@Inject(ProductQueriesService) private readonly queries: ProductQueriesService) {}
  @Get('orders/:id/tickets')
  @ApiOperation({ summary: 'Билеты и уведомление: pending/completed/failed, 503 при недоступности сервиса' })
  @RequireSession('buyer')
  tickets(@Param('id', ProductQueryUuidPipe) id: string, @CurrentPrincipal() actor: Principal) {
    return this.queries.fulfillment(id, actor.user.id);
  }
  @Get('reports/orders.csv')
  @RequireSession('organizer')
  @ApiOperation({ summary: 'Скачать CSV заказов за период подтверждения (UTC)' })
  csv(
    @CurrentPrincipal() actor: Principal,
    @Query() query: Record<string, unknown>,
    @Res() response: StreamResponse,
  ) {
    const filter = exportFilter(query);
    return streamCsv(response, (signal) => this.queries.csv(actor.user.id, filter, signal));
  }
  @Get('reports/events')
  @ApiOperation({ summary: 'Продажи моих мероприятий и SQL-рейтинг' })
  @RequireSession('organizer')
  events(@CurrentPrincipal() actor: Principal) {
    return this.queries.report('events', actor.user.id);
  }
  @Get('reports/daily')
  @ApiOperation({ summary: 'Моя дневная и накопительная выручка' })
  @RequireSession('organizer')
  daily(@CurrentPrincipal() actor: Principal) {
    return this.queries.report('daily', actor.user.id);
  }
  @Get('reports/popular')
  @ApiQuery({ name: 'limit', required: false, example: 10 })
  @ApiOperation({ summary: 'Популярные мероприятия: Redis sorted set с SQL fallback' })
  @RequireSession('organizer')
  popular(@CurrentPrincipal() actor: Principal, @Query('limit', ReportLimitPipe) limit: number) {
    return this.queries.report('popular', actor.user.id, limit);
  }
}
