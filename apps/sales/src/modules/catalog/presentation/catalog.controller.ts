import { pageInput } from '@ticketing/core';
import { Body, Controller, Get, Inject, Param, Patch, Post, Query } from '@nestjs/common';
import { ApiBody, ApiCookieAuth, ApiOperation, ApiSecurity, ApiTags } from '@nestjs/swagger';
import { CurrentPrincipal, RequireSession, type Principal } from '../../identity-access';
import { CatalogService } from '../application/catalog.service';
import type { EventInput } from '../domain/event';
import { eventInput, eventSchema, uuid } from './event.validation';

/**
 * Разделяет публичный каталог и операции организатора.
 * Идентификатор организатора всегда берётся из проверенной сессии; SQL-адаптер
 * повторно ограничивает им редактирование и личную статистику. Публичный GET
 * не зависит от сессии, поэтому сохраняет SQL-fallback при недоступном Redis.
 */
@ApiTags('Мероприятия')
@Controller()
export class CatalogController {
  constructor(@Inject(CatalogService) private readonly catalog: CatalogService) {}
  @Get('events')
  @ApiOperation({ summary: 'Каталог: Redis cache-aside с поколением из PostgreSQL' })
  list(@Query() query: Record<string, unknown>) {
    return this.catalog.list(pageInput(query));
  }
  @Get('organizer/events')
  @ApiCookieAuth('session')
  @RequireSession('organizer')
  @ApiOperation({ summary: 'Мои мероприятия, включая мероприятия без продаж (LEFT JOIN)' })
  own(@CurrentPrincipal() identity: Principal, @Query() query: Record<string, unknown>) {
    return this.catalog.organizer(identity.user.id, pageInput(query));
  }
  @Post('events')
  @ApiCookieAuth('session')
  @ApiSecurity('csrf')
  @RequireSession('organizer')
  @ApiBody({ schema: { ...eventSchema, required: ['title', 'venue', 'startsAt', 'capacity', 'unitPrice'] } })
  create(@Body() body: unknown, @CurrentPrincipal() identity: Principal) {
    return this.catalog.create(identity.user.id, eventInput(body) as EventInput);
  }
  @Patch('events/:id')
  @ApiCookieAuth('session')
  @ApiSecurity('csrf')
  @ApiBody({
    schema: {
      ...eventSchema,
      properties: { ...eventSchema.properties, version: { type: 'integer', minimum: 1 } },
      required: ['version'],
    },
  })
  @RequireSession('organizer')
  update(@Param('id') id: string, @Body() body: unknown, @CurrentPrincipal() identity: Principal) {
    return this.catalog.update(
      uuid(id),
      identity.user.id,
      eventInput(body, true) as Partial<EventInput> & { version: number },
    );
  }
}
