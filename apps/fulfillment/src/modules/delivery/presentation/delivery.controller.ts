import { Controller, Get, Headers, Inject, Param, ParseUUIDPipe, Query } from '@nestjs/common';
import { ApiHeader, ApiOperation, ApiQuery, ApiTags } from '@nestjs/swagger';
import { assertInternalToken } from '@ticketing/core';
import { ReadDelivery } from '../application/read-delivery';

export const INTERNAL_TOKEN = Symbol('FulfillmentInternalToken');

@Controller('internal/orders')
@ApiTags('Internal delivery')
@ApiHeader({ name: 'X-Internal-Token', required: true, description: 'Service credential; this API is called by Sales.' })
export class DeliveryController {
  constructor(
    @Inject(ReadDelivery) private readonly delivery: ReadDelivery,
    @Inject(INTERNAL_TOKEN) private readonly internalToken: string,
  ) {}

  @Get(':orderId')
  @ApiOperation({ summary: 'Билеты, уведомления и состояние обработки заказа покупателя' })
  @ApiQuery({ name: 'buyerId', required: true, type: String, format: 'uuid' })
  order(
    @Headers('x-internal-token') token: string | undefined,
    @Param('orderId', new ParseUUIDPipe()) orderId: string,
    @Query('buyerId', new ParseUUIDPipe()) buyerId: string,
  ) {
    assertInternalToken(token, this.internalToken);
    return this.delivery.order(orderId, buyerId);
  }
}

@Controller('health')
@ApiTags('Health')
export class HealthController {
  constructor(@Inject(ReadDelivery) private readonly delivery: ReadDelivery) {}

  @Get('live')
  live() { return { status: 'alive' }; }

  @Get('ready')
  @ApiOperation({ summary: 'Готовность чтения из БД; состояние RabbitMQ указано отдельно' })
  ready() { return this.delivery.ready(); }

  @Get('workers')
  workers() { return this.delivery.workerStates(); }
}
