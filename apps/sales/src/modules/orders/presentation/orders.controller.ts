import { pageInput } from '@ticketing/core';
import { CheckoutService } from '../../checkout';
import { Body, Controller, Get, Headers, Inject, Param, Post, Query, HttpCode } from '@nestjs/common';
import { ApiBody, ApiCookieAuth, ApiHeader, ApiOperation, ApiSecurity, ApiTags } from '@nestjs/swagger';
import { AuthService, CurrentPrincipal, RequireSession, type Principal } from '../../identity-access';
import { OrdersService } from '../application/orders.service';
import { orderId, purchaseInput } from './order.validation';

/**
 * Проверяет HTTP-контракт покупки перед входом в транзакционный сценарий.
 *
 * @remarks
 * Guard проверяет сессию, роль и CSRF. Затем purchaseInput полностью проверяет
 * тело и ключ до расходования квоты. Лимит применяется в HTTP-границе после
 * разбора запроса, но до обращения к сохранённому результату, поэтому и повторный
 * идемпотентный запрос может получить 429. Это защищает API от частых повторов,
 * не меняя итог уже подтверждённого заказа. buyerId не принимается от клиента.
 */
@ApiTags('Заказы')
@ApiCookieAuth('session')
@Controller('orders')
@RequireSession('buyer')
export class OrdersController {
  constructor(
    @Inject(OrdersService) private readonly orders: OrdersService,
    @Inject(AuthService) private readonly auth: AuthService,
    @Inject(CheckoutService) private readonly checkout: CheckoutService,
  ) {}
  @Post()
  @ApiSecurity('csrf')
  @ApiHeader({ name: 'Idempotency-Key', required: true, example: 'my-first-order' })
  @ApiOperation({ summary: 'Забронировать места на 10 минут; один ключ возвращает тот же заказ' })
  @ApiBody({
    schema: {
      type: 'object',
      required: ['eventId', 'quantity'],
      properties: {
        eventId: { type: 'string', format: 'uuid', example: '30000000-0000-4000-8000-000000000001' },
        quantity: { type: 'integer', minimum: 1, maximum: 100, example: 2 },
      },
    },
  })
  async purchase(
    @Body() body: unknown,
    @Headers('idempotency-key') rawKey: unknown,
    @CurrentPrincipal() identity: Principal,
  ) {
    const { input, key } = purchaseInput(body, rawKey);
    await this.auth.limitOrder(identity.user.id);
    return this.checkout.purchase(identity.user.id, key, input);
  }
  @Get()
  @ApiOperation({ summary: 'Мои заказы с данными мероприятий (INNER JOIN)' })
  list(@CurrentPrincipal() identity: Principal, @Query() query: Record<string, unknown>) {
    return this.orders.list(identity.user.id, pageInput(query));
  }
  @Get(':id')
  get(@Param('id') id: string, @CurrentPrincipal() identity: Principal) {
    return this.orders.get(orderId(id), identity.user.id);
  }
  @Post(':id/payments')
  @HttpCode(202)
  @ApiSecurity('csrf')
  @ApiOperation({ summary: 'Начать учебную оплату забронированного заказа' })
  pay(@Param('id') id: string, @CurrentPrincipal() identity: Principal) {
    return this.checkout.pay(orderId(id), identity.user.id);
  }
  @Post(':id/cancel')
  @HttpCode(200)
  @ApiSecurity('csrf')
  @ApiOperation({ summary: 'Отменить заказ; возврат оплаченного заказа выполняется в фоне' })
  cancel(@Param('id') id: string, @CurrentPrincipal() identity: Principal) {
    return this.checkout.cancel(orderId(id), identity.user.id);
  }
}
