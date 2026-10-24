import {
  Body,
  CanActivate,
  Controller,
  ExecutionContext,
  Get,
  Headers,
  HttpCode,
  Inject,
  Injectable,
  Param,
  Post,
  Req,
  UseGuards,
} from '@nestjs/common';
import { ApiBody, ApiOperation, ApiSecurity, ApiTags } from '@nestjs/swagger';
import { AppError, assertInternalToken } from '@ticketing/core';
import { isUuid } from '@ticketing/contracts';
import type { PaymentInput } from '../application/ports';
import { ProviderService } from '../application/provider.service';
export const PROVIDER_TOKEN = Symbol('PROVIDER_TOKEN');
@Injectable()
export class ProviderGuard implements CanActivate {
  constructor(@Inject(PROVIDER_TOKEN) private readonly token: string) {}
  canActivate(c: ExecutionContext) {
    assertInternalToken(c.switchToHttp().getRequest().headers['x-internal-token'], this.token);
    return true;
  }
}
function id(value: unknown): string {
  if (!isUuid(value)) throw new AppError('INVALID_ID', 'Ожидается UUID', 400);
  return value;
}
function input(value: unknown): PaymentInput {
  const p = value as PaymentInput;
  if (!p || !Number.isSafeInteger(p.amount) || p.amount < 1 || p.currency !== 'RUB')
    throw new AppError('INVALID_PAYMENT', 'Неверный платёж', 400);
  return { id: id(p.id), orderId: id(p.orderId), amount: p.amount, currency: 'RUB' };
}
@ApiTags('Учебный платёжный провайдер')
@ApiSecurity('internal')
@Controller()
@UseGuards(ProviderGuard)
export class ProviderController {
  constructor(@Inject(ProviderService) private readonly service: ProviderService) {}
  @Post('payments')
  @ApiOperation({ summary: 'Идемпотентно создать учебный платёж' })
  async create(
    @Body() body: unknown,
    @Headers('idempotency-key') key: unknown,
    @Req() req: { destroy(): void },
  ) {
    const p = input(body);
    if (key !== p.id) throw new AppError('INVALID_KEY', 'Ключ должен совпадать с id платежа', 400);
    const r = await this.service.create(p);
    if (r.dropResponse) {
      req.destroy();
      return;
    }
    return r.payment;
  }
  @Get('payments/:id') get(@Param('id') raw: string) {
    return this.service.get(id(raw));
  }
  @Post('payments/:id/refund') @HttpCode(202) async refund(@Param('id') raw: string) {
    const r = await this.service.refund(id(raw));
    if (r.fail) throw new AppError('SIMULATED_UNAVAILABLE', 'Учебный временный отказ возврата', 503);
    return r.payment;
  }
  @Post('internal/scenarios/:orderId')
  @HttpCode(204)
  @ApiOperation({ summary: 'Задать поведение до начала оплаты' })
  @ApiBody({
    schema: {
      type: 'object',
      required: ['mode'],
      properties: {
        mode: {
          type: 'string',
          enum: [
            'success',
            'decline',
            'lost_response',
            'duplicate_webhook',
            'delayed_webhook',
            'no_webhook',
            'refund_failure',
          ],
        },
        delayMs: { type: 'integer', minimum: 0, maximum: 60000 },
      },
    },
  })
  async scenario(@Param('orderId') raw: string, @Body() body: unknown) {
    const b = body as { mode: string; delayMs?: number };
    if (
      !b ||
      ![
        'success',
        'decline',
        'lost_response',
        'duplicate_webhook',
        'delayed_webhook',
        'no_webhook',
        'refund_failure',
      ].includes(b.mode) ||
      !Number.isInteger(b.delayMs ?? 500) ||
      (b.delayMs ?? 500) < 0 ||
      (b.delayMs ?? 500) > 60000
    )
      throw new AppError('INVALID_SCENARIO', 'Неверный сценарий', 400);
    await this.service.scenario(id(raw), b.mode, b.delayMs ?? 500);
  }
  @Post('internal/import') @HttpCode(204) async legacy(@Body() body: unknown) {
    const b = body as PaymentInput & { capturedAt: string };
    if (!b || typeof b.capturedAt !== 'string' || !Number.isFinite(Date.parse(b.capturedAt)))
      throw new AppError('INVALID_DATE', 'Неверная дата', 400);
    await this.service.importPayment(input(b), b.capturedAt);
  }
}
@Controller('health')
export class ProviderHealthController {
  constructor(@Inject(ProviderService) private readonly service: ProviderService) {}
  @Get('live') live() {
    return { status: 'live' };
  }
  @Get('ready') async ready() {
    await this.service.ready();
    return { status: 'ready' };
  }
}
