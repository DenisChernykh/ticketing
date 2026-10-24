import { createHash, createHmac, timingSafeEqual } from 'node:crypto';
import { Body, Controller, Headers, HttpCode, Inject, Post, Req } from '@nestjs/common';
import { ApiOperation, ApiTags } from '@nestjs/swagger';
import { AppError } from '@ticketing/core';
import { isUuid, parsePayment } from '@ticketing/contracts';
import type { PaymentJobs } from '../application/ports';
export const PAYMENT_WEBHOOKS = Symbol('PAYMENT_WEBHOOKS'),
  WEBHOOK_SECRET = Symbol('WEBHOOK_SECRET');
@ApiTags('Платёжные webhook')
@Controller('webhooks/payments')
export class WebhooksController {
  constructor(
    @Inject(PAYMENT_WEBHOOKS) private readonly jobs: PaymentJobs,
    @Inject(WEBHOOK_SECRET) private readonly secret: string,
  ) {}
  @Post()
  @HttpCode(204)
  @ApiOperation({ summary: 'Подписанный webhook учебного провайдера' })
  async receive(
    @Req() req: { rawBody?: Buffer },
    @Body() body: unknown,
    @Headers('x-webhook-timestamp') timestamp: unknown,
    @Headers('x-webhook-signature') signature: unknown,
  ) {
    if (
      typeof timestamp !== 'string' ||
      !/^\d+$/.test(timestamp) ||
      Math.abs(Date.now() / 1000 - Number(timestamp)) > 300 ||
      typeof signature !== 'string' ||
      !/^[a-f0-9]{64}$/.test(signature) ||
      !req.rawBody
    )
      throw new AppError('INVALID_SIGNATURE', 'Неверная подпись webhook', 401);
    const expected = createHmac('sha256', this.secret)
      .update(timestamp + '.')
      .update(req.rawBody)
      .digest();
    if (!timingSafeEqual(expected, Buffer.from(signature, 'hex')))
      throw new AppError('INVALID_SIGNATURE', 'Неверная подпись webhook', 401);
    const b = body as { eventId: unknown; payment: unknown };
    if (!b || !isUuid(b.eventId)) throw new AppError('INVALID_WEBHOOK', 'Неверный webhook', 400);
    let p;
    try {
      p = parsePayment(b.payment);
    } catch {
      throw new AppError('INVALID_WEBHOOK', 'Неверный платёж', 400);
    }
    await this.jobs.receive(b.eventId, p, createHash('sha256').update(JSON.stringify(p)).digest('hex'));
  }
}
