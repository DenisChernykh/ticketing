import { AppError } from '@ticketing/core';
import type { ProviderRepository, PaymentInput } from './ports';
export class ProviderService {
  constructor(private readonly repository: ProviderRepository) {}
  create(p: PaymentInput) {
    return this.repository.create(p);
  }
  async get(id: string) {
    const p = await this.repository.get(id);
    if (!p) throw new AppError('PAYMENT_NOT_FOUND', 'Платёж не найден', 404);
    return p;
  }
  async refund(id: string) {
    await this.get(id);
    return this.repository.refund(id);
  }
  scenario(orderId: string, mode: string, delay: number) {
    return this.repository.scenario(orderId, mode, delay);
  }
  importPayment(p: PaymentInput, capturedAt: string) {
    return this.repository.importPayment(p, capturedAt);
  }
  ready() {
    return this.repository.health();
  }
}
