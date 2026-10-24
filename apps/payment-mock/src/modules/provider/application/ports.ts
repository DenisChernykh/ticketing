import type { PaymentSnapshot } from '@ticketing/contracts';
export interface PaymentInput {
  id: string;
  orderId: string;
  amount: number;
  currency: 'RUB';
}
export interface ProviderRepository {
  create(input: PaymentInput): Promise<{ payment: PaymentSnapshot; dropResponse: boolean }>;
  get(id: string): Promise<PaymentSnapshot | undefined>;
  refund(id: string): Promise<{ payment: PaymentSnapshot; fail: boolean }>;
  scenario(orderId: string, mode: string, delay: number): Promise<void>;
  importPayment(input: PaymentInput, capturedAt: string): Promise<void>;
  health(): Promise<void>;
}
