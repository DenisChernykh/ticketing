import type { PaymentSnapshot } from '@ticketing/contracts';
export interface Payment {
  id: string;
  orderId: string;
  amount: number;
  status: 'not_started' | 'pending' | 'succeeded' | 'failed' | 'refund_pending' | 'refunded';
  providerVersion: number;
}
export interface Payments {
  ensure(orderId: string, amount: number): Promise<Payment>;
  lock(orderId: string): Promise<Payment | undefined>;
  charge(payment: Payment): Promise<void>;
  refund(payment: Payment): Promise<void>;
  record(payment: Payment, provider: PaymentSnapshot): Promise<void>;
}
export interface PaymentOperation {
  id: string;
  owner: string;
  kind: 'charge' | 'refund';
  payment: Payment;
  traceContext: Record<string, string>;
}
export interface PaymentJobs {
  claim(): Promise<PaymentOperation | undefined>;
  finish(job: PaymentOperation, success: boolean): Promise<void>;
  nextSync(): Promise<{ payment: Payment; traceContext: Record<string, string> } | undefined>;
  receive(eventId: string, payment: PaymentSnapshot, fingerprint: string): Promise<void>;
}
export interface PaymentProvider {
  charge(payment: Payment): Promise<PaymentSnapshot>;
  refund(payment: Payment): Promise<PaymentSnapshot>;
  get(payment: Payment): Promise<PaymentSnapshot | undefined>;
}
export interface PaymentSettlement {
  settle(orderId: string, provider: PaymentSnapshot): Promise<void>;
}
