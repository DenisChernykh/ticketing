import type { OrderEvent } from '@ticketing/contracts';
export type Destination = 'rabbitmq' | 'kafka';
export interface Delivery {
  messageId: string;
  owner: string;
  attempts: number;
  event: OrderEvent;
  traceContext?: Record<string, string>;
}
