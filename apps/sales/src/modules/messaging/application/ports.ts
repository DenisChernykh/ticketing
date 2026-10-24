import type { OrderEvent } from '@ticketing/contracts';
import type { Delivery, Destination } from '../domain/delivery';
export interface OutboxRepository {
  claim(destination: Destination): Promise<Delivery | undefined>;
  complete(destination: Destination, delivery: Delivery): Promise<void>;
  retry(destination: Destination, delivery: Delivery): Promise<void>;
}
export interface EventPublisher {
  publish(event: OrderEvent, traceContext?: Record<string, string>): Promise<void>;
}
export interface EventWriter {
  append(event: OrderEvent): Promise<void>;
}
