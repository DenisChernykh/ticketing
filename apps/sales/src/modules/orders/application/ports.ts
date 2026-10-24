import type { Page, PageInput } from '@ticketing/contracts';
import type { Order } from '../domain/order';
export interface OrdersRepository {
  claimKey(buyerId: string, key: string, hash: string): Promise<{ hash: string; result: Order | null }>;
  completeKey(buyerId: string, key: string, order: Order): Promise<void>;
  insert(order: Order): Promise<void>;
  save(order: Order): Promise<void>;
  lock(id: string): Promise<Order | undefined>;
  now(): Promise<string>;
  due(): Promise<string[]>;
}
export interface OrdersReader {
  get(id: string, buyerId: string): Promise<Order | undefined>;
  list(buyerId: string, page: PageInput): Promise<Page<Order>>;
}
