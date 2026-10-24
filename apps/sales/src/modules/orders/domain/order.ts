import type { OrderState, OrderConfirmed } from '@ticketing/contracts';
export type Order = OrderConfirmed['order'] &
  OrderState & { revision: number; payment?: { id: string; status: string; amount: number } | null };
export interface Purchase {
  eventId: string;
  quantity: number;
}
