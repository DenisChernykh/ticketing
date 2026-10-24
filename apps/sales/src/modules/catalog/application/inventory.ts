export interface InventorySnapshot {
  id: string;
  organizerId: string;
  title: string;
  venue: string;
  startsAt: string;
  unitPrice: number;
  currency: 'RUB';
}
export interface Inventory {
  lock(id: string): Promise<void>;
  hold(id: string, quantity: number, now: string): Promise<InventorySnapshot>;
  release(id: string, quantity: number): Promise<void>;
}
