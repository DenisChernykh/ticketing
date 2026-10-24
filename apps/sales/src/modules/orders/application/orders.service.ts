import { AppError } from '@ticketing/core';
import type { PageInput } from '@ticketing/contracts';
import type { OrdersReader } from './ports';
export class OrdersService {
  constructor(private readonly repository: OrdersReader) {}
  list(buyerId: string, page: PageInput = { limit: 20 }) {
    return this.repository.list(buyerId, page);
  }
  async get(id: string, buyerId: string) {
    const order = await this.repository.get(id, buyerId);
    if (!order) throw new AppError('ORDER_NOT_FOUND', 'Заказ не найден', 404);
    return order;
  }
}
