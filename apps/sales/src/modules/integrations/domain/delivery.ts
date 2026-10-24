export interface FulfillmentResult {
  status: 'pending' | 'completed' | 'failed' | 'cancelled';
  tickets: unknown[];
  notifications: unknown[];
}
