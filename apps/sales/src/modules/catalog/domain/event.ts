export interface Event {
  id: string;
  organizerId: string;
  title: string;
  venue: string;
  startsAt: string;
  version: number;
  capacity: number;
  available: number;
  unitPrice: number;
  currency: 'RUB';
}
export interface EventInput {
  title: string;
  venue: string;
  startsAt: string;
  capacity: number;
  unitPrice: number;
}
export interface OrganizerEvent extends Event {
  sold: number;
  revenue: number;
}
