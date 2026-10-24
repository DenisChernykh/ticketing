export type Role = 'buyer' | 'organizer';
export interface User { id: string; email: string; role: Role; }
export interface Credential extends User { passwordHash: string; isActive: boolean; }
export interface Session { userId: string; csrfToken: string; }
export interface Principal { user: User; csrfToken: string; sessionToken: string; }
