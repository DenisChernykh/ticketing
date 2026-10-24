// Public identity API. Other modules never import repositories or composition internals.
export { AuthService } from './application/auth.service';
export { header, type HttpRequest } from './presentation/request';
export { RequireSession, CurrentPrincipal, SessionGuard, OriginGuard } from './presentation/access';
export type { User, Principal, Role } from './domain/identity';
