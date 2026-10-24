export interface ReadinessPort { check(): Promise<void>; }
export class HealthService {
  constructor(private readonly readiness: ReadinessPort) {}
  async ready() { await this.readiness.check(); return { status: 'ready', service: 'sales' }; }
}
