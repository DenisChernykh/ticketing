import { Controller, Get, Inject } from '@nestjs/common';
import { ApiTags } from '@nestjs/swagger';
import { HealthService } from '../application/health.service';
@ApiTags('Состояние') @Controller('health')
export class HealthController {
  constructor(@Inject(HealthService) private readonly health: HealthService) {}
  @Get('live') live() { return { status: 'alive', service: 'sales' }; }
  @Get('ready') ready() { return this.health.ready(); }
}
