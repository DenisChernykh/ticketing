import { Body, Controller, Get, HttpCode, Inject, Post, Req, Res, UseGuards } from '@nestjs/common';
import { ApiBody, ApiCookieAuth, ApiOperation, ApiSecurity, ApiTags } from '@nestjs/swagger';
import { AuthService } from '../application/auth.service';
import type { Principal } from '../domain/identity';
import { HttpRequest, HttpResponse, writeCookie } from './request';
import { CurrentPrincipal, OriginGuard, RequireSession } from './access';
import { loginInput } from './auth.validation';

export const SESSION_OPTIONS = Symbol('SESSION_OPTIONS');
/**
 * HTTP-граница аутентификации: проверка входных данных, cookie и выдача CSRF-токена.
 *
 * @remarks
 * До входа сессии ещё нет, поэтому login проверяет Origin и лимиты, но не требует
 * CSRF-токен существующей сессии. После входа logout уже требует все проверки
 * изменяющей операции. sessionToken не попадает в JSON: браузер получает его
 * как HttpOnly cookie, а доступный JavaScript csrfToken — в теле ответа.
 */
@ApiTags('Сессии')
@Controller('auth')
export class AuthController {
  constructor(@Inject(AuthService) private readonly auth: AuthService,
    @Inject(SESSION_OPTIONS) private readonly options: { ttl: number; secure: boolean }) {}

  @Post('login') @HttpCode(200) @UseGuards(OriginGuard)
  @ApiOperation({ summary: 'Войти: cookie устанавливается браузером, csrfToken нужен для изменений' })
  @ApiBody({ schema: { type: 'object', required: ['email', 'password'], properties: {
    email: { type: 'string', example: 'buyer@example.test' }, password: { type: 'string', example: 'local-password' },
  } } })
  async login(@Body() body: unknown, @Req() request: HttpRequest, @Res({ passthrough: true }) response: HttpResponse) {
    const data = loginInput(body);
    const result = await this.auth.login(data.email, data.password, request.ip ?? request.socket.remoteAddress ?? 'unknown');
    writeCookie(response, result.sessionToken, this.options.ttl, this.options.secure);
    return { user: result.user, csrfToken: result.csrfToken };
  }

  @Get('me') @ApiCookieAuth('session') @RequireSession()
  @ApiOperation({ summary: 'Текущий пользователь; состояние роли читается из PostgreSQL' })
  me(@CurrentPrincipal() identity: Principal) {
    return identity.user;
  }

  @Get('csrf') @ApiCookieAuth('session') @RequireSession()
  @ApiOperation({ summary: 'Получить CSRF-токен для активной cookie-сессии' })
  csrf(@CurrentPrincipal() identity: Principal) {
    return { csrfToken: identity.csrfToken };
  }

  @Post('logout') @HttpCode(200) @ApiCookieAuth('session') @ApiSecurity('csrf') @RequireSession()
  async logout(@CurrentPrincipal() identity: Principal, @Res({ passthrough: true }) response: HttpResponse) {
    await this.auth.logout(identity);
    writeCookie(response, '', 0, this.options.secure);
    return { loggedOut: true };
  }
}
