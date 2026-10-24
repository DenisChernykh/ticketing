import {
  beginHttp,
  context,
  traceFields,
  startTelemetry,
  stopTelemetry,
} from '@ticketing/observability';
import 'reflect-metadata';
import { randomUUID } from 'node:crypto';
import {
  ArgumentsHost,
  Catch,
  DynamicModule,
  ExceptionFilter,
  HttpException,
  INestApplication,
  Type,
  Module,
} from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { DocumentBuilder, SwaggerModule } from '@nestjs/swagger';
import type { NextFunction, Request, Response } from 'express';
import { AppError } from './errors';
import { log } from './runtime';

/**
 * Приводит ошибки всех HTTP-сервисов к одному формату с code и requestId.
 * Ожидаемые AppError сохраняют свой статус; неизвестная ошибка получает 500
 * и общее сообщение, а подробности остаются в журнале сервера.
 */
@Catch()
class ProblemFilter implements ExceptionFilter {
  catch(error: unknown, host: ArgumentsHost): void {
    const response = host.switchToHttp().getResponse<Response>();
    const request = host.switchToHttp().getRequest<Request>();
    const status =
      error instanceof AppError
        ? error.status
        : error instanceof HttpException
          ? error.getStatus()
          : 500;
    const code =
      error instanceof AppError
        ? error.code
        : status === 500
          ? 'internal_error'
          : `http_${status}`;
    if (status >= 500)
      log('http.error', {
        code,
        path: request.path,
        error: error instanceof Error ? error.message : String(error),
        requestId: response.getHeader('X-Request-Id'),
      });
    if (response.headersSent) {
      response.destroy();
      return;
    }
    response
      .status(status)
      .type('application/problem+json')
      .json({
        type: `urn:ticketing:problem:${code}`,
        title:
          status === 500
            ? 'Внутренняя ошибка сервера'
            : error instanceof Error
              ? error.message
              : 'Ошибка запроса',
        status,
        code,
        instance: request.path,
        requestId: response.getHeader('X-Request-Id'),
      });
  }
}

/**
 * Подключает общую HTTP-основу и запускает переданный composition-модуль.
 *
 * @remarks
 * Каждый запрос получает идентификатор, журнал результата и запрет HTTP-кеширования.
 * Swagger описывает cookie, CSRF и служебный токен, но описание схем безопасности
 * не выполняет авторизацию: её проверяют guards HTTP-границы и прикладные операции.
 * Shutdown hooks вызывают закрытие ресурсов, зарегистрированных модулем.
 * По умолчанию сервер слушает loopback; порт и состав бизнес-модулей задаёт bootstrap.
 *
 * @param module - Модуль с уже выбранными реализациями портов и контроллерами.
 * @param port - Порт HTTP-сервиса.
 * @param title - Название сервиса в Swagger и журнале запуска.
 * @returns Запущенное приложение, которое владелец процесса может закрыть.
 */
@Module({})
class InstrumentedModule {}

export async function startHttp(
  module: Type<unknown> | DynamicModule,
  port: number,
  title: string,
): Promise<INestApplication> {
  startTelemetry(title);
  const app = await NestFactory.create(
    {
      module: InstrumentedModule,
      imports: [module],
      providers: [
        {
          provide: 'TELEMETRY_LIFECYCLE',
          useValue: { onApplicationShutdown: stopTelemetry },
        },
      ],
    },
    { logger: ['error', 'warn', 'log'], bodyParser: true, rawBody: true },
  );
  app.use((req: Request, res: Response, next: NextFunction) => {
    res.setHeader('X-Request-Id', randomUUID());
    res.setHeader('X-Content-Type-Options', 'nosniff');
    res.setHeader('Cache-Control', 'no-store');
    const began = Date.now();
    res.on('finish', () =>
      log('http.request', {
        method: req.method,
        path: req.path,
        status: res.statusCode,
        durationMs: Date.now() - began,
        requestId: res.getHeader('X-Request-Id'),
        traceId: res.getHeader('X-Trace-Id'),
      }),
    );
    const span = beginHttp(req.headers as Record<string, string>, {
      'http.request.method': req.method,
      'url.path': req.path,
    });
    let ended = false;
    const finish = () => {
      if (ended) return;
      ended = true;
      span.end(res.statusCode);
    };
    res.once('finish', finish);
    res.once('close', finish);
    context.with(span.context, () => {
      const fields = traceFields();
      if (fields.traceId) res.setHeader('X-Trace-Id', fields.traceId);
      next();
    });
  });
  app.useGlobalFilters(new ProblemFilter());
  app.enableShutdownHooks();
  const builder = new DocumentBuilder()
    .setTitle(title)
    .setVersion('1.0')
    .setDescription(
      'Готовое локальное приложение продажи билетов. Деньги указаны в копейках. После входа cookie сохраняется браузером; CSRF из ответа login вставьте в Authorize → csrf.',
    )
    .addCookieAuth(
      'ticketing_session',
      { type: 'apiKey', in: 'cookie' },
      'session',
    )
    .addApiKey({ type: 'apiKey', in: 'header', name: 'X-CSRF-Token' }, 'csrf')
    .addApiKey(
      { type: 'apiKey', in: 'header', name: 'X-Internal-Token' },
      'internal',
    );
  const document = SwaggerModule.createDocument(app, builder.build());
  SwaggerModule.setup('docs', app, document, {
    jsonDocumentUrl: 'openapi.json',
    swaggerOptions: { withCredentials: true, persistAuthorization: true },
  });
  await app.listen(port, process.env.HTTP_HOST ?? '127.0.0.1');
  log('http.listening', { title, port });
  return app;
}
