import { Pool } from 'pg';
import Cursor from 'pg-cursor';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import type { Response } from 'express';
import { AppError } from './errors';
import { count, measure } from '@ticketing/observability';
export type StreamResponse = Response;
/** Separate bounded pool: an export cannot consume the write pool. Generator owns its cursor. */
export class SqlRowStream {
  private readonly pool: Pool;
  constructor(url: string) {
    this.pool = new Pool({
      connectionString: url,
      max: 2,
      connectionTimeoutMillis: 5000,
      application_name: 'analytics-export',
      options: '-c statement_timeout=300000 -c idle_in_transaction_session_timeout=35000',
    });
  }
  async *rows<T>(sql: string, values: unknown[], signal: AbortSignal): AsyncIterable<T> {
    signal.throwIfAborted();
    const client = await this.pool.connect();
    let released = false;
    const destroy = () => {
      if (!released) {
        released = true;
        client.release(true);
      }
    };
    signal.addEventListener('abort', destroy, { once: true });
    let cursor: Cursor | undefined;
    try {
      signal.throwIfAborted();
      await client.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
      cursor = client.query(new Cursor(sql, values));
      while (true) {
        signal.throwIfAborted();
        const result = await cursor.read(500);
        if (!result.length) break;
        for (const row of result) {
          signal.throwIfAborted();
          yield row as T;
        }
      }
      await cursor.close();
      cursor = undefined;
      await client.query('COMMIT');
    } catch (e) {
      destroy();
      throw e;
    } finally {
      signal.removeEventListener('abort', destroy);
      if (!released) {
        try {
          if (cursor) {
            await cursor.close();
            await client.query('ROLLBACK');
          }
        } catch {
          destroy();
        }
        if (!released) {
          released = true;
          client.release();
        }
      }
    }
  }
  close() {
    return this.pool.end();
  }
}
export interface ExportFilter {
  from?: string;
  to?: string;
  eventId?: string;
}
export function exportFilter(query: Record<string, unknown>): ExportFilter {
  const result: ExportFilter = {};
  for (const key of ['from', 'to'] as const) {
    const v = query[key];
    if (v === undefined) continue;
    if (
      typeof v !== 'string' ||
      !/^\d{4}-\d{2}-\d{2}$/.test(v) ||
      !Number.isFinite(Date.parse(v)) ||
      new Date(v).toISOString().slice(0, 10) !== v
    )
      throw new AppError('INVALID_PERIOD', 'Дата должна иметь вид YYYY-MM-DD', 400);
    result[key] = v;
  }
  if (result.from && result.to && result.from > result.to)
    throw new AppError('INVALID_PERIOD', 'Начало периода позже конца', 400);
  if (query.eventId !== undefined) {
    if (
      typeof query.eventId !== 'string' ||
      !/^[a-f0-9]{8}-[a-f0-9]{4}-[1-8][a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/i.test(query.eventId)
    )
      throw new AppError('INVALID_EVENT', 'Ожидается UUID мероприятия', 400);
    result.eventId = query.eventId;
  }
  return result;
}
/** Transport owns deadlines and disconnect handling all the way to the database iterator. */
export async function streamCsv(
  response: Response,
  source: (signal: AbortSignal) => Promise<AsyncIterable<string | Uint8Array>>,
): Promise<void> {
  const stop = new AbortController();
  const started = performance.now();
  const abort = () => stop.abort(new Error('Export aborted'));
  let idle: ReturnType<typeof setTimeout>;
  const reset = () => {
    clearTimeout(idle);
    idle = setTimeout(abort, 30000);
    idle.unref();
  };
  const total = setTimeout(abort, 300000),
    first = setTimeout(abort, 5000);
  total.unref();
  first.unref();
  response.once('close', abort);
  reset();
  let iterator: AsyncIterator<string | Uint8Array> | undefined;
  try {
    const iterable = await source(stop.signal);
    iterator = iterable[Symbol.asyncIterator]();
    const initial = await iterator.next();
    clearTimeout(first);
    stop.signal.throwIfAborted();
    response
      .status(200)
      .type('text/csv; charset=utf-8')
      .setHeader('Content-Disposition', 'attachment; filename="orders.csv"');
    const body = async function* () {
      try {
        if (!initial.done) {
          reset();
          yield initial.value;
        }
        while (true) {
          const next = await iterator!.next();
          if (next.done) break;
          reset();
          yield next.value;
        }
      } finally {
        await iterator?.return?.();
      }
    };
    await pipeline(Readable.from(body(), { objectMode: false, highWaterMark: 16384 }), response, {
      signal: stop.signal,
    });
    count('csv_exports', { result: 'completed' });
  } catch (error) {
    count('csv_exports', { result: 'failed' });
    if (response.headersSent || response.destroyed) response.destroy();
    else throw new AppError('EXPORT_UNAVAILABLE', 'Выгрузка временно недоступна', 503);
  } finally {
    clearTimeout(total);
    clearTimeout(first);
    clearTimeout(idle!);
    response.off('close', abort);
    stop.abort();
    try {
      await iterator?.return?.();
    } catch {}
    measure('csv_duration_ms', performance.now() - started);
  }
}
export function csvCell(value: unknown): string {
  let text = value === null || value === undefined ? '' : String(value);
  if (/^[\s]*[=+@-]/.test(text) && typeof value !== 'number') text = "'" + text;
  return '"' + text.replaceAll('"', '""') + '"';
}
