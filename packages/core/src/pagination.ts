import { AppError } from './errors';
import { isUuid, type PageInput } from '@ticketing/contracts';
export function pageInput(query: Record<string, unknown>): PageInput {
  const limit = query.limit === undefined ? 20 : Number(query.limit);
  if (
    (query.limit !== undefined && (typeof query.limit !== 'string' || !/^\d+$/.test(query.limit))) ||
    !Number.isInteger(limit) ||
    limit < 1 ||
    limit > 100 ||
    (query.cursor !== undefined && (typeof query.cursor !== 'string' || query.cursor.length > 2048))
  )
    throw new AppError('INVALID_PAGE', 'limit: 1–100; cursor: строка', 400);
  return { limit, cursor: query.cursor as string | undefined };
}
export function encodeCursor(value: unknown): string {
  return Buffer.from(JSON.stringify(value)).toString('base64url');
}
export function decodeCursor(
  raw: string | undefined,
  scope: string,
): { date: string; id: string; revision?: string } | undefined {
  if (!raw) return;
  try {
    const v = JSON.parse(Buffer.from(raw, 'base64url').toString());
    if (
      v.scope !== scope ||
      typeof v.date !== 'string' ||
      !Number.isFinite(Date.parse(v.date)) ||
      !isUuid(v.id) ||
      (v.revision !== undefined && typeof v.revision !== 'string')
    )
      throw new Error();
    return v;
  } catch {
    throw new AppError('INVALID_CURSOR', 'Курсор не подходит к этому списку', 400);
  }
}
