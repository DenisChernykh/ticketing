import { Injectable, type PipeTransform } from '@nestjs/common';
import { isUuid } from '@ticketing/contracts';
import { AppError } from '@ticketing/core';

/** Преобразует параметр маршрута в проверенный UUID до вызова прикладного сценария. */
@Injectable()
export class ProductQueryUuidPipe implements PipeTransform<unknown, string> {
  transform(value: unknown): string {
    if (!isUuid(value)) throw new AppError('VALIDATION_FAILED', 'Ожидается UUID', 400);
    return value;
  }
}

/**
 * Разбирает необязательный HTTP-параметр limit в целое число от 1 до 100.
 *
 * Только отсутствие параметра выбирает 10. Пробелы, знаки, дроби, экспоненты
 * и повторённые query-параметры не превращаются в число через неявное приведение.
 * Application получает уже проверенное число и не знает синтаксиса строки URL.
 */
@Injectable()
export class ReportLimitPipe implements PipeTransform<unknown, number> {
  transform(value: unknown): number {
    if (value === undefined) return 10;
    if (typeof value !== 'string' || value.length === 0 || /\D/.test(value) || Number(value) < 1 || Number(value) > 100) {
      throw new AppError('VALIDATION_FAILED', 'limit должен быть от 1 до 100', 400);
    }
    return Number(value);
  }
}
