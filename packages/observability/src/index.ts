import {
  context,
  propagation,
  trace,
  metrics,
  SpanStatusCode,
  ROOT_CONTEXT,
  type Attributes,
  type Context,
} from '@opentelemetry/api';
import { NodeSDK } from '@opentelemetry/sdk-node';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-proto';
import { OTLPMetricExporter } from '@opentelemetry/exporter-metrics-otlp-proto';
import { PeriodicExportingMetricReader } from '@opentelemetry/sdk-metrics';

export type TraceCarrier = Record<string, string>;
let sdk: NodeSDK | undefined;
/** Bootstrap calls this before starting HTTP, consumers or workers. No business dependency on the SDK. */
export function startTelemetry(service: string): void {
  if (sdk || process.env.OTEL_SDK_DISABLED === 'true') return;
  process.env.OTEL_SERVICE_NAME = service;
  process.env.OTEL_EXPORTER_OTLP_ENDPOINT ??= 'http://127.0.0.1:34318';
  sdk = new NodeSDK({
    traceExporter: new OTLPTraceExporter(),
    metricReader: new PeriodicExportingMetricReader({
      exporter: new OTLPMetricExporter(),
      exportIntervalMillis: 5000,
    }),
  });
  sdk.start();
}
export async function stopTelemetry() {
  await sdk?.shutdown();
  sdk = undefined;
}
export function carrier(): TraceCarrier {
  const result: TraceCarrier = {};
  propagation.inject(context.active(), result);
  return result;
}
export function traceFields() {
  const s = trace.getSpan(context.active())?.spanContext();
  return s ? { traceId: s.traceId, spanId: s.spanId } : {};
}
export async function traced<T>(
  name: string,
  attributes: Attributes,
  operation: () => Promise<T>,
  incoming?: TraceCarrier,
  linked = false,
): Promise<T> {
  const parent = incoming
    ? propagation.extract(ROOT_CONTEXT, incoming)
    : context.active();
  const source = trace.getSpanContext(parent);
  return trace
    .getTracer('ticketing')
    .startActiveSpan(
      name,
      {
        attributes,
        ...(linked && source ? { links: [{ context: source }] } : {}),
      },
      linked ? ROOT_CONTEXT : parent,
      async (span) => {
        try {
          return await operation();
        } catch (e) {
          span.recordException(e instanceof Error ? e : String(e));
          span.setStatus({ code: SpanStatusCode.ERROR });
          throw e;
        } finally {
          span.end();
        }
      },
    );
}
export function count(name: string, attributes: Attributes = {}, value = 1) {
  metrics.getMeter('ticketing').createCounter(name).add(value, attributes);
}
export function measure(
  name: string,
  value: number,
  attributes: Attributes = {},
) {
  metrics.getMeter('ticketing').createHistogram(name).record(value, attributes);
}
export function gauge(
  name: string,
  value: number,
  attributes: Attributes = {},
) {
  metrics.getMeter('ticketing').createGauge(name).record(value, attributes);
}
export function beginHttp(
  incoming: TraceCarrier,
  attributes: Attributes,
): { context: Context; end(status: number): void } {
  const parent = propagation.extract(ROOT_CONTEXT, incoming);
  const span = trace
    .getTracer('ticketing')
    .startSpan('http.request', { attributes }, parent);
  const started = performance.now();
  return {
    context: trace.setSpan(parent, span),
    end(status) {
      span.setAttribute('http.response.status_code', status);
      if (status >= 500) span.setStatus({ code: SpanStatusCode.ERROR });
      span.end();
      measure('http_duration_ms', performance.now() - started, {
        service: process.env.OTEL_SERVICE_NAME ?? '',
        method: String(attributes['http.request.method']),
        status,
      });
    },
  };
}
export { context };
