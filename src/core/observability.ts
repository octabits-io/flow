import type { WorkflowId, StepId } from './types';

// ============================================================================
// Run history / metrics — FlowObserver (gap 10)
// ============================================================================

/** Lifecycle event types the engine emits at every workflow/step transition. */
export type FlowEventType =
  | 'workflow.started'
  | 'workflow.completed'
  | 'workflow.failed'
  | 'workflow.cancelled'
  | 'step.started'
  | 'step.completed'
  | 'step.failed'
  | 'step.retrying'
  | 'step.skipped'
  | 'step.waiting'
  | 'step.resumed'
  | 'step.mapping'
  | 'step.compensating'
  | 'step.compensated';

/**
 * An append-only run-history record. The engine emits one per transition; an observer can
 * persist it (a timeline), turn it into OpenTelemetry metrics (counters/histograms), or both.
 */
export interface FlowEvent {
  type: FlowEventType;
  /** ISO timestamp of the transition. */
  at: string;
  partitionKey: string;
  workflowId: WorkflowId;
  workflowType?: string;
  stepId?: StepId;
  stepKey?: string;
  stepType?: string;
  /** Execution attempt number (for step events). */
  attempt?: number;
  /** Wall-clock duration of the step run in ms (for step.completed / step.failed). */
  durationMs?: number;
  error?: string;
}

/**
 * Sink for run-history / metrics. Implementations **must not throw** and should be cheap
 * (fire-and-forget) — the engine guards calls but never awaits them. Wire a Postgres event
 * sink for history, or an OTel-meter adapter for metrics. Defaults to a no-op.
 */
export interface FlowObserver {
  record(event: FlowEvent): void;
}

export const noopObserver: FlowObserver = { record() {} };

/** In-memory observer that accumulates events — for tests and introspection. */
export interface RecordingObserver extends FlowObserver {
  readonly events: FlowEvent[];
  clear(): void;
}

export function createRecordingObserver(): RecordingObserver {
  const events: FlowEvent[] = [];
  return {
    events,
    record(event) {
      events.push(event);
    },
    clear() {
      events.length = 0;
    },
  };
}

// ============================================================================
// Tracing — FlowTracer (gap 10)
// ============================================================================

/** A minimal span handle. Maps 1:1 onto an OpenTelemetry span in an adapter. */
export interface FlowSpan {
  setAttribute(key: string, value: string | number | boolean): void;
  recordError(error: Error): void;
  end(): void;
}

/**
 * A minimal tracer. The engine wraps each `executeStep` in a span. Provide an OTel-backed
 * adapter (`startSpan` → `tracer.startSpan`) to get distributed traces; defaults to a no-op.
 */
export interface FlowTracer {
  startSpan(name: string, attributes?: Record<string, string | number | boolean>): FlowSpan;
}

const noopSpan: FlowSpan = { setAttribute() {}, recordError() {}, end() {} };
export const noopTracer: FlowTracer = { startSpan: () => noopSpan };

/** A span captured in memory — for tests. */
export interface RecordedSpan {
  name: string;
  attributes: Record<string, string | number | boolean>;
  error?: string;
  ended: boolean;
}

export interface RecordingTracer extends FlowTracer {
  readonly spans: RecordedSpan[];
}

export function createRecordingTracer(): RecordingTracer {
  const spans: RecordedSpan[] = [];
  return {
    spans,
    startSpan(name, attributes = {}) {
      const rec: RecordedSpan = { name, attributes: { ...attributes }, ended: false };
      spans.push(rec);
      return {
        setAttribute(key, value) {
          rec.attributes[key] = value;
        },
        recordError(error) {
          rec.error = error.message;
        },
        end() {
          rec.ended = true;
        },
      };
    },
  };
}
