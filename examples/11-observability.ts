/**
 * 11 — Observability: events + spans (gap 10)
 *
 * The engine emits a `FlowEvent` at every transition (run history / metrics) and wraps each step
 * execution in a span. Both are pluggable and no-op by default. Here we use the in-memory
 * recorders to inspect them; in production pass `createPgEventSink` (persists to flow_step_event)
 * and an OpenTelemetry-backed tracer.
 */
import { z } from 'zod';
import { defineStep, buildWorkflow, createRecordingObserver, createRecordingTracer } from '@octabits-io/flow';
import { createInMemoryRuntime } from './runtime';

const inputSchema = z.object({});

const a = defineStep({ type: 'a', workflowInputSchema: inputSchema, outputSchema: z.object({ a: z.number() }), handler: async () => ({ a: 1 }) });
const b = defineStep({
  type: 'b', workflowInputSchema: inputSchema, outputSchema: z.object({ b: z.number() }), dependencies: { a },
  handler: async (ctx) => ({ b: ctx.deps.a.a + 1 }),
});

const wf = buildWorkflow({ type: 'observed', inputSchema, steps: { a, b } });

async function main() {
  const observer = createRecordingObserver();
  const tracer = createRecordingTracer();
  const { engine, registry, drain } = createInMemoryRuntime({ observer, tracer });
  wf.register(registry);

  const started = await wf.start(engine, {});
  if (!started.ok) throw new Error(started.error.message);
  await drain();

  // Run-history event stream:
  console.log('events:');
  for (const e of observer.events) {
    const d = e.durationMs != null ? ` (${e.durationMs}ms)` : '';
    console.log(`  ${e.type}${e.stepKey ? ` ${e.stepKey}` : ''}${d}`);
  }
  // → workflow.started / step.started a / step.completed a / step.started b / step.completed b / workflow.completed

  // One span per step execution:
  console.log('spans:', tracer.spans.map((s) => `${s.attributes['flow.step_key']}${s.ended ? '✓' : ''}`).join(', '));
  // → spans: a✓, b✓
}

main();
