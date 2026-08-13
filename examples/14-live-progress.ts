/**
 * 14 — Live progress: FlowObserver → fan-out hub → SSE
 *
 * Flow ships no dashboard. What it ships is the two seams you need to build one:
 * `toPublicWorkflow()` for the read side (fetch the current state) and
 * `FlowObserver` for the live side (every transition, as it happens).
 *
 * This wires the live side end to end: the engine's observer feeds an in-process
 * hub, subscribers are filtered by `partitionKey` so one tenant never sees
 * another's events, and each event is framed as an SSE message ready to write to
 * a `Response` body.
 *
 * The whole hub is ~20 lines. The interesting part is what it does NOT do —
 * see the deployment note at the bottom.
 */
import { z } from 'zod';
import { defineStep, buildWorkflow } from 'octaflow';
import type { FlowEvent, FlowObserver } from 'octaflow';
import { createInMemoryRuntime } from './runtime';

// ---------------------------------------------------------------------------
// The hub: fan out engine transitions to whoever is listening for that partition
// ---------------------------------------------------------------------------

interface Subscriber {
  /** Only events for this partition reach this subscriber. */
  partitionKey: string;
  send(frame: string): void;
}

function createEventHub() {
  const subscribers = new Set<Subscriber>();

  /** Server-Sent Events framing: an event name, a JSON payload, a blank line. */
  function toSseFrame(event: FlowEvent): string {
    const { type, workflowId, stepKey, at, durationMs, error } = event;
    // Note what is NOT forwarded: partitionKey is a routing concern, not the
    // client's business, and internal ids stay internal.
    const data = { workflowId, stepKey, at, durationMs, error };
    return `event: ${type}\ndata: ${JSON.stringify(data)}\n\n`;
  }

  return {
    subscribe(sub: Subscriber): () => void {
      subscribers.add(sub);
      return () => subscribers.delete(sub);
    },

    /**
     * Hand this to `createWorkflowEngine({ observer })`. The engine never awaits
     * it, so a slow or throwing subscriber can't stall a step — but that also
     * means delivery is best-effort. Durable history is a separate concern:
     * `createPgEventSink()` from `octaflow/store-pg` persists the same stream.
     */
    observer: {
      record(event) {
        const frame = toSseFrame(event);
        for (const sub of subscribers) {
          if (sub.partitionKey !== event.partitionKey) continue;
          try {
            sub.send(frame);
          } catch {
            // One broken subscriber must not affect the others.
          }
        }
      },
    } satisfies FlowObserver,
  };
}

// ---------------------------------------------------------------------------
// A workflow to watch
// ---------------------------------------------------------------------------

const input = z.object({});
const out = z.object({ ok: z.literal(true) });
const work = async () => ({ ok: true as const });

const fetchDraft = defineStep({ type: 'fetchDraft', workflowInputSchema: input, outputSchema: out, handler: work });
const summarize = defineStep({ type: 'summarize', workflowInputSchema: input, outputSchema: out, dependencies: { fetchDraft }, handler: work });
const publish = defineStep({ type: 'publish', workflowInputSchema: input, outputSchema: out, dependencies: { summarize }, handler: work });

const wf = buildWorkflow({ type: 'publish-article', inputSchema: input, steps: { fetchDraft, summarize, publish } });

async function main() {
  const hub = createEventHub();

  // Two "browsers", each watching a different tenant.
  const acme: string[] = [];
  const globex: string[] = [];
  hub.subscribe({ partitionKey: 'acme', send: (f) => acme.push(f) });
  hub.subscribe({ partitionKey: 'globex', send: (f) => globex.push(f) });

  // One engine per partition — the observer is shared, routing is by partitionKey.
  for (const partitionKey of ['acme', 'globex']) {
    const { engine, registry, drain } = createInMemoryRuntime({ partitionKey, observer: hub.observer });
    wf.register(registry);
    await wf.start(engine, {});
    await drain();
  }

  console.log(`acme received ${acme.length} frames, globex ${globex.length} — neither saw the other's`);
  console.log('\nwhat the browser actually reads:\n');
  console.log(acme.slice(0, 3).join(''));
}

main();

/**
 * DEPLOYMENT NOTE — this hub is per-process.
 *
 * Subscribers only receive events emitted by an engine in the *same* process. A
 * real deployment runs the HTTP server and the step workers separately, so a
 * transition that happens on a worker never reaches a browser attached to the
 * server. To bridge that gap you need a shared channel; Postgres LISTEN/NOTIFY
 * is the obvious one since the database is already there. Two things bite:
 *
 * - **Use a dedicated connection, never a pooled one.** Pools reset connections
 *   between checkouts and silently drop the LISTEN registration — the channel
 *   then looks healthy and delivers nothing.
 * - **Bypass transaction-mode poolers (PgBouncer).** LISTEN does not survive
 *   transaction pooling; connect to the database directly, exactly as pg-boss
 *   already requires.
 *
 * And NOTIFY is not durable: a subscriber that was disconnected missed those
 * events for good. If a client must be able to catch up, pair the live stream
 * with `createPgEventSink()` for the persisted history and have the client
 * replay from its last-seen point on reconnect, then switch to the stream.
 */
