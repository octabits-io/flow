import type { StepHandlerRegistry, StepRegistration } from './types';

/**
 * Creates an in-memory step handler registry.
 * Populate it at application startup (e.g. by registering every workflow's steps).
 * Each registration carries the handler plus its optional retry/timeout policy.
 */
export function createStepHandlerRegistry<TContext = unknown>(): StepHandlerRegistry<TContext> {
  const entries = new Map<string, StepRegistration<TContext>>();

  return {
    register(type, handler, options) {
      entries.set(type, {
        handler,
        retry: options?.retry,
        timeoutMs: options?.timeoutMs,
        delayMs: options?.delayMs,
        waitForEvent: options?.waitForEvent,
        map: options?.map,
        childType: options?.childType,
        subWorkflowDefinition: options?.subWorkflowDefinition,
        compensate: options?.compensate,
      });
    },
    get(type) {
      return entries.get(type)?.handler;
    },
    getRegistration(type) {
      return entries.get(type);
    },
    has(type) {
      return entries.has(type);
    },
    types() {
      return Array.from(entries.keys());
    },
  };
}
