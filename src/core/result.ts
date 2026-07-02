// ============================================================================
// Result type
// ============================================================================
//
// flow-core defines its own minimal Result so the package has zero coupling to
// any host's error model. It is structurally identical to the `Result<T, E>`
// used by @octabits-io/foundation, so the two interoperate without adapters.

export interface FlowErrorShape {
  key: string;
  message: string;
}

export type Result<T = void, E extends FlowErrorShape = FlowErrorShape> =
  | { ok: true; value: T }
  | { ok: false; error: E };

export const ok = <T>(value: T): Result<T, never> => ({ ok: true, value });
export const err = <E extends FlowErrorShape>(error: E): Result<never, E> => ({ ok: false, error });
