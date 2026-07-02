// ============================================================================
// Logger
// ============================================================================
//
// flow-core takes a minimal structured logger so it can emit lifecycle events
// without depending on any host's logging stack. Hosts adapt their own logger
// to this shape; a no-op default is used when none is provided.

export type LogAttributes = Record<string, unknown>;

export interface Logger {
  info(message: string, attributes?: LogAttributes): void;
  warn(message: string, attributes?: LogAttributes): void;
  error(message: string, error?: Error, attributes?: LogAttributes): void;
}

export const noopLogger: Logger = {
  info() {},
  warn() {},
  error() {},
};
