import { describe, it, expect } from 'vitest';
import { WIRE_START_PAYLOAD_SCHEMA, resolveStartIdempotencyKey, type WireStartPayload } from './payload';

const base: WireStartPayload = {
  partitionKey: 'tenant-1',
  workflowType: 'enrichment',
  input: {},
};

describe('resolveStartIdempotencyKey', () => {
  it('returns undefined when neither key nor prefix is set', () => {
    expect(resolveStartIdempotencyKey(base, 'job-1')).toBeUndefined();
  });

  it('appends the job id to a prefix, so each tick gets a distinct key', () => {
    const p = { ...base, idempotencyKeyPrefix: 'nightly' };
    expect(resolveStartIdempotencyKey(p, 'job-1')).toBe('nightly:job-1');
    expect(resolveStartIdempotencyKey(p, 'job-2')).toBe('nightly:job-2');
  });

  it('is stable across redeliveries of the same job', () => {
    const p = { ...base, idempotencyKeyPrefix: 'nightly' };
    expect(resolveStartIdempotencyKey(p, 'job-7')).toBe(resolveStartIdempotencyKey(p, 'job-7'));
  });

  it('uses an explicit key verbatim — the "once, ever" case', () => {
    const p = { ...base, idempotencyKey: 'backfill-2026' };
    expect(resolveStartIdempotencyKey(p, 'job-1')).toBe('backfill-2026');
    expect(resolveStartIdempotencyKey(p, 'job-2')).toBe('backfill-2026');
  });

  it('prefers an explicit key over a prefix', () => {
    const p = { ...base, idempotencyKey: 'pinned', idempotencyKeyPrefix: 'nightly' };
    expect(resolveStartIdempotencyKey(p, 'job-1')).toBe('pinned');
  });

  it('treats an empty-string key as explicit rather than falling through to the prefix', () => {
    const p = { ...base, idempotencyKey: '', idempotencyKeyPrefix: 'nightly' };
    expect(resolveStartIdempotencyKey(p, 'job-1')).toBe('');
  });
});

describe('WIRE_START_PAYLOAD_SCHEMA', () => {
  it('accepts a payload carrying only a prefix', () => {
    const parsed = WIRE_START_PAYLOAD_SCHEMA.safeParse({
      partitionKey: 'p', workflowType: 't', idempotencyKeyPrefix: 'nightly',
    });
    expect(parsed.success).toBe(true);
    expect(parsed.success && parsed.data.idempotencyKeyPrefix).toBe('nightly');
    expect(parsed.success && parsed.data.input).toEqual({});
  });

  it('still accepts a payload carrying an explicit key', () => {
    const parsed = WIRE_START_PAYLOAD_SCHEMA.safeParse({
      partitionKey: 'p', workflowType: 't', idempotencyKey: 'once',
    });
    expect(parsed.success && parsed.data.idempotencyKey).toBe('once');
  });
});
