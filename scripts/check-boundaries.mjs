#!/usr/bin/env node
/**
 * Enforces the package's internal dependency tree. Run as part of `lint`.
 *
 * Layers live under src/<layer>/. The default entry (src/index.ts) re-exports
 * only `core`, so importing `@octabits-io/flow` never pulls in the AI SDK, pg,
 * or pg-boss. These rules keep that guarantee true:
 *
 *   core               → depends on nothing internal; forbidden externals below
 *   ai                 → may use core; forbidden: pg, pg-boss
 *   store-pg           → may use core; forbidden: ai/@ai-sdk, pg-boss
 *   dispatcher-pgboss  → may use core; forbidden: ai/@ai-sdk, pg
 *   (root index.ts)    → may use core only
 *
 * ai and the two adapters may never depend on each other.
 */
import { readdirSync, readFileSync, statSync } from 'node:fs';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const SRC = resolve(dirname(fileURLToPath(import.meta.url)), '..', 'src');

/** layer → { internal: allowed internal layers, externals: forbidden external package names } */
const RULES = {
  core: { internal: [], externals: ['ai', '@ai-sdk', 'pg', 'pg-boss'] },
  ai: { internal: ['core'], externals: ['pg', 'pg-boss'] },
  'store-pg': { internal: ['core'], externals: ['ai', '@ai-sdk', 'pg-boss'] },
  'dispatcher-pgboss': { internal: ['core'], externals: ['ai', '@ai-sdk', 'pg'] },
  // the root entry (src/index.ts) — files directly under src/
  '': { internal: ['core'], externals: ['ai', '@ai-sdk', 'pg', 'pg-boss'] },
};
const LAYERS = ['core', 'ai', 'store-pg', 'dispatcher-pgboss'];

function walk(dir) {
  const out = [];
  for (const entry of readdirSync(dir)) {
    const full = join(dir, entry);
    if (statSync(full).isDirectory()) out.push(...walk(full));
    else if (full.endsWith('.ts')) out.push(full);
  }
  return out;
}

/** layer a file belongs to (first path segment under src/, or '' for root files) */
function layerOf(absPath) {
  const rel = relative(SRC, absPath);
  const seg = rel.split(/[\\/]/);
  // First segment names the layer for both files (core/engine.ts) and the
  // layer directory itself (core, as targeted by a '../core' import).
  return LAYERS.includes(seg[0]) ? seg[0] : '';
}

/** package name of a bare import specifier ('@ai-sdk/provider' → '@ai-sdk/provider', 'pg-boss' → 'pg-boss') */
function pkgName(spec) {
  if (spec.startsWith('@')) {
    const [scope, name] = spec.split('/');
    return name ? `${scope}/${name}` : scope;
  }
  return spec.split('/')[0];
}

const IMPORT_RE = /\b(?:import|export)\b[^'"]*?\bfrom\s*['"]([^'"]+)['"]|\bimport\s*['"]([^'"]+)['"]|\brequire\(\s*['"]([^'"]+)['"]\s*\)/g;

const violations = [];

for (const file of walk(SRC)) {
  const layer = layerOf(file);
  const rule = RULES[layer];
  const src = readFileSync(file, 'utf8');
  const rel = relative(SRC, file);

  for (const m of src.matchAll(IMPORT_RE)) {
    const spec = m[1] ?? m[2] ?? m[3];
    if (!spec) continue;

    if (spec.startsWith('.')) {
      // internal (relative) import — find the target layer
      const targetLayer = layerOf(resolve(dirname(file), spec));
      if (targetLayer === layer) continue; // same layer, fine
      if (!rule.internal.includes(targetLayer)) {
        violations.push(`${rel}: '${layer || '(root)'}' may not import internal layer '${targetLayer || '(root)'}'  →  ${spec}`);
      }
    } else {
      // external import — check forbidden package list
      const name = pkgName(spec);
      const forbidden = rule.externals.find((f) => (f === '@ai-sdk' ? name.startsWith('@ai-sdk') : name === f));
      if (forbidden) {
        violations.push(`${rel}: '${layer || '(root)'}' may not depend on external '${name}'  →  ${spec}`);
      }
    }
  }
}

if (violations.length > 0) {
  console.error('✗ flow dependency-boundary violations:\n');
  for (const v of violations) console.error('  ' + v);
  console.error(`\n${violations.length} violation(s). See scripts/check-boundaries.mjs for the rules.`);
  process.exit(1);
}

console.log('✓ flow dependency boundaries OK');
