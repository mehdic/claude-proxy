/**
 * Tests for canonicalizeModelLabel (src/server/routes.ts) — the function
 * that bounds /metrics cardinality by reducing arbitrary client model
 * strings to a fixed label set.
 *
 * Pinned implementation here (mirrors production) — drift between this
 * test and production breaks the test, which is the alarm we want.
 */

import test from "node:test";
import assert from "node:assert/strict";

const KNOWN_MODEL_LABELS = new Set([
  "claude-opus-4-8", "claude-opus-4-7", "claude-opus-4-6", "claude-opus-4",
  "claude-sonnet-4-6", "claude-sonnet-4",
  "claude-haiku-4-5-20251001", "claude-haiku-4-5", "claude-haiku-4",
]);
function canonicalizeModelLabel(model: string | undefined): string {
  if (!model) return "unknown";
  const stripped = model.replace(/^(claude-proxy|claude-code-cli)\//, "");
  return KNOWN_MODEL_LABELS.has(stripped) ? stripped : "other";
}

test("strips claude-proxy/ provider prefix", () => {
  assert.equal(canonicalizeModelLabel("claude-proxy/claude-opus-4-8"), "claude-opus-4-8");
});

test("strips claude-code-cli/ legacy provider prefix", () => {
  assert.equal(canonicalizeModelLabel("claude-code-cli/claude-haiku-4-5-20251001"), "claude-haiku-4-5-20251001");
});

test("known bare model id passes through unchanged", () => {
  assert.equal(canonicalizeModelLabel("claude-sonnet-4-6"), "claude-sonnet-4-6");
});

test("unknown ids collapse to 'other' (cardinality guard)", () => {
  assert.equal(canonicalizeModelLabel("openai/gpt-5"), "other");
  assert.equal(canonicalizeModelLabel("totally-fake-model"), "other");
  assert.equal(canonicalizeModelLabel("claude-opus-99-99"), "other");
});

test("empty/undefined → 'unknown'", () => {
  assert.equal(canonicalizeModelLabel(undefined), "unknown");
  assert.equal(canonicalizeModelLabel(""), "unknown");
});

test("provider prefix on unknown id still collapses to 'other'", () => {
  assert.equal(canonicalizeModelLabel("claude-proxy/something-weird"), "other");
});
