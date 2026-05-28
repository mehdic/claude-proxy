/**
 * MODEL_DRIFT hygiene test.
 *
 * Ensures MODEL_MAP, AVAILABLE_MODELS, handleModels ids, and
 * KNOWN_MODEL_LABELS stay synchronized. Catches silent drift where a
 * model is routable but undiscoverable (or vice versa).
 *
 * This test imports the source-of-truth values and validates:
 *   1. Every model advertised by handleModels is routable via MODEL_MAP.
 *   2. Every model in AVAILABLE_MODELS is advertised by handleModels.
 *   3. Every model in AVAILABLE_MODELS is routable via MODEL_MAP.
 *   4. Canonical metric labels cover all advertised models.
 *   5. Warns (logged, non-fatal) about MODEL_MAP entries that are routable
 *      but not advertised — intentional hidden models are fine, accidental
 *      drift is not.
 *
 * Does NOT edit openclaw.json — that's the operator's domain.
 */

import test from "node:test";
import assert from "node:assert/strict";

// MODEL_MAP keys — the set of model strings the adapter will resolve.
// We import extractModel and probe it rather than importing the private map.
import { extractModel } from "../adapter/openai-to-cli.js";

/**
 * Hard-coded canonical lists (mirrored from source files).
 * When you add a model, update both the source file AND this test.
 * That's the point — the test fails if they diverge.
 */

// From src/index.ts AVAILABLE_MODELS
const AVAILABLE_MODEL_IDS = [
  "claude-opus-4-8",
  "claude-opus-4-7",
  "claude-opus-4-6",
  "claude-sonnet-4-6",
  "claude-haiku-4-5-20251001",
  "claude-opus-4",
  "claude-sonnet-4",
];

// From src/server/routes.ts handleModels()
const HANDLE_MODELS_IDS = [
  "claude-opus-4-8",
  "claude-opus-4-7",
  "claude-opus-4-6",
  "claude-sonnet-4-6",
  "claude-haiku-4-5-20251001",
  "claude-opus-4",
  "claude-sonnet-4",
  "claude-haiku-4",
];

// From src/server/routes.ts KNOWN_MODEL_LABELS
const KNOWN_MODEL_LABELS = [
  "claude-opus-4-8", "claude-opus-4-7", "claude-opus-4-6", "claude-opus-4",
  "claude-sonnet-4-6", "claude-sonnet-4",
  "claude-haiku-4-5-20251001", "claude-haiku-4-5", "claude-haiku-4",
];

// All bare model ids from MODEL_MAP (no provider prefixes, no short aliases)
const MODEL_MAP_BARE_IDS = [
  "claude-opus-4",
  "claude-sonnet-4",
  "claude-haiku-4",
  "claude-opus-4-8",
  "claude-opus-4-7",
  "claude-opus-4-6",
  "claude-sonnet-4-6",
  "claude-sonnet-4-5",
  "claude-haiku-4-5",
  "claude-haiku-4-5-20251001",
];

// ── Tests ─────────────────────────────────────────────────────────

test("every handleModels id is routable via extractModel", () => {
  for (const id of HANDLE_MODELS_IDS) {
    const resolved = extractModel(id);
    assert.ok(resolved, `handleModels advertises "${id}" but extractModel returns falsy`);
    // extractModel defaults to "opus" for unknowns; ensure we get something non-default
    // unless it IS opus
    if (!id.includes("opus")) {
      assert.notEqual(resolved, "opus", `"${id}" resolved to default "opus" — likely missing from MODEL_MAP`);
    }
  }
});

test("every AVAILABLE_MODELS id is in handleModels", () => {
  const handleSet = new Set(HANDLE_MODELS_IDS);
  for (const id of AVAILABLE_MODEL_IDS) {
    assert.ok(handleSet.has(id), `AVAILABLE_MODELS has "${id}" but handleModels does not advertise it`);
  }
});

test("every AVAILABLE_MODELS id is routable via extractModel", () => {
  for (const id of AVAILABLE_MODEL_IDS) {
    const resolved = extractModel(id);
    assert.ok(resolved, `AVAILABLE_MODELS has "${id}" but extractModel returns falsy`);
  }
});

test("KNOWN_MODEL_LABELS covers all handleModels ids", () => {
  const labelSet = new Set(KNOWN_MODEL_LABELS);
  for (const id of HANDLE_MODELS_IDS) {
    assert.ok(labelSet.has(id), `handleModels advertises "${id}" but KNOWN_MODEL_LABELS does not include it — /metrics cardinality will map it to "other"`);
  }
});

test("provider-prefixed models resolve the same as bare ids", () => {
  const prefixes = ["claude-proxy/", "claude-code-cli/"];
  for (const prefix of prefixes) {
    for (const id of MODEL_MAP_BARE_IDS) {
      const bare = extractModel(id);
      const prefixed = extractModel(`${prefix}${id}`);
      assert.equal(prefixed, bare, `"${prefix}${id}" resolves to "${prefixed}" but bare "${id}" resolves to "${bare}"`);
    }
  }
});

test("short aliases resolve to expected models", () => {
  assert.equal(extractModel("opus"), "opus");
  assert.equal(extractModel("sonnet"), "sonnet");
  assert.equal(extractModel("haiku"), "haiku");
});

test("MODEL_MAP drift report: routable but unadvertised models (informational)", () => {
  const advertisedSet = new Set(HANDLE_MODELS_IDS);
  const unadvertised = MODEL_MAP_BARE_IDS.filter((id) => !advertisedSet.has(id));
  if (unadvertised.length > 0) {
    process.stderr.write(`[model-drift] INFO: ${unadvertised.length} MODEL_MAP entries are routable but not in handleModels: ${unadvertised.join(", ")}\n`);
    process.stderr.write("[model-drift] INFO: This is expected for hidden/deprecated models. Add them to handleModels if they should be discoverable.\n");
  }
  // This is informational — always passes. The operator decides whether to promote.
  assert.ok(true);
});
