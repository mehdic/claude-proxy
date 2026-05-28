import test from "node:test";
import assert from "node:assert/strict";
import { estimateCost, normalizeModel, pricingSnapshot } from "../server/pricing.js";

test("normalizes Claude provider prefixes and date suffixes for pricing", () => {
  assert.equal(normalizeModel("anthropic/claude-sonnet-4-6-20260217"), "claude-sonnet-4-6");
  assert.equal(normalizeModel("claude-proxy/claude-haiku-4-5-20251001"), "claude-haiku-4-5");
  assert.equal(normalizeModel("claude-proxy/claude-opus-4-8"), "claude-opus-4-8");
  assert.equal(normalizeModel("sonnet"), "claude-sonnet-4-6");
});

test("estimates Claude token cost with cache read and cache creation rates", () => {
  const estimate = estimateCost("claude-sonnet-4-6", {
    inputTokens: 1_000_000,
    cacheCreationInputTokens: 100_000,
    cachedInputTokens: 200_000,
    outputTokens: 500_000,
    totalTokens: 1_800_000,
  });

  assert.equal(estimate.currency, "USD");
  assert.equal(estimate.input_cost_usd, 3);
  assert.equal(estimate.cache_creation_input_cost_usd, 0.375);
  assert.equal(estimate.cached_input_cost_usd, 0.06);
  assert.equal(estimate.output_cost_usd, 7.5);
  assert.equal(estimate.total_cost_usd, 10.935);
});

test("pricing snapshot exposes an Anthropic pricing book", () => {
  const snapshot = pricingSnapshot();
  assert.equal(snapshot.models["claude-opus-4-8"].inputPer1M, 5);
  assert.equal(snapshot.models["claude-opus-4-7"].inputPer1M, 5);
  assert.equal(snapshot.models["claude-haiku-4-5"].outputPer1M, 5);
});
