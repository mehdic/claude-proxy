import { readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

export interface ModelPrice {
  inputPer1M: number;
  cacheCreationInputPer1M?: number;
  cachedInputPer1M?: number;
  outputPer1M: number;
  currency?: "USD";
  source?: string;
  updatedAt?: string;
  note?: string;
}

export interface PricingBook {
  updatedAt?: string;
  source?: string;
  models: Record<string, ModelPrice>;
  warnings?: string[];
}

export interface ClaudeTokenUsageBreakdown {
  inputTokens: number;
  cacheCreationInputTokens: number;
  cachedInputTokens: number;
  outputTokens: number;
  totalTokens: number;
}

export interface UsageCostEstimate {
  currency: "USD";
  total_cost_usd: number;
  input_cost_usd: number;
  cache_creation_input_cost_usd: number;
  cached_input_cost_usd: number;
  output_cost_usd: number;
  model: string;
  pricing: {
    input_per_1m: number;
    cache_creation_input_per_1m: number;
    cached_input_per_1m: number;
    output_per_1m: number;
    source: string;
    updated_at: string;
    note?: string;
  };
}

const DEFAULT_UPDATED_AT = "2026-04-30";

// Local reporting estimates only. Claude Proxy still uses the caller's Claude
// CLI/auth path and does not bill or meter requests itself.
const FALLBACK_PRICING: PricingBook = {
  updatedAt: DEFAULT_UPDATED_AT,
  source: "static fallback from Anthropic public pricing; refresh with scripts/update-pricing.mjs",
  models: {
    "claude-opus-4-8": {
      inputPer1M: 5,
      cacheCreationInputPer1M: 6.25,
      cachedInputPer1M: 0.5,
      outputPer1M: 25,
      source: "Anthropic Claude Opus 4.8 public product/pricing page",
      updatedAt: "2026-05-28",
    },
    "claude-opus-4-7": {
      inputPer1M: 5,
      cacheCreationInputPer1M: 6.25,
      cachedInputPer1M: 0.5,
      outputPer1M: 25,
      source: "Anthropic Claude Opus 4.7 public product/pricing page",
      updatedAt: DEFAULT_UPDATED_AT,
    },
    "claude-opus-4-6": {
      inputPer1M: 5,
      cacheCreationInputPer1M: 6.25,
      cachedInputPer1M: 0.5,
      outputPer1M: 25,
      source: "Anthropic public pricing fallback for Opus 4.6",
      updatedAt: DEFAULT_UPDATED_AT,
    },
    "claude-opus-4": {
      inputPer1M: 15,
      cacheCreationInputPer1M: 18.75,
      cachedInputPer1M: 1.5,
      outputPer1M: 75,
      source: "Anthropic public pricing page",
      updatedAt: DEFAULT_UPDATED_AT,
    },
    "claude-sonnet-4-6": {
      inputPer1M: 3,
      cacheCreationInputPer1M: 3.75,
      cachedInputPer1M: 0.3,
      outputPer1M: 15,
      source: "Anthropic public pricing fallback for Sonnet 4.6",
      updatedAt: DEFAULT_UPDATED_AT,
    },
    "claude-sonnet-4-5": {
      inputPer1M: 3,
      cacheCreationInputPer1M: 3.75,
      cachedInputPer1M: 0.3,
      outputPer1M: 15,
      source: "Anthropic public pricing page",
      updatedAt: DEFAULT_UPDATED_AT,
    },
    "claude-sonnet-4": {
      inputPer1M: 3,
      cacheCreationInputPer1M: 3.75,
      cachedInputPer1M: 0.3,
      outputPer1M: 15,
      source: "Anthropic public pricing page",
      updatedAt: DEFAULT_UPDATED_AT,
    },
    "claude-haiku-4-5": {
      inputPer1M: 1,
      cacheCreationInputPer1M: 1.25,
      cachedInputPer1M: 0.1,
      outputPer1M: 5,
      source: "Anthropic public pricing page",
      updatedAt: DEFAULT_UPDATED_AT,
    },
    "claude-haiku-4": {
      inputPer1M: 1,
      cacheCreationInputPer1M: 1.25,
      cachedInputPer1M: 0.1,
      outputPer1M: 5,
      source: "family fallback aligned to Claude Haiku 4.5",
      updatedAt: DEFAULT_UPDATED_AT,
      note: "estimated fallback",
    },
  },
};

let cachedBook: PricingBook | null = null;
let cachedPath: string | null = null;

export function pricingFilePath(): string {
  return process.env.CLAUDE_PROXY_PRICING_FILE || join(homedir(), ".claude-proxy", "pricing.json");
}

export function loadPricingBook(): PricingBook {
  const path = pricingFilePath();
  if (cachedBook && cachedPath === path) return cachedBook;

  cachedPath = path;
  cachedBook = mergePricing(FALLBACK_PRICING, readExternalBook(path));
  return cachedBook;
}

export function pricingSnapshot(): PricingBook {
  return loadPricingBook();
}

export function priceForModel(model: string): ModelPrice {
  const book = loadPricingBook();
  const key = normalizeModel(model);
  return book.models[key] || book.models[String(model || "")] || inferPrice(key, book);
}

export function estimateCost(model: string, usage: ClaudeTokenUsageBreakdown): UsageCostEstimate {
  const normalizedModel = normalizeModel(model);
  const price = priceForModel(normalizedModel);
  const inputTokens = Math.max(0, usage.inputTokens || 0);
  const cacheCreationTokens = Math.max(0, usage.cacheCreationInputTokens || 0);
  const cachedTokens = Math.max(0, usage.cachedInputTokens || 0);
  const outputTokens = Math.max(0, usage.outputTokens || 0);
  const inputRate = price.inputPer1M;
  const cacheCreationRate = price.cacheCreationInputPer1M ?? inputRate * 1.25;
  const cachedRate = price.cachedInputPer1M ?? inputRate * 0.1;
  const outputRate = price.outputPer1M;
  const inputCost = inputTokens / 1_000_000 * inputRate;
  const cacheCreationCost = cacheCreationTokens / 1_000_000 * cacheCreationRate;
  const cachedCost = cachedTokens / 1_000_000 * cachedRate;
  const outputCost = outputTokens / 1_000_000 * outputRate;
  const total = inputCost + cacheCreationCost + cachedCost + outputCost;

  return {
    currency: "USD",
    total_cost_usd: roundUsd(total),
    input_cost_usd: roundUsd(inputCost),
    cache_creation_input_cost_usd: roundUsd(cacheCreationCost),
    cached_input_cost_usd: roundUsd(cachedCost),
    output_cost_usd: roundUsd(outputCost),
    model: normalizedModel,
    pricing: {
      input_per_1m: inputRate,
      cache_creation_input_per_1m: cacheCreationRate,
      cached_input_per_1m: cachedRate,
      output_per_1m: outputRate,
      source: price.source || loadPricingBook().source || "unknown",
      updated_at: price.updatedAt || loadPricingBook().updatedAt || DEFAULT_UPDATED_AT,
      ...(price.note ? { note: price.note } : {}),
    },
  };
}

export function normalizeModel(model: string): string {
  const stripped = String(model || "claude-sonnet-4-6")
    .replace(/^(anthropic|claude-proxy|claude-code-cli|openrouter\/anthropic)\//, "")
    .trim();

  if (stripped === "opus") return "claude-opus-4-6";
  if (stripped === "sonnet") return "claude-sonnet-4-6";
  if (stripped === "haiku") return "claude-haiku-4-5";
  if (/^claude-opus-4-8/.test(stripped)) return "claude-opus-4-8";
  if (/^claude-opus-4-7/.test(stripped)) return "claude-opus-4-7";
  if (/^claude-opus-4-6/.test(stripped)) return "claude-opus-4-6";
  if (/^claude-opus-4/.test(stripped)) return "claude-opus-4";
  if (/^claude-sonnet-4-6/.test(stripped)) return "claude-sonnet-4-6";
  if (/^claude-sonnet-4-5/.test(stripped)) return "claude-sonnet-4-5";
  if (/^claude-sonnet-4/.test(stripped)) return "claude-sonnet-4";
  if (/^claude-haiku-4-5/.test(stripped)) return "claude-haiku-4-5";
  if (/^claude-haiku-4/.test(stripped)) return "claude-haiku-4";
  return stripped;
}

function readExternalBook(path: string): PricingBook | null {
  try {
    const parsed = JSON.parse(readFileSync(path, "utf-8")) as PricingBook;
    if (!parsed || typeof parsed !== "object" || !parsed.models || typeof parsed.models !== "object") return null;
    return parsed;
  } catch {
    return null;
  }
}

function mergePricing(base: PricingBook, external: PricingBook | null): PricingBook {
  if (!external) return base;
  return {
    updatedAt: external.updatedAt || base.updatedAt,
    source: external.source || base.source,
    warnings: external.warnings,
    models: { ...base.models, ...external.models },
  };
}

function inferPrice(model: string, book: PricingBook): ModelPrice {
  if (model.includes("opus")) return book.models["claude-opus-4-6"];
  if (model.includes("haiku")) return book.models["claude-haiku-4-5"];
  return book.models["claude-sonnet-4-6"];
}

function roundUsd(value: number): number {
  return Math.round(value * 1_000_000) / 1_000_000;
}
