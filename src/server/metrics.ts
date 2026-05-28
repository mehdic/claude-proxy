/**
 * Prometheus-format /metrics endpoint.
 *
 * Hand-rolled (no prom-client dep) to keep dependency surface small.
 * The exposition format we need is plain text with TYPE/HELP comments
 * and `name{label="value"} number\n` lines — straightforward.
 *
 * Cardinality discipline: we never label by request id, prompt hash,
 * full model id from request (only canonical ids from MODEL_MAP), or
 * user-controlled strings. Reasons for fallback are from a fixed
 * allowlist defined in routes.ts classifyFallbackReason().
 *
 * Counters live where they're produced (poolCounters in session-pool.ts,
 * fallbackCounters in routes.ts) and we read them at scrape time.
 */

import type { Request, Response } from "express";
import { poolCounters, poolStats } from "../subprocess/session-pool.js";
import { stickyPoolCounters, stickyPoolStats, resetStickyPoolForTests } from "../subprocess/sticky-session-pool.js";
import { fallbackCounters } from "./routes.js";
import { defaultRuntime } from "../subprocess/runtime.js";
import type { ClaudeTokenUsageBreakdown, UsageCostEstimate } from "./pricing.js";
import { traceStore } from "../trace/store.js";
import type { ProtocolErrorClass } from "../errors.js";

// Per-request counters maintained inline by the chat-completion handlers.
// Recorded with a fixed label set: runtime + canonical model + status.
export interface RequestRecord {
  runtime: "stream-json" | "print";
  model: string;
  status: "ok" | "error";
  durationMs: number;
}

interface RequestBucket {
  count: number;
  sumDurationMs: number;
  // Histogram buckets in ms — fixed set keeps cardinality bounded
  buckets: { [le: number]: number };
}

const HIST_BUCKETS_MS = [100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000, 120000];
const requestRecords: Map<string, RequestBucket> = new Map();
const subprocessSpawnFailures: Record<string, number> = {};
const tokenCounters: Record<string, number> = {};
const costCounters: Record<string, number> = {};
const errorClassCounters: Record<string, number> = {};
export type ToolCallParseOutcome = "emitted" | "no_call" | "malformed" | "rejected";
const toolCallParseCounters: Record<ToolCallParseOutcome, number> & { total_calls: number } = {
  emitted: 0,
  no_call: 0,
  malformed: 0,
  rejected: 0,
  total_calls: 0,
};

/** Call from chat handlers when a request finishes. */
export function recordRequest(rec: RequestRecord): void {
  const key = `${rec.runtime}|${rec.model}|${rec.status}`;
  let bucket = requestRecords.get(key);
  if (!bucket) {
    bucket = { count: 0, sumDurationMs: 0, buckets: Object.fromEntries(HIST_BUCKETS_MS.map((b) => [b, 0])) };
    requestRecords.set(key, bucket);
  }
  bucket.count++;
  bucket.sumDurationMs += rec.durationMs;
  for (const le of HIST_BUCKETS_MS) {
    if (rec.durationMs <= le) bucket.buckets[le]++;
  }
}

export function recordSpawnFailure(runtime: "stream-json" | "print"): void {
  subprocessSpawnFailures[runtime] = (subprocessSpawnFailures[runtime] || 0) + 1;
}

export function recordErrorClass(cls: ProtocolErrorClass): void {
  errorClassCounters[cls] = (errorClassCounters[cls] || 0) + 1;
}

export function recordToolCallParse(outcome: ToolCallParseOutcome, callCount: number): void {
  toolCallParseCounters[outcome]++;
  toolCallParseCounters.total_calls += Math.max(0, callCount);
}

export function recordTokenUsage(
  model: string,
  usage: ClaudeTokenUsageBreakdown,
  cost: UsageCostEstimate | undefined,
  estimated: boolean,
): void {
  const labels = { model: canonicalizeMetricModel(model), estimated: estimated ? "true" : "false" };
  addLabeled(tokenCounters, "claude_proxy_tokens_total", usage.inputTokens || 0, { ...labels, direction: "input" });
  addLabeled(tokenCounters, "claude_proxy_tokens_total", usage.cacheCreationInputTokens || 0, { ...labels, direction: "cache_creation_input" });
  addLabeled(tokenCounters, "claude_proxy_tokens_total", usage.cachedInputTokens || 0, { ...labels, direction: "cached_input" });
  addLabeled(tokenCounters, "claude_proxy_tokens_total", usage.outputTokens || 0, { ...labels, direction: "output" });
  addLabeled(tokenCounters, "claude_proxy_tokens_total", usage.totalTokens || 0, { ...labels, direction: "total" });
  if (cost) addLabeled(costCounters, "claude_proxy_estimated_cost_usd_total", cost.total_cost_usd, labels);
}

function escapeLabel(v: string): string {
  return v.replace(/\\/g, "\\\\").replace(/"/g, '\\"').replace(/\n/g, "\\n");
}

function canonicalizeMetricModel(model: string): string {
  const stripped = String(model || "").replace(/^(anthropic|claude-proxy|claude-code-cli|openrouter\/anthropic)\//, "");
  if (stripped === "opus") return "claude-opus-4-6";
  if (stripped === "sonnet") return "claude-sonnet-4-6";
  if (stripped === "haiku") return "claude-haiku-4-5";
  if (stripped.startsWith("claude-opus-4-8")) return "claude-opus-4-8";
  if (stripped.startsWith("claude-opus-4-7")) return "claude-opus-4-7";
  if (stripped.startsWith("claude-opus-4-6")) return "claude-opus-4-6";
  if (stripped.startsWith("claude-opus-4")) return "claude-opus-4";
  if (stripped.startsWith("claude-sonnet-4-6")) return "claude-sonnet-4-6";
  if (stripped.startsWith("claude-sonnet-4-5")) return "claude-sonnet-4-5";
  if (stripped.startsWith("claude-sonnet-4")) return "claude-sonnet-4";
  if (stripped.startsWith("claude-haiku-4-5")) return "claude-haiku-4-5";
  if (stripped.startsWith("claude-haiku-4")) return "claude-haiku-4";
  return stripped ? "other" : "unknown";
}

function labeledKey(name: string, labels?: Record<string, string>): string {
  if (!labels || Object.keys(labels).length === 0) return name;
  const parts = Object.entries(labels)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([k, v]) => `${k}="${escapeLabel(v)}"`)
    .join(",");
  return `${name}{${parts}}`;
}

function addLabeled(target: Record<string, number>, name: string, value: number, labels?: Record<string, string>): void {
  target[labeledKey(name, labels)] = (target[labeledKey(name, labels)] || 0) + Math.max(0, Number(value) || 0);
}

export function renderMetrics(): string {
  const lines: string[] = [];

  // claude_proxy_requests_total
  lines.push("# HELP claude_proxy_requests_total Total chat-completion requests.");
  lines.push("# TYPE claude_proxy_requests_total counter");
  for (const [key, bucket] of requestRecords) {
    const [runtime, model, status] = key.split("|");
    lines.push(
      `claude_proxy_requests_total{runtime="${escapeLabel(runtime)}",model="${escapeLabel(model)}",status="${escapeLabel(status)}"} ${bucket.count}`,
    );
  }

  // claude_proxy_tokens_total + claude_proxy_estimated_cost_usd_total
  lines.push("# HELP claude_proxy_tokens_total Claude token usage observed in completed responses.");
  lines.push("# TYPE claude_proxy_tokens_total counter");
  for (const [key, val] of Object.entries(tokenCounters)) {
    lines.push(`${key} ${val}`);
  }

  lines.push("# HELP claude_proxy_estimated_cost_usd_total Estimated Claude API-equivalent cost in USD.");
  lines.push("# TYPE claude_proxy_estimated_cost_usd_total counter");
  for (const [key, val] of Object.entries(costCounters)) {
    lines.push(`${key} ${val.toFixed(6)}`);
  }

  // claude_proxy_request_duration_seconds (histogram)
  lines.push("# HELP claude_proxy_request_duration_seconds Request handler latency.");
  lines.push("# TYPE claude_proxy_request_duration_seconds histogram");
  for (const [key, bucket] of requestRecords) {
    const [runtime, model, status] = key.split("|");
    const labels = `runtime="${escapeLabel(runtime)}",model="${escapeLabel(model)}",status="${escapeLabel(status)}"`;
    for (const le of HIST_BUCKETS_MS) {
      lines.push(`claude_proxy_request_duration_seconds_bucket{${labels},le="${(le / 1000).toFixed(3)}"} ${bucket.buckets[le]}`);
    }
    lines.push(`claude_proxy_request_duration_seconds_bucket{${labels},le="+Inf"} ${bucket.count}`);
    lines.push(`claude_proxy_request_duration_seconds_sum{${labels}} ${(bucket.sumDurationMs / 1000).toFixed(6)}`);
    lines.push(`claude_proxy_request_duration_seconds_count{${labels}} ${bucket.count}`);
  }

  // claude_proxy_stream_fallback_total
  lines.push("# HELP claude_proxy_stream_fallback_total Stream-json → print fallbacks by reason.");
  lines.push("# TYPE claude_proxy_stream_fallback_total counter");
  if (Object.keys(fallbackCounters.byReason).length === 0) {
    lines.push(`claude_proxy_stream_fallback_total{reason="none"} 0`);
  } else {
    for (const [reason, n] of Object.entries(fallbackCounters.byReason)) {
      lines.push(`claude_proxy_stream_fallback_total{reason="${escapeLabel(reason)}"} ${n}`);
    }
  }

  // claude_proxy_pool_size
  lines.push("# HELP claude_proxy_pool_size Live workers in the session pool.");
  lines.push("# TYPE claude_proxy_pool_size gauge");
  const ps = poolStats();
  lines.push(`claude_proxy_pool_size{state="live"} ${ps.size}`);
  lines.push(`claude_proxy_pool_size{state="max"} ${ps.max}`);

  // claude_proxy_pool_ttl_evictions_total + lru_evictions_total
  lines.push("# HELP claude_proxy_pool_ttl_evictions_total Workers evicted for idle TTL.");
  lines.push("# TYPE claude_proxy_pool_ttl_evictions_total counter");
  lines.push(`claude_proxy_pool_ttl_evictions_total ${poolCounters.ttlEvictions}`);

  lines.push("# HELP claude_proxy_pool_lru_evictions_total Workers evicted to honor MAX_SESSIONS cap.");
  lines.push("# TYPE claude_proxy_pool_lru_evictions_total counter");
  lines.push(`claude_proxy_pool_lru_evictions_total ${poolCounters.lruEvictions}`);

  lines.push("# HELP claude_proxy_pool_fingerprint_mismatches_total Slots discarded for fingerprint drift.");
  lines.push("# TYPE claude_proxy_pool_fingerprint_mismatches_total counter");
  lines.push(`claude_proxy_pool_fingerprint_mismatches_total ${poolCounters.fingerprintMismatches}`);

  lines.push("# HELP claude_proxy_pool_warm_hits_total Conversations served from a warm pool slot.");
  lines.push("# TYPE claude_proxy_pool_warm_hits_total counter");
  lines.push(`claude_proxy_pool_warm_hits_total ${poolCounters.warmHits}`);

  lines.push("# HELP claude_proxy_pool_cold_spawns_total Conversations that took the cold path.");
  lines.push("# TYPE claude_proxy_pool_cold_spawns_total counter");
  lines.push(`claude_proxy_pool_cold_spawns_total ${poolCounters.coldSpawns}`);

  // claude_proxy_sticky_pool_* — explicit opt-in sticky session pool.
  const stickyStats = stickyPoolStats();
  lines.push("# HELP claude_proxy_sticky_pool_size Live workers in the opt-in sticky session pool.");
  lines.push("# TYPE claude_proxy_sticky_pool_size gauge");
  lines.push(`claude_proxy_sticky_pool_size{state="live"} ${stickyStats.size}`);
  lines.push(`claude_proxy_sticky_pool_size{state="max"} ${stickyStats.max}`);
  lines.push("# HELP claude_proxy_sticky_pool_enabled 1 when opt-in sticky sessions are enabled.");
  lines.push("# TYPE claude_proxy_sticky_pool_enabled gauge");
  lines.push(`claude_proxy_sticky_pool_enabled ${stickyStats.enabled ? 1 : 0}`);
  lines.push("# HELP claude_proxy_sticky_session_hits_total Sticky requests served from an existing live Claude CLI session.");
  lines.push("# TYPE claude_proxy_sticky_session_hits_total counter");
  lines.push(`claude_proxy_sticky_session_hits_total ${stickyPoolCounters.hits}`);
  lines.push("# HELP claude_proxy_sticky_session_cold_starts_total Sticky requests that created a new live Claude CLI session.");
  lines.push("# TYPE claude_proxy_sticky_session_cold_starts_total counter");
  lines.push(`claude_proxy_sticky_session_cold_starts_total ${stickyPoolCounters.coldStarts}`);
  lines.push("# HELP claude_proxy_sticky_session_evictions_total Sticky session evictions by bounded reason.");
  lines.push("# TYPE claude_proxy_sticky_session_evictions_total counter");
  lines.push(`claude_proxy_sticky_session_evictions_total{reason="idle_ttl"} ${stickyPoolCounters.ttlEvictions}`);
  lines.push(`claude_proxy_sticky_session_evictions_total{reason="absolute_ttl"} ${stickyPoolCounters.absoluteTtlEvictions}`);
  lines.push(`claude_proxy_sticky_session_evictions_total{reason="lru"} ${stickyPoolCounters.lruEvictions}`);
  lines.push(`claude_proxy_sticky_session_evictions_total{reason="unhealthy"} ${stickyPoolCounters.unhealthyEvictions}`);
  lines.push(`claude_proxy_sticky_session_evictions_total{reason="fingerprint_mismatch"} ${stickyPoolCounters.fingerprintMismatches}`);
  lines.push("# HELP claude_proxy_session_mode_total Requests accepted or rejected by explicit session mode.");
  lines.push("# TYPE claude_proxy_session_mode_total counter");
  for (const mode of ["pool", "sticky", "stateless"] as const) {
    lines.push(`claude_proxy_session_mode_total{mode="${mode}",status="accepted"} ${stickyPoolCounters.modeAccepted[mode]}`);
    lines.push(`claude_proxy_session_mode_total{mode="${mode}",status="rejected"} ${stickyPoolCounters.modeRejected[mode]}`);
  }

  // claude_proxy_subprocess_spawn_failures_total
  lines.push("# HELP claude_proxy_subprocess_spawn_failures_total Failed claude subprocess spawns.");
  lines.push("# TYPE claude_proxy_subprocess_spawn_failures_total counter");
  if (Object.keys(subprocessSpawnFailures).length === 0) {
    lines.push(`claude_proxy_subprocess_spawn_failures_total{runtime="none"} 0`);
  } else {
    for (const [runtime, n] of Object.entries(subprocessSpawnFailures)) {
      lines.push(`claude_proxy_subprocess_spawn_failures_total{runtime="${escapeLabel(runtime)}"} ${n}`);
    }
  }

  // claude_proxy_runtime_default — informational gauge for the resolved default runtime
  lines.push("# HELP claude_proxy_runtime_default 1 if the named runtime is the default.");
  lines.push("# TYPE claude_proxy_runtime_default gauge");
  lines.push(`claude_proxy_runtime_default{runtime="stream-json"} ${defaultRuntime() === "stream-json" ? 1 : 0}`);
  lines.push(`claude_proxy_runtime_default{runtime="print"} ${defaultRuntime() === "print" ? 1 : 0}`);

  // claude_proxy_error_class_total — bounded error classification counters
  lines.push("# HELP claude_proxy_error_class_total Errors by protocol error class.");
  lines.push("# TYPE claude_proxy_error_class_total counter");
  if (Object.keys(errorClassCounters).length === 0) {
    lines.push(`claude_proxy_error_class_total{class="none"} 0`);
  } else {
    for (const [cls, n] of Object.entries(errorClassCounters)) {
      lines.push(`claude_proxy_error_class_total{class="${escapeLabel(cls)}"} ${n}`);
    }
  }

  // claude_proxy_tool_call_parse — tool call parse outcome counters
  lines.push("# HELP claude_proxy_tool_call_parse_total Tool call parse outcomes for caller-dispatched tool bridge.");
  lines.push("# TYPE claude_proxy_tool_call_parse_total counter");
  lines.push(`claude_proxy_tool_call_parse_total{outcome="emitted"} ${toolCallParseCounters.emitted}`);
  lines.push(`claude_proxy_tool_call_parse_total{outcome="no_call"} ${toolCallParseCounters.no_call}`);
  lines.push(`claude_proxy_tool_call_parse_total{outcome="malformed"} ${toolCallParseCounters.malformed}`);
  lines.push(`claude_proxy_tool_call_parse_total{outcome="rejected"} ${toolCallParseCounters.rejected}`);
  lines.push(`claude_proxy_tool_call_parse_total{outcome="calls_emitted"} ${toolCallParseCounters.total_calls}`);

  // claude_proxy_trace_store — trace store gauge
  const ts = traceStore.stats();
  lines.push("# HELP claude_proxy_trace_store_size Current trace store occupancy.");
  lines.push("# TYPE claude_proxy_trace_store_size gauge");
  lines.push(`claude_proxy_trace_store_size{state="current"} ${ts.size}`);
  lines.push(`claude_proxy_trace_store_size{state="capacity"} ${ts.capacity}`);
  lines.push(`claude_proxy_trace_store_enabled ${ts.enabled ? 1 : 0}`);

  return lines.join("\n") + "\n";
}

export function resetMetrics(): void {
  requestRecords.clear();
  for (const key of Object.keys(subprocessSpawnFailures)) delete subprocessSpawnFailures[key];
  for (const key of Object.keys(tokenCounters)) delete tokenCounters[key];
  for (const key of Object.keys(costCounters)) delete costCounters[key];
  for (const key of Object.keys(errorClassCounters)) delete errorClassCounters[key];
  toolCallParseCounters.emitted = 0;
  toolCallParseCounters.no_call = 0;
  toolCallParseCounters.malformed = 0;
  toolCallParseCounters.rejected = 0;
  toolCallParseCounters.total_calls = 0;
  resetStickyPoolForTests();
}

export function handleMetrics(_req: Request, res: Response): void {
  res.setHeader("Content-Type", "text/plain; version=0.0.4");
  res.send(renderMetrics());
}
