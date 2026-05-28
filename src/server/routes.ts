/**
 * API Route Handlers
 *
 * Implements OpenAI-compatible endpoints for Clawdbot integration
 */

import type { Request, Response } from "express";
import { v4 as uuidv4 } from "uuid";
import { ClaudeSubprocess } from "../subprocess/manager.js";
import { acquireSubprocess } from "../subprocess/pool.js";
import { acquireSession, returnSession, discardSession } from "../subprocess/session-pool.js";
import { acquirePreInit } from "../subprocess/init-pool.js";
import { StreamJsonSubprocess } from "../subprocess/stream-json-manager.js";
import {
  acquireStickySession,
  stickyPoolCounters,
  stickyPoolStats,
  type StickyAcquireResult,
  type StickyEvictionReason,
} from "../subprocess/sticky-session-pool.js";
import { resolveSessionOptions, isSessionOptionsError, type ResolvedSessionOptions, type SessionOptionsError } from "./sticky-options.js";
import { extractModel, messagesToPrompt, openaiToCli } from "../adapter/openai-to-cli.js";
import {
  cliResultToOpenai,
  createDoneChunk,
  createToolCallChunks,
  extractTextContent,
  resultUsageToOpenAI,
} from "../adapter/cli-to-openai.js";
import { parseToolCalls, shouldBridgeExternalTools, type ToolCallParseResult } from "../adapter/tools.js";
import type { OpenAIChatRequest, OpenAIChatChunk, ResponsesRequest } from "../types/openai.js";
import type { ClaudeCliAssistant, ClaudeCliResult, ClaudeCliStreamEvent } from "../types/claude-cli.js";
import { attachN8nDetector } from "../n8n/detector.js";
import { n8nProgressEnabled, getRunningExecution, formatProgress } from "../n8n/progress.js";
import { resolveRuntime, defaultRuntime } from "../subprocess/runtime.js";
import { poolStats } from "../subprocess/session-pool.js";
import { recordRequest, recordSpawnFailure, recordTokenUsage, recordToolCallParse, recordErrorClass } from "./metrics.js";
import { pricingSnapshot } from "./pricing.js";
import { annotateClaudeUsage, modelFromResult, usageFromClaudeResult } from "./usage.js";
import { UPSTREAM_SOFT_DEAD_MS, shouldTriggerSoftDead, buildSoftDeadDiagnostic, sampleDescendants } from "./watchdog.js";
import type { DescendantInfo } from "./watchdog.js";
import {
  responsesToChatRequest,
  chatResponseToResponses,
  chatUsageToResponsesUsage,
  buildResponsesStreamEvents,
  buildTextDeltaEvent,
  buildStreamDoneEvents,
} from "../adapter/responses.js";
import { classifyError, isStreamLayerFault, type ProtocolErrorClass } from "../errors.js";
import { createTraceBuilder, type TraceBuilder } from "../trace/builder.js";
import { traceStore } from "../trace/store.js";
import { detectOverlappingTools, isMcpInjectionEnabled, mcpGovernanceSummary } from "../mcp/governance.js";
import { getClaudeCliCapabilities } from "../subprocess/claude-flags.js";
import { attachPhaseTracker } from "./phase-tracker.js";
import { formatIntentionalWaitStatus, type IntentionalWaitState } from "../subprocess/intentional-wait.js";

const FALLBACK_ENABLED = process.env.CLAUDE_PROXY_FALLBACK_ON_STREAM_FAILURE === "1";

// Cached Claude CLI version — resolved once at first health call.
let cachedCliVersion: string | null = null;
async function getCliVersion(): Promise<string> {
  if (cachedCliVersion) return cachedCliVersion;
  try {
    const { verifyClaude } = await import("../subprocess/manager.js");
    const result = await verifyClaude();
    cachedCliVersion = result.version || "unknown";
  } catch {
    cachedCliVersion = "unknown";
  }
  return cachedCliVersion;
}

/**
 * Record error class on metrics whenever we classify an error.
 */
function classifyAndRecordError(err: unknown): ProtocolErrorClass {
  const cls = classifyError(err);
  recordErrorClass(cls);
  return cls;
}

/**
 * Record tool call parse outcome on metrics.
 */
function recordToolCallParseOutcome(parsed: Pick<ToolCallParseResult, "toolCalls" | "diagnostics">, bridgeTools: boolean): void {
  if (!bridgeTools) return;
  if (parsed.toolCalls.length > 0) {
    recordToolCallParse("emitted", parsed.toolCalls.length);
  } else if (parsed.diagnostics.malformedJsonObjects > 0) {
    recordToolCallParse("malformed", 0);
  } else if (parsed.diagnostics.rejectedToolCalls > 0 || parsed.diagnostics.attemptedToolCall) {
    recordToolCallParse("rejected", 0);
  } else {
    recordToolCallParse("no_call", 0);
  }
}

/**
 * Transport keepalives must not masquerade as assistant text. Generic idle
 * protection is sent as standards-compliant SSE comments, while genuinely
 * useful progress (for example n8n status) can still be emitted as visible
 * assistant content.
 */
export function createSseKeepaliveComment(requestId: string, count: number): string {
  return `:keepalive req_id=${requestId} count=${count}\n\n`;
}

export function createLivenessProgressText(): string {
  return "\nBubbling...\n🫧 Working maybe: thinking…\n";
}

export function livenessProgressEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return env.CLAUDE_PROXY_LIVENESS_PROGRESS === "1";
}

export function interimNarrationProgressEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return env.CLAUDE_PROXY_INTERIM_NARRATION_PROGRESS === "1";
}

function endsAtNaturalNarrationBoundary(text: string): boolean {
  return /(?:[.!?…][)"'\]]*|\n)\s*$/.test(text);
}

export function createInterimNarrationProgressText(text: string): string {
  if (!endsAtNaturalNarrationBoundary(text)) return "";
  const compact = text.replace(/\s+/g, " ").trim();
  if (!compact) return "";
  const shortened = compact.length > 240 ? `${compact.slice(0, 237)}…` : compact;
  return `\nBubbling...\n🧠 Thinking: ${shortened}\n`;
}

export const EMPTY_FINAL_RESPONSE_FALLBACK = "Claude finished without a final answer. I only received progress updates from the underlying Claude Code run.";

export function resolveStreamJsonFinalText(params: {
  resultText?: string | null;
  assistantMessageText?: string | null;
  contentDeltaText?: string | null;
  allowContentDeltaFallback?: boolean;
}): { text: string; source: "result_text" | "assistant_message" | "buffered_text" | "fallback"; usedFallback: boolean } {
  const resultText = params.resultText?.trim();
  if (resultText) return { text: resultText, source: "result_text", usedFallback: false };

  const assistantMessageText = params.assistantMessageText?.trim();
  if (assistantMessageText) return { text: assistantMessageText, source: "assistant_message", usedFallback: false };

  const contentDeltaText = params.contentDeltaText?.trim();
  if (params.allowContentDeltaFallback && contentDeltaText) {
    return { text: contentDeltaText, source: "buffered_text", usedFallback: false };
  }

  return { text: EMPTY_FINAL_RESPONSE_FALLBACK, source: "fallback", usedFallback: true };
}

export function shouldSuppressSoftDeadForIntentionalWait(state: IntentionalWaitState | null): state is IntentionalWaitState {
  return state?.detectedBy === "result_text";
}

export function hasRenderableAssistantContent(text: string): boolean {
  return text.replace(/[\u200B\u200C\u200D\uFEFF]/g, "").trim().length > 0;
}

export function createProgressChunk(
  requestId: string,
  model: string,
  includeRole: boolean = false,
  content: string,
): OpenAIChatChunk {
  if (!hasRenderableAssistantContent(content)) {
    throw new Error("progress chunk content must be renderable assistant text");
  }
  return {
    id: `chatcmpl-${requestId}`,
    object: "chat.completion.chunk",
    created: Math.floor(Date.now() / 1000),
    model,
    choices: [{
      index: 0,
      delta: {
        ...(includeRole ? { role: "assistant" as const } : {}),
        content,
      },
      finish_reason: null,
    }],
  };
}

export function createResponsesProgressFrame(responseId: string, model: string, content: string): string {
  if (!hasRenderableAssistantContent(content)) {
    throw new Error("responses progress frame content must be renderable progress text");
  }
  // Do not encode proxy progress as response.output_text.delta: strict Responses
  // clients expect concatenated text deltas to match response.output_text.done.
  // response.in_progress is a provider-parseable lifecycle event that keeps the
  // stream active without corrupting the assistant's final text.
  return `event: response.in_progress\ndata: ${JSON.stringify({
    type: "response.in_progress",
    response: {
      id: responseId,
      object: "response",
      created_at: Math.floor(Date.now() / 1000),
      model,
      output: [],
      status: "in_progress",
      metadata: { proxy_progress: content.trim() },
    },
  })}\n\n`;
}

/**
 * When OpenClaw provides tools, the proxy asks Claude to return external tool
 * calls as a JSON object. We must not stream that JSON into the user-visible
 * answer preview. But if the response clearly starts as normal prose, we can
 * safely flush the buffered text and then stream subsequent deltas live.
 */
export function shouldHoldBridgeToolStreamText(text: string): boolean {
  const trimmed = text.trimStart();
  if (!trimmed) return true;

  const lower = trimmed.toLowerCase();
  if ("```".startsWith(lower) || "```json".startsWith(lower)) return true;

  if (lower.startsWith("```")) {
    const fenceMatch = trimmed.match(/^```(?:json)?(?:\s|$)/i);
    if (!fenceMatch) return false;
    const afterFence = trimmed.slice(fenceMatch[0].length).trimStart();
    if (!afterFence) return true;
    return afterFence.startsWith("{");
  }

  return trimmed.startsWith("{");
}

// Counters for /metrics. Keep cardinality fixed.
export const fallbackCounters = {
  total: 0,
  byReason: {} as Record<string, number>,
};

/**
 * Reduce arbitrary client-provided model strings to one of a fixed set of
 * label values for /metrics. Bounded cardinality is critical — we never want
 * /metrics to grow unbounded labels from random model strings.
 */
const KNOWN_MODEL_LABELS = new Set([
  "claude-opus-4-8", "claude-opus-4-7", "claude-opus-4-6", "claude-opus-4",
  "claude-sonnet-4-6", "claude-sonnet-4",
  "claude-haiku-4-5-20251001", "claude-haiku-4-5", "claude-haiku-4",
]);
function canonicalizeModelLabel(model: string | undefined): string {
  if (!model) return "unknown";
  // Strip provider prefix (claude-proxy/ or claude-code-cli/).
  const stripped = model.replace(/^(claude-proxy|claude-code-cli)\//, "");
  return KNOWN_MODEL_LABELS.has(stripped) ? stripped : "other";
}

/**
 * Set the trace ID header on a response. Works for both streaming (before
 * flushHeaders) and non-streaming responses.
 */
function setTraceHeader(res: Response, traceId: string): void {
  if (!res.headersSent) {
    res.setHeader("X-Claude-Proxy-Trace-Id", traceId);
  }
}

function sendSessionOptionsError(res: Response, err: SessionOptionsError): void {
  if (!res.headersSent) {
    res.status(err.status).json({
      error: {
        message: err.message,
        type: "invalid_request_error",
        code: err.code,
      },
    });
  }
}

function recordSessionModeAccepted(mode: ResolvedSessionOptions["mode"]): void {
  stickyPoolCounters.modeAccepted[mode]++;
}

function recordSessionModeRejected(mode: ResolvedSessionOptions["mode"] | "sticky"): void {
  stickyPoolCounters.modeRejected[mode]++;
}

async function acquireStatelessStreamJson(model: string, disallowedTools: string[] = []): Promise<StreamJsonSubprocess> {
  if (disallowedTools.length === 0) return acquirePreInit(model);
  const subprocess = new StreamJsonSubprocess();
  await subprocess.start({ model, disallowedTools });
  return subprocess;
}

function isStickyBusyError(err: unknown): boolean {
  return err instanceof Error && (err.message === "sticky_session_busy" || err.message === "sticky_session_capacity_busy");
}

function sendStickyBusy(res: Response, message = "Sticky session is busy"): void {
  if (!res.headersSent) {
    res.status(429).json({
      error: {
        message,
        type: "rate_limit_error",
        code: "sticky_session_busy",
      },
    });
  }
}

/**
 * Handle POST /v1/chat/completions
 *
 * Main endpoint for chat requests, supports both streaming and non-streaming
 */
export async function handleChatCompletions(
  req: Request,
  res: Response
): Promise<void> {
  const requestId = uuidv4().replace(/-/g, "").slice(0, 24);
  const traceId = `trc_${requestId}`;
  const body = req.body as OpenAIChatRequest;
  const stream = body.stream === true;
  const reqStart = Date.now();
  let usedRuntime: "stream-json" | "print" = "stream-json";

  const tb = createTraceBuilder({
    traceId,
    requestId,
    model: extractModel(body.model),
    requestedModel: body.model || "unknown",
    stream,
    endpoint: "chat.completions",
  });
  tb.setMessageCount(body.messages?.length || 0);

  // Attempt to record metrics on response close, regardless of branch taken.
  res.on("close", () => {
    const status: "ok" | "error" = res.statusCode >= 400 ? "error" : "ok";
    const canonModel = canonicalizeModelLabel(body.model);
    recordRequest({ runtime: usedRuntime, model: canonModel, status, durationMs: Date.now() - reqStart });
  });

  try {
    // Validate request
    if (!body.messages || !Array.isArray(body.messages) || body.messages.length === 0) {
      tb.setError("invalid_request", "messages is required and must be a non-empty array");
      tb.commit();
      res.status(400).json({
        error: {
          message: "messages is required and must be a non-empty array",
          type: "invalid_request_error",
          code: "invalid_messages",
        },
      });
      return;
    }

    const sessionOptions = resolveSessionOptions(req);
    if (isSessionOptionsError(sessionOptions)) {
      tb.setError("invalid_request", sessionOptions.message);
      tb.commit();
      recordSessionModeRejected("sticky");
      sendSessionOptionsError(res, sessionOptions);
      return;
    }
    tb.setSessionMode(sessionOptions.mode);
    recordSessionModeAccepted(sessionOptions.mode);

    const runtime = resolveRuntime(req);
    usedRuntime = runtime;
    tb.setRuntime(runtime);
    if (sessionOptions.mode === "sticky" && runtime !== "stream-json") {
      tb.setError("invalid_request", "Sticky sessions require the stream-json runtime");
      tb.commit();
      recordSessionModeRejected("sticky");
      res.status(400).json({
        error: {
          message: "Sticky sessions require the stream-json runtime",
          type: "invalid_request_error",
          code: "sticky_requires_stream_json",
        },
      });
      return;
    }
    if (process.env.DEBUG) console.error(`[runtime] resolved=${runtime} req_id=${requestId}`);

    if (runtime === "stream-json") {
      const model = extractModel(body.model);
      try {
        await handleStreamJsonRequest(req, res, model, body, requestId, stream, tb, sessionOptions);
        return;
      } catch (err) {
        if (isStickyBusyError(err)) {
          tb.setError("invalid_request", (err as Error).message);
          tb.commit();
          sendStickyBusy(res, (err as Error).message === "sticky_session_capacity_busy" ? "Sticky session pool is at capacity and all sessions are busy" : "Sticky session is busy");
          return;
        }
        const errClass = classifyAndRecordError(err);
        // Auto-fallback: only fires when CLAUDE_PROXY_FALLBACK_ON_STREAM_FAILURE=1,
        // the failure is a recognized stream-layer fault, AND no SSE bytes
        // have been committed to the client yet.
        if (
          FALLBACK_ENABLED
          && !res.headersSent
          && !res.writableEnded
          && isStreamLayerFault(err)
        ) {
          fallbackCounters.byReason[errClass] =
            (fallbackCounters.byReason[errClass] || 0) + 1;
          fallbackCounters.total++;
          tb.setFallback(errClass);
          console.warn(
            `[stream_fallback] reason=${errClass} req_id=${requestId} err="${(err as Error).message}"`,
          );
          // fall through to --print path below
        } else {
          tb.setError(errClass, (err as Error).message);
          tb.commit();
          throw err;
        }
      }
    }

    // --print path (default fallback / runtime override / fallback retry)
    usedRuntime = "print";
    tb.setRuntime("print");
    setTraceHeader(res, traceId);
    const cliInput = openaiToCli(body);
    let subprocess: ClaudeSubprocess;
    try {
      subprocess = await acquireSubprocess(cliInput.model, cliInput.disallowedTools);
    } catch (err) {
      recordSpawnFailure("print");
      tb.setError(classifyError(err), (err as Error).message);
      tb.commit();
      throw err;
    }

    const bridgeTools = shouldBridgeExternalTools(body);
    tb.setBridgeTools(bridgeTools, body);

    if (stream) {
      await handleStreamingResponse(req, res, subprocess, cliInput, requestId, body.stream_options?.include_usage === true, body, tb);
    } else {
      await handleNonStreamingResponse(res, subprocess, cliInput, requestId, body, tb);
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";
    console.error("[handleChatCompletions] Error:", message);

    if (!res.headersSent) {
      res.status(500).json({
        error: {
          message,
          type: "server_error",
          code: null,
        },
      });
    }
  }
}

/**
 * Handle streaming response (SSE)
 *
 * IMPORTANT: The Express req.on("close") event fires when the request body
 * is fully received, NOT when the client disconnects. For SSE connections,
 * we use res.on("close") to detect actual client disconnection.
 */
async function handleStreamingResponse(
  req: Request,
  res: Response,
  subprocess: ClaudeSubprocess,
  cliInput: ReturnType<typeof openaiToCli>,
  requestId: string,
  includeUsage: boolean,
  body: OpenAIChatRequest,
  tb: TraceBuilder,
): Promise<void> {
  // Set SSE headers
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.setHeader("X-Request-Id", requestId);
  setTraceHeader(res, tb.traceId);

  // CRITICAL: Flush headers immediately to establish SSE connection
  // Without this, headers are buffered and client times out waiting
  res.flushHeaders();

  // Send initial comment to confirm connection is alive
  res.write(":ok\n\n");

  return new Promise<void>((resolve, reject) => {
    let isFirst = true;
    let lastModel = "claude-sonnet-4";
    let isComplete = false;
    let bufferedText = "";
    const bridgeTools = shouldBridgeExternalTools(body);

    // Handle actual client disconnect (response stream closed)
    res.on("close", () => {
      if (!isComplete) {
        // Client disconnected before response completed - kill subprocess
        subprocess.kill();
      }
      resolve();
    });

    // Handle streaming content deltas
    subprocess.on("content_delta", (event: ClaudeCliStreamEvent) => {
      const text = event.event.delta?.text || "";
      if (text && bridgeTools) {
        bufferedText += text;
        return;
      }
      if (text && !res.writableEnded) {
        const chunk = {
          id: `chatcmpl-${requestId}`,
          object: "chat.completion.chunk",
          created: Math.floor(Date.now() / 1000),
          model: lastModel,
          choices: [{
            index: 0,
            delta: {
              role: isFirst ? "assistant" : undefined,
              content: text,
            },
            finish_reason: null,
          }],
        };
        res.write(`data: ${JSON.stringify(chunk)}\n\n`);
        isFirst = false;
      }
    });

    // Handle final assistant message (for model name)
    subprocess.on("assistant", (message: ClaudeCliAssistant) => {
      lastModel = message.message.model;
    });

    subprocess.on("result", (result: ClaudeCliResult) => {
      isComplete = true;
      if (!res.writableEnded) {
        annotateAndRecordUsage(result, cliInput.model);
        const rawText = result.result || bufferedText;
        const parsed = parseToolCalls(rawText, body);
        recordToolCallParseOutcome(parsed, bridgeTools);
        const finishReason = parsed.toolCalls.length > 0 ? "tool_calls" as const : "stop" as const;
        tb.setFinishReason(finishReason);
        tb.setToolCallParseSource(result.result ? "result_text" : "buffered_text");
        for (const tc of parsed.toolCalls) tb.addToolCall(tc);
        recordUsageOnTrace(tb, result);
        tb.commit();

        if (parsed.toolCalls.length > 0) {
          for (const chunk of createToolCallChunks(requestId, lastModel, parsed.toolCalls)) {
            res.write(`data: ${JSON.stringify(chunk)}\n\n`);
          }
          res.write(`data: ${JSON.stringify(createDoneChunk(requestId, lastModel, includeUsage ? resultUsageToOpenAI(result) : undefined, "tool_calls"))}\n\n`);
        } else {
          if (bridgeTools && parsed.textContent && !res.writableEnded) {
            const contentChunk = {
              id: `chatcmpl-${requestId}`,
              object: "chat.completion.chunk" as const,
              created: Math.floor(Date.now() / 1000),
              model: lastModel,
              choices: [{ index: 0, delta: { role: isFirst ? "assistant" as const : undefined, content: parsed.textContent }, finish_reason: null }],
            };
            res.write(`data: ${JSON.stringify(contentChunk)}\n\n`);
            isFirst = false;
          }
          res.write(`data: ${JSON.stringify(createDoneChunk(requestId, lastModel, includeUsage ? resultUsageToOpenAI(result) : undefined))}\n\n`);
        }
        res.write("data: [DONE]\n\n");
        res.end();
      }
      resolve();
    });

    subprocess.on("error", (error: Error) => {
      console.error("[Streaming] Error:", error.message);
      tb.setError(classifyError(error), error.message);
      tb.commit();
      if (!res.writableEnded) {
        res.write(
          `data: ${JSON.stringify({
            error: { message: error.message, type: "server_error", code: null },
          })}\n\n`
        );
        res.end();
      }
      resolve();
    });

    subprocess.on("close", (code: number | null) => {
      // Subprocess exited - ensure response is closed
      if (!res.writableEnded) {
        if (code !== 0 && !isComplete) {
          // Abnormal exit without result - send error
          res.write(`data: ${JSON.stringify({
            error: { message: `Process exited with code ${code}`, type: "server_error", code: null },
          })}\n\n`);
        }
        res.write("data: [DONE]\n\n");
        res.end();
      }
      resolve();
    });

    // Subprocess is already prepared by the pool; just write the prompt.
    try {
      subprocess.submit(cliInput.prompt);
    } catch (err) {
      console.error("[Streaming] Submit error:", err);
      reject(err);
    }
  });
}

/**
 * Handle non-streaming response
 */
async function handleNonStreamingResponse(
  res: Response,
  subprocess: ClaudeSubprocess,
  cliInput: ReturnType<typeof openaiToCli>,
  requestId: string,
  body: OpenAIChatRequest,
  tb: TraceBuilder,
): Promise<void> {
  return new Promise((resolve) => {
    let finalResult: ClaudeCliResult | null = null;

    subprocess.on("result", (result: ClaudeCliResult) => {
      finalResult = result;
    });

    subprocess.on("error", (error: Error) => {
      console.error("[NonStreaming] Error:", error.message);
      tb.setError(classifyError(error), error.message);
      tb.commit();
      res.status(500).json({
        error: {
          message: error.message,
          type: "server_error",
          code: null,
        },
      });
      resolve();
    });

    subprocess.on("close", (code: number | null) => {
      if (finalResult) {
        annotateAndRecordUsage(finalResult, cliInput.model);
        setUsageHeaders(res, finalResult);
        const response = cliResultToOpenai(finalResult, requestId, body);
        const finishReason = response.choices[0]?.finish_reason || "stop";
        tb.setFinishReason(finishReason as "stop" | "tool_calls");
        if (response.choices[0]?.message.tool_calls) {
          for (const tc of response.choices[0].message.tool_calls) tb.addToolCall(tc);
        }
        recordUsageOnTrace(tb, finalResult);
        tb.commit();
        res.json(response);
      } else if (!res.headersSent) {
        tb.setError("worker_died", `Claude CLI exited with code ${code} without response`);
        tb.commit();
        res.status(500).json({
          error: {
            message: `Claude CLI exited with code ${code} without response`,
            type: "server_error",
            code: null,
          },
        });
      }
      resolve();
    });

    // Subprocess is already prepared by the pool; just write the prompt.
    try {
      subprocess.submit(cliInput.prompt);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown error";
      tb.setError(classifyError(error), message);
      tb.commit();
      res.status(500).json({
        error: { message, type: "server_error", code: null },
      });
      resolve();
    }
  });
}

/**
 * Handle a chat completion via stream-json transport with conversation pooling.
 * Either reuses a live subprocess (warm: cache hits the prior turns) or spawns
 * a new one (cold: sends the conversation as one flat user message).
 */
async function handleStreamJsonRequest(
  _req: Request,
  res: Response,
  model: string,
  body: OpenAIChatRequest,
  requestId: string,
  stream: boolean,
  tb: TraceBuilder,
  sessionOptions: ResolvedSessionOptions = { mode: "pool" },
): Promise<void> {
  const cliInput = openaiToCli(body);
  const bridgeTools = shouldBridgeExternalTools(body);

  let subprocess: Awaited<ReturnType<typeof acquirePreInit>>;
  let userText = cliInput.prompt;
  let subprocessReleased = false;
  let releaseSuccess: (assistantText: string) => void;
  let releaseDiscard: (reason: StickyEvictionReason) => void;

  if (sessionOptions.mode === "sticky" && sessionOptions.sticky) {
    const sticky: StickyAcquireResult = await acquireStickySession({
      sessionKeyHash: sessionOptions.sticky.keyHash,
      sessionKeyHashShort: sessionOptions.sticky.keyHashShort,
      ttlSeconds: sessionOptions.sticky.ttlSeconds,
      reset: sessionOptions.sticky.reset,
      model,
      messages: body.messages,
      bodyForPrompt: body,
      disallowedTools: cliInput.disallowedTools,
      sessionPolicy: sessionOptions.sticky.policy,
    });
    subprocess = sticky.subprocess;
    userText = sticky.userText;
    tb.setSessionWarmHit(sticky.isWarm);
    tb.setStickySession({
      hit: sticky.isStickyHit,
      keyHash: sticky.keyHashShort,
      ttlSeconds: sticky.ttlSeconds,
      turnCount: sticky.turnCount,
    });
    releaseSuccess = (text) => {
      if (subprocessReleased) return;
      subprocessReleased = true;
      sticky.release({ status: "success", assistantText: text });
    };
    releaseDiscard = (reason) => {
      if (subprocessReleased) return;
      subprocessReleased = true;
      tb.setStickyEviction(reason);
      sticky.release({ status: "discard", reason });
    };
  } else if (sessionOptions.mode === "stateless") {
    subprocess = await acquireStatelessStreamJson(model, cliInput.disallowedTools);
    tb.setSessionWarmHit(false);
    releaseSuccess = () => {
      if (subprocessReleased) return;
      subprocessReleased = true;
      subprocess.kill();
    };
    releaseDiscard = () => {
      if (subprocessReleased) return;
      subprocessReleased = true;
      subprocess.kill();
    };
  } else {
    const acquired = await acquireSession(model, body.messages, { disallowedTools: cliInput.disallowedTools });
    subprocess = acquired.subprocess;
    tb.setSessionWarmHit(acquired.isWarm);
    const lastMessage = body.messages[body.messages.length - 1];
    userText = acquired.isWarm
      ? (bridgeTools ? messagesToPrompt([lastMessage], body) : acquired.lastUserText)
      : cliInput.prompt;
    releaseSuccess = (assistantText) => {
      if (subprocessReleased) return;
      subprocessReleased = true;
      returnSession(subprocess, model, body.messages, assistantText, { disallowedTools: cliInput.disallowedTools });
    };
    releaseDiscard = () => {
      if (subprocessReleased) return;
      subprocessReleased = true;
      discardSession(subprocess);
    };
  }

  tb.setBridgeTools(bridgeTools, body);
  recordMcpGovernanceOnTrace(tb, subprocess, body);

  if (stream) {
    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache");
    res.setHeader("Connection", "keep-alive");
    res.setHeader("X-Request-Id", requestId);
    setTraceHeader(res, tb.traceId);
    res.flushHeaders();
    res.write(":ok\n\n");
  } else {
    setTraceHeader(res, tb.traceId);
  }

  let isFirst = true;
  let lastModel = "claude-sonnet-4";
  let assistantText = "";
  let assistantMessageText = "";
  let bridgeTextBuffer = "";
  let bridgeTextStreaming = false;
  let streamedAssistantText = false;
  const streamAssistantTextDeltas = process.env.CLAUDE_PROXY_STREAM_ASSISTANT_DELTAS === "1";
  const emitLivenessProgress = livenessProgressEnabled();
  const emitInterimNarrationProgress = interimNarrationProgressEnabled();
  let interimNarrationBuffer = "";
  let done = false;
  let keepaliveCount = 0;
  const requestStartAt = Date.now();
  let lastClaudeActivityAt = Date.now();
  let lastClientActivityAt = Date.now();
  console.error(`[StreamJson] request start req_id=${requestId} trace_id=${tb.traceId} model=${model} runtime=stream-json stream=${stream} bridgeTools=${bridgeTools}`);

  // -------------------- Keepalive ---------------------
  const KEEPALIVE_GAP_MS = 10_000;
  const KEEPALIVE_CHECK_MS = 5_000;

  const n8nDetector = attachN8nDetector(subprocess);
  let lastReportedExecution = "";

  const phaseTracker = attachPhaseTracker(subprocess);
  let intentionalWaitState: IntentionalWaitState | null = null;
  let lastIntentionalWaitProgressKey = "";
  const onIntentionalWait = (state: IntentionalWaitState) => {
    intentionalWaitState = state;
    console.error(`[StreamJson] intentional wait req_id=${requestId} kind=${state.kind} detectedBy=${state.detectedBy} tool=${state.toolName || ""}`);
  };
  subprocess.on("intentional_wait", onIntentionalWait);

  const writeKeepaliveChunk = async () => {
    if (res.writableEnded) return;
    keepaliveCount++;
    let content = "";
    let mode: "comment" | "progress" | "phase" = "comment";

    // Priority 1: n8n workflow progress (real external system status).
    if (n8nProgressEnabled() && n8nDetector.isInFlight()) {
      const snap = await getRunningExecution();
      if (snap) {
        const line = formatProgress(snap);
        if (snap.executionId !== lastReportedExecution) {
          content = "\n" + line + "\n";
          lastReportedExecution = snap.executionId;
          mode = "progress";
        }
      }
    }

    // Priority 2: Claude runtime phase (tool_use start, tool wait).
    if (!hasRenderableAssistantContent(content)) {
      const phase = phaseTracker.poll();
      if (phase) {
        content = "\n" + phase.text + "\n";
        mode = "phase";
      }
    }

    // Priority 3: Claude explicitly parked or is preparing to park the turn
    // waiting for wakeup/background completion. Emit at most once per 30s bucket.
    if (!hasRenderableAssistantContent(content) && intentionalWaitState) {
      const waitText = formatIntentionalWaitStatus(intentionalWaitState);
      const waitKey = `${intentionalWaitState.kind}:${intentionalWaitState.detectedBy}:${Math.floor((Date.now() - intentionalWaitState.startedAt) / 30_000)}`;
      if (waitKey !== lastIntentionalWaitProgressKey) {
        content = `\nBubbling...\n🫧 Working: ${waitText}\n`;
        lastIntentionalWaitProgressKey = waitKey;
        mode = "progress";
      }
    }

    // Priority 4: when final-only assistant text streaming is active, keep
    // OpenClaw's provider-event idle watchdog alive with a progress data chunk.
    // SSE comments keep the HTTP socket warm but OpenClaw strips comment-only
    // frames before provider parsing, so comments alone do not reset model idle.
    if (!hasRenderableAssistantContent(content) && emitLivenessProgress && !streamAssistantTextDeltas && keepaliveCount > 1) {
      content = createLivenessProgressText();
      mode = "progress";
    }

    if (hasRenderableAssistantContent(content)) {
      const chunk = createProgressChunk(requestId, lastModel, isFirst, content);
      res.write(`data: ${JSON.stringify(chunk)}\n\n`);
      isFirst = false;
    } else {
      res.write(createSseKeepaliveComment(requestId, keepaliveCount));
    }
    lastClientActivityAt = Date.now();
    console.error(`[StreamJson] keepalive req_id=${requestId} count=${keepaliveCount} mode=${mode} bridgeTools=${bridgeTools} contentBytes=${Buffer.byteLength(content, "utf8")}`);
  };

  // Layer 1: eager handshake, fires before claude starts.
  if (stream && !res.writableEnded) {
    const handshake = {
      id: `chatcmpl-${requestId}`,
      object: "chat.completion.chunk",
      created: Math.floor(Date.now() / 1000),
      model: lastModel,
      choices: [{ index: 0, delta: { role: "assistant" }, finish_reason: null }],
    };
    res.write(`data: ${JSON.stringify(handshake)}\n\n`);
    isFirst = false;
    void writeKeepaliveChunk();
  }

  // Layer 2: track ANY claude event for observability and clear parked-wait
  // watchdog suppression as soon as Claude resumes emitting messages.
  const onAnyClaudeEvent = () => {
    if (shouldSuppressSoftDeadForIntentionalWait(intentionalWaitState)) {
      console.error(`[StreamJson] intentional wait resumed req_id=${requestId} kind=${intentionalWaitState.kind} waitAgeMs=${Date.now() - intentionalWaitState.startedAt}`);
      intentionalWaitState = null;
    }
    lastClaudeActivityAt = Date.now();
  };
  subprocess.on("message", onAnyClaudeEvent);

  // Layer 3: periodic safety-net keepalive.
  const keepaliveTimer = stream
    ? setInterval(() => {
        if (done || res.writableEnded) return;
        if (Date.now() - lastClientActivityAt >= KEEPALIVE_GAP_MS) {
          void writeKeepaliveChunk();
        }
      }, KEEPALIVE_CHECK_MS)
    : null;
  const stopKeepalive = () => {
    if (keepaliveTimer) clearInterval(keepaliveTimer);
  };

  // -------------------- Upstream Soft-Dead Watchdog ---------------------
  const WATCHDOG_CHECK_MS = 30_000;
  let watchdogFired = false;
  const watchdogTimer = setInterval(() => {
    if (done || watchdogFired) return;
    const snap = subprocess.snapshot();

    let descendants: DescendantInfo | null = null;
    const now = Date.now();
    const silenceMs = now - lastClaudeActivityAt;
    if (silenceMs >= UPSTREAM_SOFT_DEAD_MS && snap.pid) {
      descendants = sampleDescendants(snap.pid);
    }

    const waitStateForWatchdog = intentionalWaitState;
    if (shouldSuppressSoftDeadForIntentionalWait(waitStateForWatchdog)) {
      console.error(`[StreamJson] watchdog suppressed during intentional wait req_id=${requestId} kind=${waitStateForWatchdog.kind} detectedBy=${waitStateForWatchdog.detectedBy} waitAgeMs=${now - waitStateForWatchdog.startedAt}`);
      return;
    }

    if (!shouldTriggerSoftDead(lastClaudeActivityAt, snap, now, descendants)) return;

    watchdogFired = true;
    done = true;
    const diag = buildSoftDeadDiagnostic(requestId, lastClaudeActivityAt, snap, now, {
      model,
      runtime: "stream-json",
      stream,
      bridgeTools,
      lastClientActivityAgeMs: now - lastClientActivityAt,
      lastClaudeActivityAgeMs: now - lastClaudeActivityAt,
      childPid: snap.pid,
      processActivityCount: snap.processActivityCount,
      watchdogAction: "kill",
      descendantCount: descendants?.count,
      descendantCpuPct: descendants?.totalCpuPct,
    });
    console.error(`[StreamJson] WATCHDOG ${diag.reason} req_id=${requestId} model=${model} stream=${stream} bridgeTools=${bridgeTools} silenceMs=${diag.silenceMs} lastClientAgeMs=${diag.context?.lastClientActivityAgeMs} lastClaudeAgeMs=${diag.context?.lastClaudeActivityAgeMs} pid=${snap.pid} processActivityCount=${snap.processActivityCount} descendants=${descendants ? `count=${descendants.count},running=${descendants.running},cpu=${descendants.totalCpuPct}%` : "none"} action=kill+discard`);

    tb.setError("upstream_soft_dead", `upstream ${diag.reason}: silent for ${Math.round(diag.silenceMs / 1000)}s`);
    tb.commit();

    releaseDiscard("watchdog");

    if (stream && !res.writableEnded) {
      const errMsg = `upstream ${diag.reason}: Claude CLI silent for ${Math.round(diag.silenceMs / 1000)}s`;
      res.write(`data: ${JSON.stringify({ error: { message: errMsg, type: "server_error", code: "upstream_dead" } })}\n\n`);
      res.write("data: [DONE]\n\n");
      res.end();
    } else if (!stream && !res.headersSent) {
      const errMsg = `upstream ${diag.reason}: Claude CLI silent for ${Math.round(diag.silenceMs / 1000)}s`;
      res.status(504).json({ error: { message: errMsg, type: "server_error", code: "upstream_dead" } });
    }
  }, WATCHDOG_CHECK_MS);
  const stopWatchdog = () => clearInterval(watchdogTimer);

  const writeContentChunk = (text: string, options: { assistantText?: boolean } = { assistantText: true }) => {
    if (!stream || res.writableEnded || !text) return;
    const chunk = {
      id: `chatcmpl-${requestId}`,
      object: "chat.completion.chunk",
      created: Math.floor(Date.now() / 1000),
      model: lastModel,
      choices: [{
        index: 0,
        delta: { role: isFirst ? "assistant" as const : undefined, content: text },
        finish_reason: null,
      }],
    };
    res.write(`data: ${JSON.stringify(chunk)}\n\n`);
    isFirst = false;
    if (options.assistantText !== false) streamedAssistantText = true;
    lastClientActivityAt = Date.now();
  };

  const onContentDelta = (event: ClaudeCliStreamEvent) => {
    const text = event.event.delta?.text || "";
    if (!text) return;
    assistantText += text;
    lastClaudeActivityAt = Date.now();

    // Claude Code stream-json emits intermediate assistant narration while tools are
    // running ("Now applying...", "Found root cause...", etc.). OpenAI chat
    // deltas have no phase metadata, so forwarding them live makes OpenClaw/Telegram
    // mix scratchpad/tool narration into the user-visible final answer. By default
    // keep final-only text. Operators with an OpenClaw progress bridge can opt in
    // to showing this narration as provider progress instead.
    if (!streamAssistantTextDeltas) {
      if (emitInterimNarrationProgress && stream && !res.writableEnded) {
        interimNarrationBuffer += text;
        const shouldFlush = endsAtNaturalNarrationBoundary(interimNarrationBuffer);
        if (shouldFlush) {
          const progress = createInterimNarrationProgressText(interimNarrationBuffer);
          interimNarrationBuffer = "";
          if (progress) writeContentChunk(progress, { assistantText: false });
        }
      }
      return;
    }

    if (bridgeTools) {
      if (!stream || res.writableEnded) return;
      if (bridgeTextStreaming) {
        writeContentChunk(text);
        return;
      }
      bridgeTextBuffer += text;
      if (!shouldHoldBridgeToolStreamText(bridgeTextBuffer)) {
        bridgeTextStreaming = true;
        writeContentChunk(bridgeTextBuffer);
        bridgeTextBuffer = "";
      }
      return;
    }
    writeContentChunk(text);
  };

  const onAssistant = (m: ClaudeCliAssistant) => {
    lastModel = m.message.model;
    const text = extractTextContent(m);
    if (text) assistantMessageText = text;
    // Capture full text in case streaming deltas were missed in legacy/live-delta mode.
    if (!assistantText) assistantText = text;
  };

  subprocess.on("content_delta", onContentDelta);
  subprocess.on("assistant", onAssistant);

  res.on("close", () => {
    if (!done) {
      done = true;
      console.error(`[StreamJson] client disconnected pre-completion req_id=${requestId} keepalives=${keepaliveCount} lastClientIdleMs=${Date.now() - lastClientActivityAt} lastClaudeIdleMs=${Date.now() - lastClaudeActivityAt}`);
      tb.setError("client_disconnect", "client disconnected before stream completion");
      tb.commit();
      releaseDiscard("client_disconnect");
    }
  });

  try {
    const result = await subprocess.submitTurn(userText);
    done = true;
    console.error(`[StreamJson] submit complete req_id=${requestId} keepalives=${keepaliveCount} durationMs=${Date.now() - requestStartAt}`);
    annotateAndRecordUsage(result, model);

    const finalText = resolveStreamJsonFinalText({
      resultText: result.result,
      assistantMessageText,
      contentDeltaText: assistantText,
      allowContentDeltaFallback: streamAssistantTextDeltas,
    });
    const rawText = finalText.text;
    const parsed = parseToolCalls(rawText, body);
    recordToolCallParseOutcome(parsed, bridgeTools);

    const finishReason = parsed.toolCalls.length > 0 ? "tool_calls" as const : "stop" as const;
    tb.setFinishReason(finishReason);
    tb.setToolCallParseSource(finalText.source === "result_text" ? "result_text" : "buffered_text");
    for (const tc of parsed.toolCalls) tb.addToolCall(tc);
    recordUsageOnTrace(tb, result);
    tb.commit();

    if (stream && !res.writableEnded) {
      if (!streamAssistantTextDeltas && emitInterimNarrationProgress && interimNarrationBuffer) {
        const progress = createInterimNarrationProgressText(interimNarrationBuffer);
        interimNarrationBuffer = "";
        if (progress) writeContentChunk(progress, { assistantText: false });
      }
      const usage = body.stream_options?.include_usage === true ? resultUsageToOpenAI(result) : undefined;
      if (parsed.toolCalls.length > 0) {
        for (const chunk of createToolCallChunks(requestId, lastModel, parsed.toolCalls)) {
          res.write(`data: ${JSON.stringify(chunk)}\n\n`);
        }
        res.write(`data: ${JSON.stringify(createDoneChunk(requestId, lastModel, usage, "tool_calls"))}\n\n`);
      } else {
        if (!streamedAssistantText && parsed.textContent) {
          writeContentChunk(parsed.textContent);
        }
        res.write(`data: ${JSON.stringify(createDoneChunk(requestId, lastModel, usage))}\n\n`);
      }
      res.write("data: [DONE]\n\n");
      res.end();
    } else if (!stream && !res.headersSent) {
      setUsageHeaders(res, result);
      res.json(cliResultToOpenai(result, requestId, body));
    }

    // Re-pool or retain the subprocess for the next turn according to session mode.
    releaseSuccess(parsed.toolCalls.length > 0 ? rawText : parsed.textContent);
  } catch (err) {
    // If another path already handled cleanup, skip duplicate release/error writes.
    if (watchdogFired || subprocessReleased) return;
    done = true;
    releaseDiscard("turn_error");
    const message = err instanceof Error ? err.message : "Unknown error";
    tb.setError(classifyError(err), message);
    tb.commit();
    console.error(`[StreamJson] turn error req_id=${requestId} keepalives=${keepaliveCount}:`, message);
    if (stream && !res.writableEnded) {
      res.write(`data: ${JSON.stringify({ error: { message, type: "server_error", code: null } })}\n\n`);
      res.end();
    } else if (!stream && !res.headersSent) {
      res.status(500).json({ error: { message, type: "server_error", code: null } });
    }
  } finally {
    stopKeepalive();
    stopWatchdog();
    n8nDetector.detach();
    phaseTracker.detach();
    subprocess.off("intentional_wait", onIntentionalWait);
    subprocess.off("content_delta", onContentDelta);
    subprocess.off("assistant", onAssistant);
    subprocess.off("message", onAnyClaudeEvent);
  }
}


function recordMcpGovernanceOnTrace(
  tb: TraceBuilder,
  subprocess: { getMcpDecisions?: () => import("../trace/types.js").TraceMcpDecision[] },
  body: Pick<OpenAIChatRequest, "tools" | "tool_choice">,
): void {
  const decisions = subprocess.getMcpDecisions?.() || [];
  for (const d of decisions) tb.addMcpDecision(d);

  if (!isMcpInjectionEnabled() || !shouldBridgeExternalTools(body)) return;
  const loadedServerNames = decisions.filter((d) => d.action === "loaded").map((d) => d.server);
  if (loadedServerNames.length === 0) return;
  const callerToolNames = (body.tools || [])
    .filter((tool) => tool.type === "function" && tool.function?.name)
    .map((tool) => tool.function.name);
  const pseudoServers = Object.fromEntries(loadedServerNames.map((name) => [name, { command: "", args: [], env: {} }]));
  for (const d of detectOverlappingTools(callerToolNames, pseudoServers)) tb.addMcpDecision(d);
}
function annotateAndRecordUsage(result: ClaudeCliResult, requestedModel: string): void {
  annotateClaudeUsage(result, requestedModel);
  recordTokenUsage(modelFromResult(result, requestedModel), usageFromClaudeResult(result), result.cost, Boolean(result.usageEstimated));
}

function recordUsageOnTrace(tb: TraceBuilder, result: ClaudeCliResult): void {
  const inputTokens = result.usage?.input_tokens || 0;
  const outputTokens = result.usage?.output_tokens || 0;
  const cacheRead = result.usage?.cache_read_input_tokens || 0;
  const cacheCreation = result.usage?.cache_creation_input_tokens || 0;
  tb.setUsage({
    promptTokens: inputTokens + cacheCreation + cacheRead,
    responseTokens: outputTokens,
    cacheReadTokens: cacheRead,
  });
}

function setUsageHeaders(res: Response, result: ClaudeCliResult): void {
  if (!result.usage || res.headersSent) return;
  const usage = usageFromClaudeResult(result);
  res.setHeader("X-Claude-Proxy-Prompt-Tokens", String(usage.inputTokens + usage.cacheCreationInputTokens + usage.cachedInputTokens));
  res.setHeader("X-Claude-Proxy-Completion-Tokens", String(usage.outputTokens));
  res.setHeader("X-Claude-Proxy-Total-Tokens", String(usage.totalTokens));
  res.setHeader("X-Claude-Proxy-Usage-Estimated", result.usageEstimated ? "true" : "false");
  if (result.cost) res.setHeader("X-Claude-Proxy-Estimated-Cost-Usd", result.cost.total_cost_usd.toFixed(6));
}

/**
 * Handle POST /v1/responses
 *
 * OpenAI Responses API compatibility. Translates to a Chat Completions request
 * internally, reusing the existing Claude CLI transport, then reshapes the
 * result into the Responses API envelope.
 */
export async function handleResponses(
  req: Request,
  res: Response,
): Promise<void> {
  const requestId = uuidv4().replace(/-/g, "").slice(0, 24);
  const traceId = `trc_${requestId}`;
  const body = req.body as ResponsesRequest;
  const stream = body.stream === true;
  const reqStart = Date.now();
  let usedRuntime: "stream-json" | "print" = "print";

  const tb = createTraceBuilder({
    traceId,
    requestId,
    model: extractModel(body.model),
    requestedModel: body.model || "unknown",
    stream,
    endpoint: "responses",
  });

  res.on("close", () => {
    const status: "ok" | "error" = res.statusCode >= 400 ? "error" : "ok";
    recordRequest({ runtime: usedRuntime, model: canonicalizeModelLabel(body.model), status, durationMs: Date.now() - reqStart });
  });

  try {
    // Validate: input is required
    if (body.input === undefined || body.input === null) {
      tb.setError("invalid_request", "input is required");
      tb.commit();
      res.status(400).json({
        error: {
          message: "input is required",
          type: "invalid_request_error",
          code: "invalid_input",
        },
      });
      return;
    }

    const sessionOptions = resolveSessionOptions(req);
    if (isSessionOptionsError(sessionOptions)) {
      tb.setError("invalid_request", sessionOptions.message);
      tb.commit();
      recordSessionModeRejected("sticky");
      sendSessionOptionsError(res, sessionOptions);
      return;
    }
    tb.setSessionMode(sessionOptions.mode);
    recordSessionModeAccepted(sessionOptions.mode);

    setTraceHeader(res, traceId);

    // Translate Responses request → Chat Completions request
    const chatReq = responsesToChatRequest(body);
    tb.setBridgeTools(shouldBridgeExternalTools(chatReq), chatReq);

    usedRuntime = resolveRuntime(req);
    tb.setRuntime(usedRuntime);
    if (sessionOptions.mode === "sticky" && usedRuntime !== "stream-json") {
      tb.setError("invalid_request", "Sticky sessions require the stream-json runtime");
      tb.commit();
      recordSessionModeRejected("sticky");
      res.status(400).json({
        error: {
          message: "Sticky sessions require the stream-json runtime",
          type: "invalid_request_error",
          code: "sticky_requires_stream_json",
        },
      });
      return;
    }
    if (usedRuntime === "stream-json") {
      try {
        await handleResponsesStreamJson(req, res, chatReq, requestId, body.model, stream, tb, sessionOptions);
      } catch (err) {
        if (isStickyBusyError(err)) {
          tb.setError("invalid_request", (err as Error).message);
          tb.commit();
          sendStickyBusy(res, (err as Error).message === "sticky_session_capacity_busy" ? "Sticky session pool is at capacity and all sessions are busy" : "Sticky session is busy");
          return;
        }
        throw err;
      }
    } else if (stream) {
      await handleResponsesStreaming(req, res, chatReq, requestId, body.model, tb);
    } else {
      await handleResponsesNonStreaming(res, chatReq, requestId, tb);
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";
    console.error("[handleResponses] Error:", message);
    tb.setError(classifyError(error), message);
    tb.commit();
    if (!res.headersSent) {
      res.status(500).json({
        error: { message, type: "server_error", code: null },
      });
    }
  }
}


/**
 * Responses API via the warm stream-json runtime. Mirrors the chat stream-json
 * transport but reshapes output into Responses envelopes/events.
 */
async function handleResponsesStreamJson(
  _req: Request,
  res: Response,
  chatReq: OpenAIChatRequest,
  requestId: string,
  requestModel: string,
  stream: boolean,
  tb: TraceBuilder,
  sessionOptions: ResolvedSessionOptions = { mode: "pool" },
): Promise<void> {
  const model = extractModel(chatReq.model);
  const cliInput = openaiToCli(chatReq);
  const bridgeTools = shouldBridgeExternalTools(chatReq);

  let subprocess: Awaited<ReturnType<typeof acquirePreInit>>;
  let userText = cliInput.prompt;
  let subprocessReleased = false;
  let releaseSuccess: (assistantText: string) => void;
  let releaseDiscard: (reason: StickyEvictionReason) => void;

  if (sessionOptions.mode === "sticky" && sessionOptions.sticky) {
    const sticky = await acquireStickySession({
      sessionKeyHash: sessionOptions.sticky.keyHash,
      sessionKeyHashShort: sessionOptions.sticky.keyHashShort,
      ttlSeconds: sessionOptions.sticky.ttlSeconds,
      reset: sessionOptions.sticky.reset,
      model,
      messages: chatReq.messages,
      bodyForPrompt: chatReq,
      disallowedTools: cliInput.disallowedTools,
      sessionPolicy: sessionOptions.sticky.policy,
    });
    subprocess = sticky.subprocess;
    userText = sticky.userText;
    tb.setSessionWarmHit(sticky.isWarm);
    tb.setStickySession({
      hit: sticky.isStickyHit,
      keyHash: sticky.keyHashShort,
      ttlSeconds: sticky.ttlSeconds,
      turnCount: sticky.turnCount,
    });
    releaseSuccess = (text) => {
      if (subprocessReleased) return;
      subprocessReleased = true;
      sticky.release({ status: "success", assistantText: text });
    };
    releaseDiscard = (reason) => {
      if (subprocessReleased) return;
      subprocessReleased = true;
      tb.setStickyEviction(reason);
      sticky.release({ status: "discard", reason });
    };
  } else if (sessionOptions.mode === "stateless") {
    subprocess = await acquireStatelessStreamJson(model, cliInput.disallowedTools);
    tb.setSessionWarmHit(false);
    releaseSuccess = () => {
      if (subprocessReleased) return;
      subprocessReleased = true;
      subprocess.kill();
    };
    releaseDiscard = () => {
      if (subprocessReleased) return;
      subprocessReleased = true;
      subprocess.kill();
    };
  } else {
    const acquired = await acquireSession(model, chatReq.messages, { disallowedTools: cliInput.disallowedTools });
    subprocess = acquired.subprocess;
    tb.setSessionWarmHit(acquired.isWarm);
    const lastMessage = chatReq.messages[chatReq.messages.length - 1];
    userText = acquired.isWarm
      ? (bridgeTools ? messagesToPrompt([lastMessage], chatReq) : acquired.lastUserText)
      : cliInput.prompt;
    releaseSuccess = (assistantText) => {
      if (subprocessReleased) return;
      subprocessReleased = true;
      returnSession(subprocess, model, chatReq.messages, assistantText, { disallowedTools: cliInput.disallowedTools });
    };
    releaseDiscard = () => {
      if (subprocessReleased) return;
      subprocessReleased = true;
      discardSession(subprocess);
    };
  }

  recordMcpGovernanceOnTrace(tb, subprocess, chatReq);

  const responseId = `resp_${requestId}`;
  const msgId = `msg_${uuidv4().replace(/-/g, "").slice(0, 12)}`;
  let assistantText = "";
  let assistantMessageText = "";
  let bridgeTextBuffer = "";
  let bridgeTextStreaming = false;
  let lastModel = requestModel;
  let done = false;
  let streamedAssistantText = false;
  const streamAssistantTextDeltas = process.env.CLAUDE_PROXY_STREAM_ASSISTANT_DELTAS === "1";
  const emitLivenessProgress = livenessProgressEnabled();
  const emitInterimNarrationProgress = interimNarrationProgressEnabled();
  let interimNarrationBuffer = "";
  let keepaliveCount = 0;
  const requestStartAt = Date.now();
  let lastClaudeActivityAt = Date.now();
  let lastClientActivityAt = Date.now();
  console.error(`[Responses StreamJson] request start req_id=${requestId} trace_id=${tb.traceId} model=${model} runtime=stream-json stream=${stream} bridgeTools=${bridgeTools}`);

  if (stream) {
    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache");
    res.setHeader("Connection", "keep-alive");
    res.setHeader("X-Request-Id", requestId);
    setTraceHeader(res, tb.traceId);
    res.flushHeaders();
    const envelope = buildResponsesStreamEvents(responseId, msgId, requestModel);
    res.write(`event: response.created\ndata: ${envelope.created}\n\n`);
    res.write(`event: response.output_item.added\ndata: ${envelope.itemAdded}\n\n`);
    res.write(`event: response.content_part.added\ndata: ${envelope.partAdded}\n\n`);
    res.write(createSseKeepaliveComment(requestId, 0));
  } else {
    setTraceHeader(res, tb.traceId);
  }

  // -------------------- Keepalive ---------------------
  const KEEPALIVE_GAP_MS = 10_000;
  const KEEPALIVE_CHECK_MS = 5_000;

  const n8nDetector = attachN8nDetector(subprocess);
  let lastReportedExecution = "";

  const phaseTracker = attachPhaseTracker(subprocess);
  let intentionalWaitState: IntentionalWaitState | null = null;
  let lastIntentionalWaitProgressKey = "";
  const onIntentionalWait = (state: IntentionalWaitState) => {
    intentionalWaitState = state;
    console.error(`[Responses StreamJson] intentional wait req_id=${requestId} kind=${state.kind} detectedBy=${state.detectedBy} tool=${state.toolName || ""}`);
  };
  subprocess.on("intentional_wait", onIntentionalWait);

  const writeKeepaliveChunk = async () => {
    if (res.writableEnded) return;
    keepaliveCount++;
    let content = "";
    let mode: "comment" | "progress" | "phase" = "comment";

    if (n8nProgressEnabled() && n8nDetector.isInFlight()) {
      const snap = await getRunningExecution();
      if (snap) {
        const line = formatProgress(snap);
        if (snap.executionId !== lastReportedExecution) {
          content = "\n" + line + "\n";
          lastReportedExecution = snap.executionId;
          mode = "progress";
        }
      }
    }

    if (!hasRenderableAssistantContent(content)) {
      const phase = phaseTracker.poll();
      if (phase) {
        content = "\n" + phase.text + "\n";
        mode = "phase";
      }
    }

    if (!hasRenderableAssistantContent(content) && intentionalWaitState) {
      const waitText = formatIntentionalWaitStatus(intentionalWaitState);
      const waitKey = `${intentionalWaitState.kind}:${intentionalWaitState.detectedBy}:${Math.floor((Date.now() - intentionalWaitState.startedAt) / 30_000)}`;
      if (waitKey !== lastIntentionalWaitProgressKey) {
        content = `\nBubbling...\n🫧 Working: ${waitText}\n`;
        lastIntentionalWaitProgressKey = waitKey;
        mode = "progress";
      }
    }

    if (!hasRenderableAssistantContent(content) && emitLivenessProgress && !streamAssistantTextDeltas && keepaliveCount > 1) {
      content = createLivenessProgressText();
      mode = "progress";
    }

    if (hasRenderableAssistantContent(content)) {
      res.write(createResponsesProgressFrame(responseId, lastModel, content));
    } else {
      res.write(createSseKeepaliveComment(requestId, keepaliveCount));
    }
    lastClientActivityAt = Date.now();
    console.error(`[Responses StreamJson] keepalive req_id=${requestId} count=${keepaliveCount} mode=${mode} bridgeTools=${bridgeTools} contentBytes=${Buffer.byteLength(content, "utf8")}`);
  };

  if (stream && !res.writableEnded) {
    void writeKeepaliveChunk();
  }

  const onAnyClaudeEvent = () => {
    if (shouldSuppressSoftDeadForIntentionalWait(intentionalWaitState)) {
      console.error(`[Responses StreamJson] intentional wait resumed req_id=${requestId} kind=${intentionalWaitState.kind} waitAgeMs=${Date.now() - intentionalWaitState.startedAt}`);
      intentionalWaitState = null;
    }
    lastClaudeActivityAt = Date.now();
  };
  subprocess.on("message", onAnyClaudeEvent);

  const keepaliveTimer = stream
    ? setInterval(() => {
        if (done || res.writableEnded) return;
        if (Date.now() - lastClientActivityAt >= KEEPALIVE_GAP_MS) {
          void writeKeepaliveChunk();
        }
      }, KEEPALIVE_CHECK_MS)
    : null;
  const stopKeepalive = () => {
    if (keepaliveTimer) clearInterval(keepaliveTimer);
  };

  // -------------------- Upstream Soft-Dead Watchdog ---------------------
  const WATCHDOG_CHECK_MS = 30_000;
  let watchdogFired = false;
  const watchdogTimer = setInterval(() => {
    if (done || watchdogFired) return;
    const snap = subprocess.snapshot();

    let descendants: DescendantInfo | null = null;
    const now = Date.now();
    const silenceMs = now - lastClaudeActivityAt;
    if (silenceMs >= UPSTREAM_SOFT_DEAD_MS && snap.pid) {
      descendants = sampleDescendants(snap.pid);
    }

    const waitStateForWatchdog = intentionalWaitState;
    if (shouldSuppressSoftDeadForIntentionalWait(waitStateForWatchdog)) {
      console.error(`[Responses StreamJson] watchdog suppressed during intentional wait req_id=${requestId} kind=${waitStateForWatchdog.kind} detectedBy=${waitStateForWatchdog.detectedBy} waitAgeMs=${now - waitStateForWatchdog.startedAt}`);
      return;
    }

    if (!shouldTriggerSoftDead(lastClaudeActivityAt, snap, now, descendants)) return;

    watchdogFired = true;
    done = true;
    const diag = buildSoftDeadDiagnostic(requestId, lastClaudeActivityAt, snap, now, {
      model,
      runtime: "stream-json",
      stream,
      bridgeTools,
      lastClientActivityAgeMs: now - lastClientActivityAt,
      lastClaudeActivityAgeMs: now - lastClaudeActivityAt,
      childPid: snap.pid,
      processActivityCount: snap.processActivityCount,
      watchdogAction: "kill",
      descendantCount: descendants?.count,
      descendantCpuPct: descendants?.totalCpuPct,
    });
    console.error(`[Responses StreamJson] WATCHDOG ${diag.reason} req_id=${requestId} model=${model} stream=${stream} bridgeTools=${bridgeTools} silenceMs=${diag.silenceMs} lastClientAgeMs=${diag.context?.lastClientActivityAgeMs} lastClaudeAgeMs=${diag.context?.lastClaudeActivityAgeMs} pid=${snap.pid} processActivityCount=${snap.processActivityCount} descendants=${descendants ? `count=${descendants.count},running=${descendants.running},cpu=${descendants.totalCpuPct}%` : "none"} action=kill+discard`);

    tb.setError("upstream_soft_dead", `upstream ${diag.reason}: silent for ${Math.round(diag.silenceMs / 1000)}s`);
    tb.commit();

    releaseDiscard("watchdog");

    if (stream && !res.writableEnded) {
      const errMsg = `upstream ${diag.reason}: Claude CLI silent for ${Math.round(diag.silenceMs / 1000)}s`;
      res.write(`event: error\ndata: ${JSON.stringify({ error: { message: errMsg, type: "server_error", code: "upstream_dead" } })}\n\n`);
      res.end();
    } else if (!stream && !res.headersSent) {
      const errMsg = `upstream ${diag.reason}: Claude CLI silent for ${Math.round(diag.silenceMs / 1000)}s`;
      res.status(504).json({ error: { message: errMsg, type: "server_error", code: "upstream_dead" } });
    }
  }, WATCHDOG_CHECK_MS);
  const stopWatchdog = () => clearInterval(watchdogTimer);

  const writeTextDelta = (text: string, options: { assistantText?: boolean } = { assistantText: true }) => {
    if (!stream || res.writableEnded || !text) return;
    res.write(`event: response.output_text.delta\ndata: ${buildTextDeltaEvent(text)}\n\n`);
    if (options.assistantText !== false) streamedAssistantText = true;
    lastClientActivityAt = Date.now();
  };

  const onContentDelta = (event: ClaudeCliStreamEvent) => {
    const text = event.event.delta?.text || "";
    if (!text) return;
    assistantText += text;
    lastClaudeActivityAt = Date.now();

    if (!streamAssistantTextDeltas) {
      if (emitInterimNarrationProgress && stream && !res.writableEnded) {
        interimNarrationBuffer += text;
        const shouldFlush = endsAtNaturalNarrationBoundary(interimNarrationBuffer);
        if (shouldFlush) {
          const progress = createInterimNarrationProgressText(interimNarrationBuffer);
          interimNarrationBuffer = "";
          if (progress) writeTextDelta(progress, { assistantText: false });
        }
      }
      return;
    }

    if (bridgeTools) {
      if (!stream || res.writableEnded) return;
      if (bridgeTextStreaming) {
        writeTextDelta(text);
        return;
      }
      bridgeTextBuffer += text;
      if (!shouldHoldBridgeToolStreamText(bridgeTextBuffer)) {
        bridgeTextStreaming = true;
        writeTextDelta(bridgeTextBuffer);
        bridgeTextBuffer = "";
      }
      return;
    }
    writeTextDelta(text);
  };
  const onAssistant = (message: ClaudeCliAssistant) => {
    lastModel = message.message.model;
    const text = extractTextContent(message);
    if (text) assistantMessageText = text;
    if (!assistantText) assistantText = text;
  };

  subprocess.on("content_delta", onContentDelta);
  subprocess.on("assistant", onAssistant);

  res.on("close", () => {
    if (!done) {
      done = true;
      console.error(`[Responses StreamJson] client disconnected pre-completion req_id=${requestId} keepalives=${keepaliveCount} lastClientIdleMs=${Date.now() - lastClientActivityAt} lastClaudeIdleMs=${Date.now() - lastClaudeActivityAt}`);
      tb.setError("client_disconnect", "client disconnected before stream completion");
      tb.commit();
      releaseDiscard("client_disconnect");
    }
  });

  try {
    const result = await subprocess.submitTurn(userText);
    done = true;
    console.error(`[Responses StreamJson] submit complete req_id=${requestId} keepalives=${keepaliveCount} durationMs=${Date.now() - requestStartAt}`);
    annotateAndRecordUsage(result, cliInput.model);
    const finalText = resolveStreamJsonFinalText({
      resultText: result.result,
      assistantMessageText,
      contentDeltaText: assistantText,
      allowContentDeltaFallback: streamAssistantTextDeltas,
    });
    const rawText = finalText.text;
    const resultForAdapters: ClaudeCliResult = { ...result, result: rawText };
    const parsed = parseToolCalls(rawText, chatReq);

    tb.setFinishReason(parsed.toolCalls.length > 0 ? "tool_calls" : "stop");
    tb.setToolCallParseSource(finalText.source === "result_text" ? "result_text" : "buffered_text");
    for (const tc of parsed.toolCalls) tb.addToolCall(tc);
    recordToolCallParseOutcome(parsed, bridgeTools);
    recordUsageOnTrace(tb, result);
    tb.commit();

    const assistantForPool = parsed.toolCalls.length > 0 ? rawText : parsed.textContent;
    releaseSuccess(assistantForPool);

    if (stream && !res.writableEnded) {
      if (!streamAssistantTextDeltas && emitInterimNarrationProgress && interimNarrationBuffer) {
        const progress = createInterimNarrationProgressText(interimNarrationBuffer);
        interimNarrationBuffer = "";
        if (progress) writeTextDelta(progress, { assistantText: false });
      }
      if (!streamedAssistantText && parsed.textContent) {
        writeTextDelta(parsed.textContent);
      }
      const usage = chatUsageToResponsesUsage(resultUsageToOpenAI(result));
      const doneEvents = buildStreamDoneEvents(responseId, msgId, lastModel, parsed.textContent, usage, parsed.toolCalls);
      for (const evt of doneEvents) {
        const parsedEvt = JSON.parse(evt);
        res.write(`event: ${parsedEvt.type}\ndata: ${evt}\n\n`);
      }
      res.write("data: [DONE]\n\n");
      res.end();
    } else if (!stream && !res.headersSent) {
      const chatResponse = cliResultToOpenai(resultForAdapters, requestId, chatReq);
      res.json(chatResponseToResponses(chatResponse, requestId));
    }
  } catch (error) {
    if (watchdogFired || subprocessReleased) return;
    done = true;
    releaseDiscard("turn_error");
    const message = error instanceof Error ? error.message : "Unknown error";
    tb.setError(classifyAndRecordError(error), message);
    tb.commit();
    if (stream && !res.writableEnded) {
      res.write(`event: error\ndata: ${JSON.stringify({ error: { message, type: "server_error" } })}\n\n`);
      res.end();
    } else if (!stream && !res.headersSent) {
      res.status(500).json({ error: { message, type: "server_error", code: null } });
    }
  } finally {
    stopKeepalive();
    stopWatchdog();
    n8nDetector.detach();
    phaseTracker.detach();
    subprocess.off("intentional_wait", onIntentionalWait);
    subprocess.off("content_delta", onContentDelta);
    subprocess.off("assistant", onAssistant);
    subprocess.off("message", onAnyClaudeEvent);
  }
}

/**
 * Non-streaming Responses API handler.
 * Internally delegates to the --print path and converts the result.
 */
async function handleResponsesNonStreaming(
  res: Response,
  chatReq: OpenAIChatRequest,
  requestId: string,
  tb: TraceBuilder,
): Promise<void> {
  const cliInput = openaiToCli(chatReq);
  const subprocess = await acquireSubprocess(cliInput.model, cliInput.disallowedTools);

  return new Promise((resolve) => {
    let finalResult: ClaudeCliResult | null = null;

    subprocess.on("result", (result: ClaudeCliResult) => {
      finalResult = result;
    });

    subprocess.on("error", (error: Error) => {
      console.error("[Responses NonStreaming] Error:", error.message);
      tb.setError(classifyError(error), error.message);
      tb.commit();
      res.status(500).json({
        error: { message: error.message, type: "server_error", code: null },
      });
      resolve();
    });

    subprocess.on("close", (code: number | null) => {
      if (finalResult) {
        annotateAndRecordUsage(finalResult, cliInput.model);
        const chatResponse = cliResultToOpenai(finalResult, requestId, chatReq);
        const responsesResponse = chatResponseToResponses(chatResponse, requestId);
        const hasToolCalls = chatResponse.choices[0]?.message?.tool_calls && chatResponse.choices[0].message.tool_calls.length > 0;
        const finishReason = hasToolCalls ? "tool_calls" as const : (chatResponse.choices[0]?.finish_reason as "stop" | "tool_calls" || "stop");
        tb.setFinishReason(finishReason);
        if (chatResponse.choices[0]?.message?.tool_calls) {
          for (const tc of chatResponse.choices[0].message.tool_calls) tb.addToolCall(tc);
          recordToolCallParse("emitted", chatResponse.choices[0].message.tool_calls.length);
        }
        recordUsageOnTrace(tb, finalResult);
        tb.commit();
        res.json(responsesResponse);
      } else if (!res.headersSent) {
        tb.setError("worker_died", `Claude CLI exited with code ${code} without response`);
        tb.commit();
        res.status(500).json({
          error: {
            message: `Claude CLI exited with code ${code} without response`,
            type: "server_error",
            code: null,
          },
        });
      }
      resolve();
    });

    try {
      subprocess.submit(cliInput.prompt);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown error";
      tb.setError(classifyAndRecordError(error), message);
      tb.commit();
      res.status(500).json({
        error: { message, type: "server_error", code: null },
      });
      resolve();
    }
  });
}

/**
 * Streaming Responses API handler.
 * Emits Responses API SSE events (response.created, response.output_text.delta, etc.)
 */
async function handleResponsesStreaming(
  req: Request,
  res: Response,
  chatReq: OpenAIChatRequest,
  requestId: string,
  requestModel: string,
  tb: TraceBuilder,
): Promise<void> {
  const cliInput = openaiToCli(chatReq);
  const subprocess = await acquireSubprocess(cliInput.model, cliInput.disallowedTools);
  const responseId = `resp_${requestId}`;
  const msgId = `msg_${uuidv4().replace(/-/g, "").slice(0, 12)}`;

  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.setHeader("X-Request-Id", requestId);
  setTraceHeader(res, tb.traceId);
  res.flushHeaders();

  // Emit opening envelope events
  const envelope = buildResponsesStreamEvents(responseId, msgId, requestModel);
  res.write(`event: response.created\ndata: ${envelope.created}\n\n`);
  res.write(`event: response.output_item.added\ndata: ${envelope.itemAdded}\n\n`);
  res.write(`event: response.content_part.added\ndata: ${envelope.partAdded}\n\n`);

  return new Promise<void>((resolve) => {
    let isComplete = false;
    let fullText = "";
    let lastModel = requestModel;
    const bridgeTools = shouldBridgeExternalTools(chatReq);

    res.on("close", () => {
      if (!isComplete) subprocess.kill();
      resolve();
    });

    subprocess.on("content_delta", (event: ClaudeCliStreamEvent) => {
      const text = event.event.delta?.text || "";
      if (text && !res.writableEnded) {
        fullText += text;
        if (!bridgeTools) {
          res.write(`event: response.output_text.delta\ndata: ${buildTextDeltaEvent(text)}\n\n`);
        }
      }
    });

    subprocess.on("assistant", (message: ClaudeCliAssistant) => {
      lastModel = message.message.model;
    });

    subprocess.on("result", (result: ClaudeCliResult) => {
      isComplete = true;
      if (!res.writableEnded) {
        annotateAndRecordUsage(result, cliInput.model);
        const rawText = result.result || fullText;
        const parsed = parseToolCalls(rawText, chatReq);
        fullText = parsed.textContent || "";
        const usage = chatUsageToResponsesUsage(resultUsageToOpenAI(result));
        const doneEvents = buildStreamDoneEvents(responseId, msgId, lastModel, fullText, usage, parsed.toolCalls);

        tb.setFinishReason(parsed.toolCalls.length > 0 ? "tool_calls" : "stop");
        for (const tc of parsed.toolCalls) tb.addToolCall(tc);
        recordToolCallParseOutcome(parsed, bridgeTools);
        recordUsageOnTrace(tb, result);
        tb.commit();

        for (const evt of doneEvents) {
          const parsed = JSON.parse(evt);
          res.write(`event: ${parsed.type}\ndata: ${evt}\n\n`);
        }
        res.write("data: [DONE]\n\n");
        res.end();
      }
      resolve();
    });

    subprocess.on("error", (error: Error) => {
      console.error("[Responses Streaming] Error:", error.message);
      tb.setError(classifyError(error), error.message);
      tb.commit();
      if (!res.writableEnded) {
        res.write(`event: error\ndata: ${JSON.stringify({ error: { message: error.message, type: "server_error" } })}\n\n`);
        res.end();
      }
      resolve();
    });

    subprocess.on("close", (code: number | null) => {
      if (!res.writableEnded) {
        if (code !== 0 && !isComplete) {
          res.write(`event: error\ndata: ${JSON.stringify({ error: { message: `Process exited with code ${code}`, type: "server_error" } })}\n\n`);
        }
        res.end();
      }
      resolve();
    });

    try {
      subprocess.submit(cliInput.prompt);
    } catch (err) {
      console.error("[Responses Streaming] Submit error:", err);
      tb.setError(classifyError(err), (err as Error).message);
      tb.commit();
      if (!res.writableEnded) {
        res.write(`event: error\ndata: ${JSON.stringify({ error: { message: (err as Error).message, type: "server_error" } })}\n\n`);
        res.end();
      }
      resolve();
    }
  });
}

/**
 * Handle GET /v1/models
 *
 * Returns available models
 */
export function handleModels(_req: Request, res: Response): void {
  const created = Math.floor(Date.now() / 1000);
  const ids = [
    "claude-opus-4-8",
    "claude-opus-4-7",
    "claude-opus-4-6",
    "claude-sonnet-4-6",
    "claude-haiku-4-5-20251001",
    "claude-opus-4",
    "claude-sonnet-4",
    "claude-haiku-4",
  ];
  res.json({
    object: "list",
    data: ids.map((id) => ({
      id,
      object: "model",
      owned_by: "anthropic",
      created,
    })),
  });
}

/**
 * Handle GET /pricing and /v1/pricing
 *
 * Exposes the local public pricing book used for API-equivalent cost estimates.
 */
export function handlePricing(_req: Request, res: Response): void {
  res.json({ object: "pricing_book", ...pricingSnapshot() });
}

/**
 * Handle GET /health
 *
 * Cheap liveness probe — returns immediately. Confirms the process is up
 * and the HTTP listener is bound. No subprocess work. Use this for
 * load-balancer-style health checks.
 */
export async function handleHealth(_req: Request, res: Response): Promise<void> {
  const cliVersion = await getCliVersion();
  const capabilities = await getClaudeCliCapabilities();
  res.json({
    status: "ok",
    provider: "claude-code-cli",
    timestamp: new Date().toISOString(),
    runtime: defaultRuntime(),
    claude_cli_version: cliVersion,
    claude_cli_capabilities: {
      source: capabilities.source,
      checkedAt: capabilities.checkedAt,
      flags: capabilities.flags,
      error: capabilities.error,
    },
    pool: poolStats(),
    sticky_pool: stickyPoolStats(),
    trace: traceStore.stats(),
    mcp: mcpGovernanceSummary(),
  });
}

// Module-level state for /healthz/deep — remembers the last successful
// deep-probe time so a failed probe can report when things last worked.
let lastDeepProbeSuccessAt: number = 0;

/**
 * Handle GET /healthz/deep
 *
 * Real probe — spawns a `claude --print` with a trivial prompt and a 5s
 * budget. Returns 200 with latency + pool stats on success, 503 with the
 * error and the last-success timestamp on failure.
 */
export async function handleHealthDeep(_req: Request, res: Response): Promise<void> {
  const start = Date.now();
  try {
    const sub = new ClaudeSubprocess();
    const ok = await new Promise<boolean>((resolve, reject) => {
      const PROBE_TIMEOUT_MS = 15000;
      const timer = setTimeout(() => reject(new Error(`deep probe timed out (${PROBE_TIMEOUT_MS / 1000}s)`)), PROBE_TIMEOUT_MS);
      let gotResult = false;
      sub.on("result", () => { gotResult = true; });
      sub.on("close", () => {
        clearTimeout(timer);
        gotResult ? resolve(true) : reject(new Error("subprocess closed without result"));
      });
      sub.on("error", (err) => {
        clearTimeout(timer);
        reject(err);
      });
      sub.start("Reply with: ok", { model: "haiku", timeout: PROBE_TIMEOUT_MS }).catch(reject);
    });

    const latencyMs = Date.now() - start;
    if (ok) lastDeepProbeSuccessAt = Date.now();

    res.json({
      ok: true,
      latency_ms: latencyMs,
      runtime: defaultRuntime(),
      pool: poolStats(),
      trace: traceStore.stats(),
      last_success_ts: lastDeepProbeSuccessAt,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    res.status(503).json({
      ok: false,
      error: message,
      latency_ms: Date.now() - start,
      runtime: defaultRuntime(),
      pool: poolStats(),
      trace: traceStore.stats(),
      last_success_ts: lastDeepProbeSuccessAt || null,
    });
  }
}

// ── Trace endpoints ─────────────────────────────────────────────────

/**
 * Localhost gate — rejects requests from non-loopback addresses.
 */
function isLocalhost(req: Request): boolean {
  const ip = req.ip || req.socket?.remoteAddress || "";
  return ip === "127.0.0.1" || ip === "::1" || ip === "::ffff:127.0.0.1";
}

/**
 * Handle GET /traces/:id
 *
 * Returns a single trace record. Localhost-gated.
 */
export function handleTraceGet(req: Request, res: Response): void {
  if (!isLocalhost(req)) {
    res.status(403).json({ error: { message: "trace endpoints are localhost-only", type: "forbidden" } });
    return;
  }
  if (!traceStore.enabled) {
    res.status(404).json({ error: { message: "tracing is disabled (set CLAUDE_PROXY_TRACE_ENABLED=1)", type: "not_found" } });
    return;
  }
  const traceId = Array.isArray(req.params.id) ? req.params.id[0] : req.params.id;
  const trace = traceStore.get(traceId || "");
  if (!trace) {
    res.status(404).json({ error: { message: "trace not found", type: "not_found" } });
    return;
  }
  res.json(trace);
}

/**
 * Handle GET /traces
 *
 * Returns a list of recent traces (summary view). Localhost-gated.
 * Query params: ?limit=50&offset=0
 */
export function handleTraceList(req: Request, res: Response): void {
  if (!isLocalhost(req)) {
    res.status(403).json({ error: { message: "trace endpoints are localhost-only", type: "forbidden" } });
    return;
  }
  if (!traceStore.enabled) {
    res.status(200).json({ object: "list", data: [], enabled: false, message: "tracing is disabled (set CLAUDE_PROXY_TRACE_ENABLED=1)" });
    return;
  }
  const limit = Math.min(Math.max(parseInt(req.query.limit as string, 10) || 50, 1), 200);
  const offset = Math.max(parseInt(req.query.offset as string, 10) || 0, 0);
  const data = traceStore.list(limit, offset);
  res.json({ object: "list", data, total: traceStore.size(), ...traceStore.stats() });
}
