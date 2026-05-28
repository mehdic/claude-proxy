import test from "node:test";
import assert from "node:assert/strict";
import { extractModel, messagesToPrompt, openaiToCli } from "../adapter/openai-to-cli.js";
import type { OpenAIChatRequest } from "../types/openai.js";

test("extractModel defaults unknown model ids to opus", () => {
  assert.equal(extractModel("not-a-real-model"), "opus");
});

test("extractModel strips claude-code-cli provider prefix", () => {
  assert.equal(extractModel("claude-code-cli/claude-sonnet-4"), "sonnet");
});

test("extractModel accepts claude-proxy provider prefix", () => {
  assert.equal(extractModel("claude-proxy/claude-opus-4-8"), "claude-opus-4-8");
});

test("messagesToPrompt joins text content parts with newlines", () => {
  const prompt = messagesToPrompt([
    { role: "user", content: [{ type: "text", text: "first" }, { type: "text", text: "second" }] },
  ]);
  assert.equal(prompt, "first\nsecond");
});

test("messagesToPrompt ignores non-text content parts", () => {
  const prompt = messagesToPrompt([
    { role: "user", content: [{ type: "image_url", image_url: { url: "data:" } }, { type: "text", text: "visible" }] },
  ]);
  assert.equal(prompt, "visible");
});

test("messagesToPrompt skips empty system messages", () => {
  const prompt = messagesToPrompt([
    { role: "system", content: null },
    { role: "user", content: "hello" },
  ]);
  assert.equal(prompt, "hello");
});

test("messagesToPrompt wraps developer messages as system context", () => {
  const prompt = messagesToPrompt([
    { role: "developer", content: "Follow policy" },
    { role: "user", content: "hello" },
  ]);
  assert.match(prompt, /<system>\nFollow policy\n<\/system>/);
});

test("messagesToPrompt wraps assistant text as previous response", () => {
  const prompt = messagesToPrompt([
    { role: "assistant", content: "Earlier answer" },
    { role: "user", content: "continue" },
  ]);
  assert.match(prompt, /<previous_response>\nEarlier answer\n<\/previous_response>/);
});

test("openaiToCli maps OpenAI user field to session id", () => {
  const req: OpenAIChatRequest = { model: "claude-sonnet-4", user: "session-1", messages: [{ role: "user", content: "hi" }] };
  assert.equal(openaiToCli(req).sessionId, "session-1");
});

test("openaiToCli omits disallowed tools when bridge is inactive", () => {
  const req: OpenAIChatRequest = { model: "claude-sonnet-4", messages: [{ role: "user", content: "hi" }] };
  assert.equal(openaiToCli(req).disallowedTools, undefined);
});

test("openaiToCli maps request model through extractModel", () => {
  const req: OpenAIChatRequest = { model: "claude-haiku-4", messages: [{ role: "user", content: "hi" }] };
  assert.equal(openaiToCli(req).model, "haiku");
});

test("messagesToPrompt skips empty assistant messages", () => {
  const prompt = messagesToPrompt([
    { role: "assistant", content: null },
    { role: "user", content: "next" },
  ]);
  assert.equal(prompt, "next");
});

test("messagesToPrompt skips empty user messages", () => {
  const prompt = messagesToPrompt([
    { role: "user", content: "" },
    { role: "user", content: "next" },
  ]);
  assert.equal(prompt, "next");
});
