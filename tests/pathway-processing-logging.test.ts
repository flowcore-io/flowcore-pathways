import { assertEquals, assertRejects } from "https://deno.land/std@0.224.0/assert/mod.ts"
import { z } from "zod"
import { type FlowcoreEvent, PathwaysBuilder } from "../src/mod.ts"
import type { Logger, LoggerMeta } from "../src/pathways/logger.ts"

Deno.test("pathway processing success is DEBUG while failures remain ERROR", async () => {
  const logs: { level: string; message: string; context?: LoggerMeta }[] = []
  const logger: Logger = {
    debug: (message, context) => logs.push({ level: "debug", message, context }),
    info: (message, context) => logs.push({ level: "info", message, context }),
    warn: (message, context) => logs.push({ level: "warn", message, context }),
    error: (message, _error, context) => logs.push({ level: "error", message: String(message), context }),
  }
  const processed = new Set<string>()
  const pathway = new PathwaysBuilder({
    baseUrl: "http://localhost:8022",
    tenant: "test-tenant",
    dataCore: "test-data-core",
    apiKey: "test-api-key",
    logger,
  }).withPathwayState({
    isProcessed: (id) => Promise.resolve(processed.has(id)),
    setProcessed: (id) => {
      processed.add(id)
      return Promise.resolve()
    },
  }).register({
    flowType: "test-flow",
    eventType: "test-event",
    schema: z.object({ fail: z.boolean() }),
    maxRetries: 0,
  })
  const path = "test-flow/test-event"
  let handled = 0
  pathway.handle(path, async (event) => {
    handled++
    if (event.payload.fail) throw new Error("handler failed")
  })
  const event: FlowcoreEvent = {
    eventId: "success-event",
    timeBucket: "20261008070000",
    tenant: "test-tenant",
    dataCoreId: "test-data-core",
    flowType: "test-flow",
    eventType: "test-event",
    metadata: {},
    payload: { fail: false },
    validTime: "2026-10-08T07:00:00.000Z",
  }

  await pathway.process(path, event)
  assertEquals(handled, 1)
  assertEquals(processed.has(event.eventId), true)
  assertEquals(logs.filter((log) => log.message === "Successfully processed pathway event"), [{
    level: "debug",
    message: "Successfully processed pathway event",
    context: { pathway: path, eventId: event.eventId },
  }])

  logs.length = 0
  const failedEvent = { ...event, eventId: "failed-event", payload: { fail: true } }
  await assertRejects(() => pathway.process(path, failedEvent), Error, "handler failed")
  assertEquals(processed.has(failedEvent.eventId), false)
  assertEquals(logs.filter((log) => log.message === "Successfully processed pathway event"), [])
  assertEquals(logs.filter((log) => log.message === "Error processing pathway event").map((log) => log.level), [
    "error",
  ])
})
