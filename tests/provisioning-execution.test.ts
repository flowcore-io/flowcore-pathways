import { assertEquals, assertRejects } from "https://deno.land/std@0.224.0/assert/mod.ts"
import { stub } from "https://deno.land/std@0.224.0/testing/mock.ts"
import { FlowcoreClient, TenantTranslateNameToIdCommand } from "@flowcore/sdk"
import {
  isRetryableProvisionError,
  mapWithConcurrency,
  resolveProvisionRetryConfig,
  retryProvisionOperation,
} from "../src/pathways/provisioning-execution.ts"

function httpError(status: number, retryAfter?: string): Error {
  const headers = new Headers()
  if (retryAfter !== undefined) headers.set("Retry-After", retryAfter)
  return Object.assign(new Error(`HTTP ${status}`), { response: { status, headers } })
}

Deno.test("provision retry retries transient HTTP failures and eventually succeeds", async () => {
  let attempts = 0
  const delays: number[] = []
  const result = await retryProvisionOperation(
    async () => {
      attempts++
      if (attempts < 3) throw httpError(500)
      return "ok"
    },
    resolveProvisionRetryConfig({ maxAttempts: 3, baseDelayMs: 10, maxDelayMs: 100, jitterRatio: 0 }),
    undefined,
    { sleep: (delay) => Promise.resolve(delays.push(delay)).then(() => undefined) },
  )

  assertEquals(result, "ok")
  assertEquals(attempts, 3)
  assertEquals(delays, [10, 20])
})

Deno.test("provision retry respects Retry-After", async () => {
  const delays: number[] = []
  let attempts = 0
  await retryProvisionOperation(
    async () => {
      attempts++
      if (attempts === 1) throw httpError(429, "2")
      return undefined
    },
    resolveProvisionRetryConfig({ maxAttempts: 2, baseDelayMs: 10, maxDelayMs: 5_000, jitterRatio: 0 }),
    undefined,
    { sleep: (delay) => Promise.resolve(delays.push(delay)).then(() => undefined) },
  )

  assertEquals(delays, [2_000])
})

Deno.test("provision retry does not shorten Retry-After to the backoff cap", async () => {
  const delays: number[] = []
  let attempts = 0
  await retryProvisionOperation(
    async () => {
      attempts++
      if (attempts === 1) throw httpError(429, "30")
      return undefined
    },
    resolveProvisionRetryConfig({ maxAttempts: 2, baseDelayMs: 10, maxDelayMs: 5_000, jitterRatio: 0 }),
    undefined,
    { sleep: (delay) => Promise.resolve(delays.push(delay)).then(() => undefined) },
  )

  assertEquals(delays, [30_000])
})

Deno.test("provision retry parses HTTP-date Retry-After", async () => {
  const now = Date.parse("2026-08-31T10:00:00.000Z")
  const delays: number[] = []
  let attempts = 0
  await retryProvisionOperation(
    async () => {
      attempts++
      if (attempts === 1) throw httpError(503, new Date(now + 10_000).toUTCString())
      return undefined
    },
    resolveProvisionRetryConfig({ maxAttempts: 2, baseDelayMs: 10, maxDelayMs: 5_000, jitterRatio: 0 }),
    undefined,
    { now: () => now, sleep: (delay) => Promise.resolve(delays.push(delay)).then(() => undefined) },
  )

  assertEquals(delays, [10_000])
})

Deno.test("provision retry handles transient network failures", async () => {
  let attempts = 0
  await retryProvisionOperation(
    async () => {
      attempts++
      if (attempts === 1) throw Object.assign(new Error("connection reset"), { code: "ECONNRESET" })
      return undefined
    },
    resolveProvisionRetryConfig({ maxAttempts: 2, baseDelayMs: 0, maxDelayMs: 0 }),
  )

  assertEquals(attempts, 2)
})

Deno.test("provision retry handles real SDK network wrappers", async () => {
  let fetchAttempts = 0
  const fetchStub = stub(globalThis, "fetch", () => {
    fetchAttempts++
    if (fetchAttempts === 1) return Promise.reject(new TypeError("error sending request: connection refused"))
    return Promise.resolve(
      new Response(JSON.stringify({ id: "tenant-id", name: "tenant" }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    )
  })

  try {
    const client = new FlowcoreClient({ apiKey: "fc_test_dummy", retry: null })
    const result = await retryProvisionOperation(
      () => client.execute(new TenantTranslateNameToIdCommand({ tenant: "tenant" })),
      resolveProvisionRetryConfig({ maxAttempts: 2, baseDelayMs: 0, maxDelayMs: 0 }),
    )

    assertEquals(result, { id: "tenant-id", name: "tenant" })
    assertEquals(fetchAttempts, 2)
  } finally {
    fetchStub.restore()
  }
})

Deno.test("provision retry does not retry ordinary client errors or not-found", async () => {
  for (const status of [400, 401, 403, 404]) {
    let attempts = 0
    await assertRejects(() =>
      retryProvisionOperation(
        async () => {
          attempts++
          throw httpError(status)
        },
        resolveProvisionRetryConfig({ maxAttempts: 3, baseDelayMs: 0, maxDelayMs: 0 }),
      )
    )
    assertEquals(attempts, 1)
  }
})

Deno.test("provision retry stops after maxAttempts", async () => {
  let attempts = 0
  await assertRejects(() =>
    retryProvisionOperation(
      async () => {
        attempts++
        throw httpError(503)
      },
      resolveProvisionRetryConfig({ maxAttempts: 3, baseDelayMs: 0, maxDelayMs: 0 }),
    )
  )
  assertEquals(attempts, 3)
})

Deno.test("retryable provisioning status classification", () => {
  for (const status of [408, 429, 500, 502, 503, 504]) {
    assertEquals(isRetryableProvisionError(httpError(status)), true)
  }
  for (const status of [400, 401, 403, 404, 409, 501]) {
    assertEquals(isRetryableProvisionError(httpError(status)), false)
  }
})

Deno.test("bounded map never exceeds its concurrency limit", async () => {
  let active = 0
  let maxActive = 0
  const values = await mapWithConcurrency([1, 2, 3, 4, 5, 6], 2, async (value) => {
    active++
    maxActive = Math.max(maxActive, active)
    await new Promise((resolve) => setTimeout(resolve, 5))
    active--
    return value * 2
  })

  assertEquals(maxActive, 2)
  assertEquals(values, [2, 4, 6, 8, 10, 12])
})

Deno.test("bounded map preserves the first concurrent failure", async () => {
  await assertRejects(
    () =>
      mapWithConcurrency(["first", "second"], 2, async (value) => {
        if (value === "second") await new Promise((resolve) => setTimeout(resolve, 5))
        throw new Error(value)
      }),
    Error,
    "first",
  )
})
