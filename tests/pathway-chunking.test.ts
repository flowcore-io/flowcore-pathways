// @ts-nocheck
import {
  assert,
  assertEquals,
  assertExists,
  assertRejects,
  assertThrows,
} from "https://deno.land/std@0.224.0/assert/mod.ts"
import { Buffer } from "node:buffer"
import { z } from "zod"
import {
  aesGcmDecrypt,
  aesGcmEncrypt,
  buildChunkParts,
  CHUNK_DATA_FIELD,
  CHUNK_ENVELOPE_FIELD,
  DEFAULT_MAX_EVENT_BYTES,
  deriveEncryptionKey,
  ENCRYPTED_PAYLOAD_FIELD,
  FlowcoreEvent,
  InternalPathwayChunkStore,
  joinChunkParts,
  parseChunkEnvelope,
  PATHWAY_CHUNK_SCHEME,
  PATHWAY_CHUNKED_METADATA_KEY,
  PATHWAY_ENCRYPTED_METADATA_KEY,
  PATHWAY_ENCRYPTION_SCHEME,
  PATHWAY_ENCRYPTION_SCHEME_METADATA_KEY,
  PathwaysBuilder,
  serializedByteLength,
  sha256Hex,
  splitUtf8,
} from "../src/mod.ts"

const ENCRYPTION_KEY = "pathway-chunking-test-key-32-chars-ok!"

const eventSchema = z.object({
  id: z.string(),
  title: z.string(),
  content: z.string(),
  workspaceId: z.string(),
})

function largeContent(bytes: number, seed = "x"): string {
  // Mix multi-byte characters in so UTF-8 boundaries are exercised.
  const unit = `${seed}æøå€😀`
  let out = ""
  while (Buffer.byteLength(out, "utf8") < bytes) {
    out += unit
  }
  return out
}

function createBuilder(options: Record<string, unknown> = {}, withStore = true) {
  const builder = new PathwaysBuilder({
    baseUrl: "http://localhost:8099",
    tenant: "test-tenant",
    dataCore: "test-data-core",
    apiKey: "test-api-key",
    ...options,
  })
  if (withStore) {
    builder.withPathwayChunkStore(new InternalPathwayChunkStore())
  }
  return builder.register({
    flowType: "big-flow",
    eventType: "created",
    schema: eventSchema,
    encrypted: options.encrypted === true,
  })
}

function createEvent(payload: unknown, metadata: Record<string, unknown> = {}, eventId = crypto.randomUUID()) {
  return {
    eventId,
    timeBucket: "202609140000",
    tenant: "test-tenant",
    dataCoreId: "test-data-core",
    flowType: "big-flow",
    eventType: "created",
    metadata,
    validTime: new Date().toISOString(),
    payload,
  } as FlowcoreEvent
}

Deno.test("splitUtf8 never splits a multi-byte character and joins back losslessly", () => {
  const text = largeContent(1_000)
  for (const budget of [4, 5, 7, 13, 64, 999]) {
    const slices = splitUtf8(text, budget)
    assertEquals(slices.join(""), text)
    for (const slice of slices) {
      assert(Buffer.byteLength(slice, "utf8") <= budget, `slice exceeds ${budget}`)
      // A broken surrogate pair or multi-byte char would not survive a UTF-8 roundtrip.
      assertEquals(Buffer.from(slice, "utf8").toString("utf8"), slice)
    }
  }
  assertEquals(splitUtf8("", 10), [""])
  assertThrows(() => splitUtf8("abc", 3), Error, "at least 4 bytes")
})

Deno.test("buildChunkParts keeps every part under the budget and joinChunkParts restores the payload", () => {
  const payload = { id: "1", content: largeContent(150_000) }
  const parts = buildChunkParts(payload, 45_000)
  assert(parts.length > 1)
  const header = parts[0][CHUNK_ENVELOPE_FIELD]
  assertEquals(header.scheme, PATHWAY_CHUNK_SCHEME)
  assertEquals(header.totalParts, parts.length)
  assertEquals(header.digest, sha256Hex(JSON.stringify(payload)))
  for (const [index, part] of parts.entries()) {
    assert(serializedByteLength(part) <= 45_000, `part ${index + 1} too large`)
    assertEquals(part[CHUNK_ENVELOPE_FIELD].part, index + 1)
    assertEquals(part[CHUNK_ENVELOPE_FIELD].id, header.id)
  }
  const restored = joinChunkParts(parts.map((part) => part[CHUNK_DATA_FIELD]), header.digest)
  assertEquals(restored, payload)
})

Deno.test("buildChunkParts accounts for encryption expansion", () => {
  const key = deriveEncryptionKey(ENCRYPTION_KEY)
  const payload = { id: "1", content: largeContent(120_000) }
  const parts = buildChunkParts(payload, 45_000, (slice) => aesGcmEncrypt(slice, key))
  for (const part of parts) {
    assert(serializedByteLength(part) <= 45_000)
  }
  const slices = parts.map((part) => aesGcmDecrypt(part[CHUNK_DATA_FIELD], key))
  assertEquals(joinChunkParts(slices, parts[0][CHUNK_ENVELOPE_FIELD].digest), payload)
})

Deno.test("joinChunkParts rejects a digest mismatch and invalid JSON", () => {
  assertThrows(() => joinChunkParts(['{"a":1', "}"], "0".repeat(64)), Error, "digest mismatch")
  const text = "not json"
  assertThrows(() => joinChunkParts([text], sha256Hex(text)), Error, "invalid JSON")
})

Deno.test("parseChunkEnvelope detects envelopes and rejects bad headers", () => {
  assertEquals(parseChunkEnvelope({ id: "plain" }), null)
  assertEquals(parseChunkEnvelope("string"), null)
  assertEquals(parseChunkEnvelope(null), null)
  // The envelope field is present but the shape is wrong: fail closed, never treat as plain.
  assertThrows(() => parseChunkEnvelope({ [CHUNK_ENVELOPE_FIELD]: { id: "x" } }), Error, "Malformed")
  assertThrows(
    () => parseChunkEnvelope({ [CHUNK_ENVELOPE_FIELD]: "nope", [CHUNK_DATA_FIELD]: "d" }),
    Error,
    "Malformed",
  )
  const parts = buildChunkParts({ content: largeContent(50_000) }, 20_000)
  const parsed = parseChunkEnvelope(parts[1])
  assertExists(parsed)
  assertEquals(parsed.header.part, 2)
  assertThrows(
    () =>
      parseChunkEnvelope({
        [CHUNK_ENVELOPE_FIELD]: { ...parts[0][CHUNK_ENVELOPE_FIELD], scheme: "other" },
        [CHUNK_DATA_FIELD]: "x",
      }),
    Error,
    "Unsupported pathway chunk scheme",
  )
  assertThrows(
    () =>
      parseChunkEnvelope({
        [CHUNK_ENVELOPE_FIELD]: { ...parts[0][CHUNK_ENVELOPE_FIELD], part: 9 },
        [CHUNK_DATA_FIELD]: "x",
      }),
    Error,
    "Invalid pathway chunk header",
  )
})

Deno.test("write splits an oversized event into parts and returns the part 1 id", async () => {
  const pathway = createBuilder()
  let captured: Record<string, unknown>[] | undefined
  let capturedMetadata: Record<string, unknown> | undefined
  pathway["batchWriters"]["big-flow/created"] = async (payload, metadata) => {
    captured = payload
    capturedMetadata = metadata
    return payload.map((_, index) => `event-${index + 1}`)
  }
  let singleCalled = false
  pathway["writers"]["big-flow/created"] = async () => {
    singleCalled = true
    return "single"
  }

  const data = { id: "1", title: "big", content: largeContent(100_000), workspaceId: "w" }
  const eventId = await pathway.write("big-flow/created", { data, options: { fireAndForget: true } })

  assertEquals(singleCalled, false)
  assertEquals(eventId, "event-1")
  assertExists(captured)
  assert(captured.length >= 3)
  for (const part of captured) {
    assert(serializedByteLength(part) <= DEFAULT_MAX_EVENT_BYTES)
    assertExists(part[CHUNK_ENVELOPE_FIELD])
  }
  assertEquals(capturedMetadata?.[PATHWAY_CHUNKED_METADATA_KEY], "true")
})

Deno.test("write sends a small event unchanged", async () => {
  const pathway = createBuilder()
  let batchCalled = false
  pathway["batchWriters"]["big-flow/created"] = async () => {
    batchCalled = true
    return []
  }
  let capturedMetadata: Record<string, unknown> | undefined
  pathway["writers"]["big-flow/created"] = async (_payload, metadata) => {
    capturedMetadata = metadata
    return "single"
  }
  const eventId = await pathway.write("big-flow/created", {
    data: { id: "1", title: "small", content: "tiny", workspaceId: "w" },
    options: { fireAndForget: true },
  })
  assertEquals(eventId, "single")
  assertEquals(batchCalled, false)
  assertEquals(capturedMetadata?.[PATHWAY_CHUNKED_METADATA_KEY], undefined)
})

Deno.test("write without a chunk store sends the oversized event as-is", async () => {
  const pathway = createBuilder({}, false)
  let captured: unknown
  pathway["writers"]["big-flow/created"] = async (payload) => {
    captured = payload
    return "single"
  }
  const data = { id: "1", title: "big", content: largeContent(100_000), workspaceId: "w" }
  await pathway.write("big-flow/created", { data, options: { fireAndForget: true } })
  assertEquals(captured, data)
})

Deno.test({
  name: "process without a chunk store rejects a part event loudly",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: async () => {
    const pathway = createBuilder({}, false)
    pathway.handle("big-flow/created", async () => {})
    const parts = buildChunkParts({ id: "1", title: "t", content: largeContent(100_000), workspaceId: "w" }, 45_000)
    await assertRejects(
      () => pathway.process("big-flow/created", createEvent(parts[0], { [PATHWAY_CHUNKED_METADATA_KEY]: "true" })),
      Error,
      "no chunk store is configured",
    )
  },
})

Deno.test("write with chunking disabled sends the oversized event as-is even with a store", async () => {
  const pathway = createBuilder({ chunking: { enabled: false } })
  let captured: unknown
  pathway["writers"]["big-flow/created"] = async (payload) => {
    captured = payload
    return "single"
  }
  const data = { id: "1", title: "big", content: largeContent(100_000), workspaceId: "w" }
  await pathway.write("big-flow/created", { data, options: { fireAndForget: true } })
  assertEquals(captured, data)
})

Deno.test("batch write expands only oversized items and returns one logical id per item", async () => {
  const pathway = createBuilder()
  let captured: Record<string, unknown>[] | undefined
  pathway["batchWriters"]["big-flow/created"] = async (payload) => {
    captured = payload
    return payload.map((_, index) => `event-${index + 1}`)
  }
  const small = { id: "s", title: "small", content: "tiny", workspaceId: "w" }
  const big = { id: "b", title: "big", content: largeContent(100_000), workspaceId: "w" }
  const ids = await pathway.write("big-flow/created", {
    batch: true,
    data: [small, big, small],
    options: { fireAndForget: true },
  })

  assertExists(captured)
  assert(captured.length > 3)
  assertEquals(captured[0], small)
  assertEquals(captured[captured.length - 1], small)
  assertEquals(ids.length, 3)
  assertEquals(ids[0], "event-1")
  assertEquals(ids[1], "event-2")
  assertEquals(ids[2], `event-${captured.length}`)
})

Deno.test("encrypted oversized write keeps every part under the cap and stays decryptable", async () => {
  const pathway = createBuilder({ encryption: { mode: "symmetric", key: ENCRYPTION_KEY }, encrypted: true })
  let captured: Record<string, unknown>[] | undefined
  let capturedMetadata: Record<string, unknown> | undefined
  pathway["batchWriters"]["big-flow/created"] = async (payload, metadata) => {
    captured = payload
    capturedMetadata = metadata
    return payload.map((_, index) => `event-${index + 1}`)
  }
  const data = { id: "1", title: "big", content: largeContent(100_000), workspaceId: "w" }
  await pathway.write("big-flow/created", { data, options: { fireAndForget: true } })

  assertExists(captured)
  const key = deriveEncryptionKey(ENCRYPTION_KEY)
  const slices: string[] = []
  for (const part of captured) {
    assert(serializedByteLength(part) <= DEFAULT_MAX_EVENT_BYTES)
    assertEquals(part[ENCRYPTED_PAYLOAD_FIELD], undefined)
    // Ciphertext, not plaintext, is on the wire.
    assert(!part[CHUNK_DATA_FIELD].includes("æøå"))
    slices.push(aesGcmDecrypt(part[CHUNK_DATA_FIELD], key))
  }
  assertEquals(joinChunkParts(slices, captured[0][CHUNK_ENVELOPE_FIELD].digest), data)
  assertEquals(capturedMetadata?.[PATHWAY_CHUNKED_METADATA_KEY], "true")
  assertEquals(capturedMetadata?.[PATHWAY_ENCRYPTED_METADATA_KEY], "true")
})

Deno.test({
  name: "process reassembles out-of-order parts, hands the handler the full event and strips markers",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: async () => {
    const pathway = createBuilder()
    const data = { id: "1", title: "big", content: largeContent(100_000), workspaceId: "w" }
    const parts = buildChunkParts(data, 45_000)
    const eventIds = parts.map((_, index) => `part-${index + 1}`)

    const handled: FlowcoreEvent[] = []
    pathway.handle("big-flow/created", async (event) => {
      handled.push(event)
    })

    const metadata = { [PATHWAY_CHUNKED_METADATA_KEY]: "true", custom: "kept" }
    const order = [...parts.keys()].reverse()
    for (const index of order) {
      await pathway.process("big-flow/created", createEvent(parts[index], { ...metadata }, eventIds[index]))
      const isLast = index === order[order.length - 1]
      if (!isLast) {
        assertEquals(handled.length, 0)
        // Non-first parts are marked processed right away; part 1 waits for assembly.
        if (index !== 0) {
          assertEquals(await pathway["pathwayState"].isProcessed(eventIds[index]), true)
        }
      }
    }

    assertEquals(handled.length, 1)
    assertEquals(handled[0].payload, data)
    assertEquals(handled[0].eventId, "part-1")
    assertEquals(handled[0].metadata, { custom: "kept" })
    for (const id of eventIds) {
      assertEquals(await pathway["pathwayState"].isProcessed(id), true)
    }
  },
})

Deno.test({
  name: "process keeps part 1 unprocessed until the chunk is complete",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: async () => {
    const pathway = createBuilder()
    const parts = buildChunkParts({ id: "1", title: "t", content: largeContent(100_000), workspaceId: "w" }, 45_000)
    pathway.handle("big-flow/created", async () => {})
    await pathway.process("big-flow/created", createEvent(parts[0], { [PATHWAY_CHUNKED_METADATA_KEY]: "true" }, "p1"))
    assertEquals(await pathway["pathwayState"].isProcessed("p1"), false)
    for (let index = 1; index < parts.length; index++) {
      await pathway.process(
        "big-flow/created",
        createEvent(parts[index], { [PATHWAY_CHUNKED_METADATA_KEY]: "true" }, `p${index + 1}`),
      )
    }
    assertEquals(await pathway["pathwayState"].isProcessed("p1"), true)
  },
})

Deno.test({
  name: "process accepts exact replays and rejects a conflicting part",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: async () => {
    const pathway = createBuilder()
    const parts = buildChunkParts({ id: "1", title: "t", content: largeContent(100_000), workspaceId: "w" }, 45_000)
    let handledCount = 0
    pathway.handle("big-flow/created", async () => {
      handledCount++
    })
    const meta = () => ({ [PATHWAY_CHUNKED_METADATA_KEY]: "true" })

    await pathway.process("big-flow/created", createEvent(parts[0], meta(), "p1"))
    await pathway.process("big-flow/created", createEvent(parts[0], meta(), "p1"))
    await pathway.process("big-flow/created", createEvent(parts[1], meta(), "p2"))
    assertEquals(handledCount, 0)

    // Same part number, different bytes: a conflict, not a replay.
    const tampered = { ...parts[1], [CHUNK_DATA_FIELD]: parts[1][CHUNK_DATA_FIELD] + "!" }
    await assertRejects(
      () => pathway.process("big-flow/created", createEvent(tampered, meta(), "p2-bad")),
      Error,
      "Conflicting pathway chunk part",
    )
    // The conflicting part was rejected, so the chunk still completes with the genuine parts.
    for (let index = 2; index < parts.length; index++) {
      await pathway.process("big-flow/created", createEvent(parts[index], meta(), `p${index + 1}`))
    }
    assertEquals(handledCount, 1)
  },
})

Deno.test({
  name: "process rejects a reassembled payload whose digest does not match",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: async () => {
    const pathway = createBuilder()
    const parts = buildChunkParts({ id: "1", title: "t", content: largeContent(100_000), workspaceId: "w" }, 45_000)
    pathway.handle("big-flow/created", async () => {})
    const meta = () => ({ [PATHWAY_CHUNKED_METADATA_KEY]: "true" })
    const wrongDigest = parts.map((part) => ({
      ...part,
      [CHUNK_ENVELOPE_FIELD]: { ...part[CHUNK_ENVELOPE_FIELD], digest: "0".repeat(64) },
    }))
    for (let index = 0; index < wrongDigest.length - 1; index++) {
      await pathway.process("big-flow/created", createEvent(wrongDigest[index], meta(), `p${index + 1}`))
    }
    await assertRejects(
      () =>
        pathway.process(
          "big-flow/created",
          createEvent(wrongDigest[wrongDigest.length - 1], meta(), `p${wrongDigest.length}`),
        ),
      Error,
      "digest mismatch",
    )
  },
})

Deno.test({
  name: "process decrypts encrypted parts before collecting them",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: async () => {
    const pathway = createBuilder({ encryption: { mode: "symmetric", key: ENCRYPTION_KEY }, encrypted: true })
    const key = deriveEncryptionKey(ENCRYPTION_KEY)
    const data = { id: "1", title: "big", content: largeContent(100_000), workspaceId: "w" }
    const parts = buildChunkParts(data, 45_000, (slice) => aesGcmEncrypt(slice, key))
    const handled: FlowcoreEvent[] = []
    pathway.handle("big-flow/created", async (event) => {
      handled.push(event)
    })
    const meta = () => ({
      [PATHWAY_CHUNKED_METADATA_KEY]: "true",
      [PATHWAY_ENCRYPTED_METADATA_KEY]: "true",
      [PATHWAY_ENCRYPTION_SCHEME_METADATA_KEY]: PATHWAY_ENCRYPTION_SCHEME,
    })
    for (const [index, part] of parts.entries()) {
      await pathway.process("big-flow/created", createEvent(part, meta(), `p${index + 1}`))
    }
    assertEquals(handled.length, 1)
    assertEquals(handled[0].payload, data)
    assertEquals(handled[0].metadata, {})
  },
})

Deno.test({
  name: "process leaves a plain event untouched even when the chunk marker is present",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: async () => {
    const pathway = createBuilder()
    const handled: FlowcoreEvent[] = []
    pathway.handle("big-flow/created", async (event) => {
      handled.push(event)
    })
    const data = { id: "1", title: "t", content: "plain", workspaceId: "w" }
    await pathway.process("big-flow/created", createEvent(data, { [PATHWAY_CHUNKED_METADATA_KEY]: "true" }))
    assertEquals(handled.length, 1)
    assertEquals(handled[0].payload, data)
  },
})

Deno.test("InternalPathwayChunkStore reports exactly one completion and expires stale chunks", async () => {
  const store = new InternalPathwayChunkStore({ ttlMs: 20 })
  const base = { chunkId: crypto.randomUUID(), totalParts: 2, digest: "d" }
  assertEquals((await store.storePart({ ...base, part: 2, data: "b", eventId: "e2" })).status, "stored")
  assertEquals((await store.storePart({ ...base, part: 2, data: "b", eventId: "e2" })).status, "duplicate")
  await assertRejects(() => store.storePart({ ...base, part: 2, data: "x", eventId: "e2" }), Error, "Conflicting")
  const complete = await store.storePart({ ...base, part: 1, data: "a", eventId: "e1" })
  assertEquals(complete.status, "complete")
  assertEquals(complete.parts, ["a", "b"])
  assertEquals(complete.partEventIds, ["e1", "e2"])
  assertEquals((await store.storePart({ ...base, part: 1, data: "a", eventId: "e1" })).status, "duplicate")
  await store.deleteChunk(base.chunkId)

  const mixed = { chunkId: crypto.randomUUID(), totalParts: 2, digest: "d" }
  await store.storePart({ ...mixed, part: 1, data: "a", eventId: "e1" })
  await assertRejects(
    () => store.storePart({ ...mixed, part: 2, data: "b", eventId: "e2", totalParts: 3 }),
    Error,
    "Inconsistent",
  )
  await assertRejects(
    () => store.storePart({ ...mixed, part: 2, data: "b", eventId: "e2", digest: "other" }),
    Error,
    "Inconsistent",
  )
  await store.deleteChunk(mixed.chunkId)

  const stale = { chunkId: crypto.randomUUID(), totalParts: 2, digest: "d" }
  await store.storePart({ ...stale, part: 1, data: "a", eventId: "e1" })
  await new Promise((resolve) => setTimeout(resolve, 30))
  // Cleanup runs on the next write; the stale part is gone so this chunk starts over.
  assertEquals((await store.storePart({ ...stale, part: 2, data: "b", eventId: "e2" })).status, "stored")
})
