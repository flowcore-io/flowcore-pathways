import { assertEquals, assertFalse, assertRejects, assertThrows } from "https://deno.land/std@0.224.0/assert/mod.ts"
import { z } from "zod"
import {
  buildChunkParts,
  CHUNK_DATA_FIELD,
  CHUNK_ENVELOPE_FIELD,
  createPostgresPathwayChunkStore,
  createPostgresPathwayState,
  type FlowcoreEvent,
  PATHWAY_CHUNKED_METADATA_KEY,
  PathwaysBuilder,
} from "../src/mod.ts"

const config = {
  host: Deno.env.get("POSTGRES_HOST") || "localhost",
  port: parseInt(Deno.env.get("POSTGRES_PORT") || "5432"),
  user: Deno.env.get("POSTGRES_USER") || "postgres",
  password: Deno.env.get("POSTGRES_PASSWORD") || "postgres",
  database: Deno.env.get("POSTGRES_DB") || "pathway_test",
  pool: { max: 1 },
}
const pathwayName = "chunk-recovery/created"

function fixture() {
  const payload = { id: crypto.randomUUID(), content: "æøå€😀".repeat(10_000) }
  const parts = buildChunkParts(payload, 45_000)
  const events: FlowcoreEvent[] = parts.map((part) => ({
    eventId: crypto.randomUUID(),
    timeBucket: "202610040000",
    tenant: "test-tenant",
    dataCoreId: "test-data-core",
    flowType: "chunk-recovery",
    eventType: "created",
    validTime: new Date().toISOString(),
    metadata: { [PATHWAY_CHUNKED_METADATA_KEY]: "true" },
    payload: part,
  }))
  return { payload, parts, events }
}

function consumer(prefix: string, chunkOptions: { lockTimeoutMs?: number } = {}) {
  const state = createPostgresPathwayState({ ...config, statePrefix: prefix })
  const store = createPostgresPathwayChunkStore({ ...config, statePrefix: prefix, ...chunkOptions })
  const builder = new PathwaysBuilder({
    baseUrl: "http://localhost:8099",
    tenant: "test-tenant",
    dataCore: "test-data-core",
    apiKey: "test-api-key",
  }).withPathwayState(state).withPathwayChunkStore(store).register({
    flowType: "chunk-recovery",
    eventType: "created",
    schema: z.object({ id: z.string(), content: z.string() }),
    maxRetries: 0,
  })
  return { state, store, builder }
}

async function dropTables(client: ReturnType<typeof consumer>, prefix: string) {
  // Each test owns a random namespace. Never drop another suite's tables.
  await client.state.isProcessed("initialize-cleanup")
  const adapter = (client.state as any).postgres
  await adapter.execute(`DROP TABLE IF EXISTS ${prefix}_pathway_chunks, ${prefix}_pathway_state`)
}

Deno.test("PostgreSQL chunk lock timeout rejects unbounded and invalid values", () => {
  for (const lockTimeoutMs of [0, -1, NaN, Infinity, 0.5, 2_147_483_648]) {
    assertThrows(() => createPostgresPathwayChunkStore({ ...config, lockTimeoutMs }), Error, "lockTimeoutMs must be")
  }
})

Deno.test({
  name:
    "PostgreSQL hung-handler lock waits time out without receipts or part loss and return the waiter pool connection",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: async () => {
    const prefix = `wait_${crypto.randomUUID().replaceAll("-", "")}`
    const holder = consumer(prefix)
    const waiter = consumer(prefix, { lockTimeoutMs: 60 })
    const { payload, parts, events } = fixture()
    const chunkId = parts[0][CHUNK_ENVELOPE_FIELD].id
    let entered!: () => void
    const started = new Promise<void>((resolve) => {
      entered = resolve
    })
    let release!: () => void
    const blocked = new Promise<void>((resolve) => {
      release = resolve
    })
    let processing: Promise<void> | undefined
    holder.builder.handle(pathwayName, async (received) => {
      assertEquals(received.payload, payload)
      entered()
      await blocked
    })
    try {
      for (const part of events.slice(0, -1)) await holder.builder.process(pathwayName, structuredClone(part))
      const header = parts[0][CHUNK_ENVELOPE_FIELD]
      // Initialize the distinct waiter pool before the holder acquires its processing lock.
      await waiter.store.storePart({
        chunkId,
        part: 1,
        totalParts: header.totalParts,
        digest: header.digest,
        data: parts[0][CHUNK_DATA_FIELD],
        eventId: events[0].eventId,
      })
      processing = holder.builder.process(pathwayName, structuredClone(events.at(-1)!))
      await deadline(() => started)
      await assertRejects(
        () => deadline(() => waiter.builder.process(pathwayName, structuredClone(events.at(-1)!))),
        Error,
        "Timed out acquiring pathway chunk lock after 60ms",
      )
      let ran = false
      await assertRejects(
        () =>
          deadline(() =>
            waiter.store.withChunkLock(chunkId, async () => {
              ran = true
            })
          ),
        Error,
        "Timed out acquiring pathway chunk lock after 60ms",
      )
      assertFalse(ran)
      assertFalse(await waiter.state.isProcessed(events[0].eventId))
      assertFalse(await waiter.state.isProcessed(events.at(-1)!.eventId))
      const rows = await (waiter.store as any).postgres.query(
        `SELECT part FROM ${prefix}_pathway_chunks WHERE chunk_id = $1`,
        [chunkId],
      )
      assertEquals(rows.length, parts.length)
      // max:1 would deadlock here if a timeout leaked its transaction connection.
      await deadline(() => waiter.store.withChunkLock(crypto.randomUUID(), async () => {}))
    } finally {
      release()
      await processing
      await dropTables(waiter, prefix)
      await holder.store.close()
      await holder.state.close()
      await waiter.store.close()
      await waiter.state.close()
    }
  },
})

Deno.test({
  name:
    "PostgreSQL chunk recovery survives consumer replacement and concurrent redelivery with single-connection pools",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: async () => {
    const prefix = `retry_${crypto.randomUUID().replaceAll("-", "")}`
    const clients = [consumer(prefix), consumer(prefix)]
    const [a, b] = clients
    const { payload, events } = fixture()
    const final = events.at(-1)!
    let attempts = 0
    b.builder.handle(pathwayName, (received) => {
      assertEquals(received.payload, payload)
      attempts++
      throw new Error("projection unavailable")
    })
    try {
      // Parts land on distinct stores/connections, sharing only PostgreSQL.
      for (const [index, part] of events.slice(0, -1).entries()) {
        await clients[index % 2].builder.process(pathwayName, structuredClone(part))
      }
      await assertRejects(() => b.builder.process(pathwayName, structuredClone(final)), Error, "projection unavailable")
      assertFalse(await b.state.isProcessed(events[0].eventId))
      assertFalse(await b.state.isProcessed(final.eventId))
      await b.store.close()
      await b.state.close()

      const replacement = consumer(prefix)
      clients.push(replacement)
      let entered!: () => void
      const started = new Promise<void>((resolve) => {
        entered = resolve
      })
      let release!: () => void
      const blocked = new Promise<void>((resolve) => {
        release = resolve
      })
      const handle = async (received: FlowcoreEvent) => {
        assertEquals(received.payload, payload)
        attempts++
        entered()
        await blocked
      }
      replacement.builder.handle(pathwayName, handle)
      a.builder.handle(pathwayName, handle)
      const recovered = replacement.builder.process(pathwayName, structuredClone(final))
      await started
      const duplicate = a.builder.process(pathwayName, structuredClone(final))
      try {
        await new Promise((resolve) => setTimeout(resolve, 30))
        assertEquals(attempts, 2)
        assertFalse(await replacement.state.isProcessed(final.eventId))
      } finally {
        release()
        await Promise.all([recovered, duplicate])
      }
      assertEquals(attempts, 2)
      for (const part of events) assertEquals(await replacement.state.isProcessed(part.eventId), true)
      // A full history replay must not invoke the completed handler again.
      for (const part of events) await replacement.builder.process(pathwayName, structuredClone(part))
      assertEquals(attempts, 2)
    } finally {
      await dropTables(a, prefix)
      for (const client of clients) {
        await client.store.close()
        await client.state.close()
      }
    }
  },
})

Deno.test({
  name: "PostgreSQL retained assembly recovers after the lock-owning process exits",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: async () => {
    const prefix = `crash_${crypto.randomUUID().replaceAll("-", "")}`
    const original = consumer(prefix)
    const replacement = consumer(prefix)
    const { payload, parts, events } = fixture()
    const chunkId = parts[0][CHUNK_ENVELOPE_FIELD].id
    let child: Deno.ChildProcess | undefined
    let reader: ReadableStreamDefaultReader<Uint8Array> | undefined
    let exited = false
    try {
      // Stop at durable assembly, before any successful handler receipt exists.
      for (const [index, part] of parts.entries()) {
        const header = part[CHUNK_ENVELOPE_FIELD]
        await original.store.storePart({
          chunkId,
          part: header.part,
          totalParts: header.totalParts,
          digest: header.digest,
          data: part[CHUNK_DATA_FIELD],
          eventId: events[index].eventId,
        })
      }
      const moduleUrl = new URL("../src/mod.ts", import.meta.url).href
      const script = `
        import { createPostgresPathwayChunkStore } from ${JSON.stringify(moduleUrl)};
        const store = createPostgresPathwayChunkStore(${JSON.stringify({ ...config, statePrefix: prefix })});
        await store.withChunkLock(${JSON.stringify(chunkId)}, async () => {
          console.log("locked");
          await new Promise(() => {});
        });
      `
      child = new Deno.Command(Deno.execPath(), {
        args: ["eval", "--quiet", script],
        stdout: "piped",
        stderr: "inherit",
      }).spawn()
      reader = child.stdout.getReader()
      await deadline(async () => {
        let output = ""
        while (!output.includes("locked")) {
          const next = await reader!.read()
          if (next.done) throw new Error("child exited before acquiring the chunk lock")
          output += new TextDecoder().decode(next.value)
        }
      })
      child.kill("SIGKILL")
      await child.status
      exited = true
      let handled = 0
      replacement.builder.handle(pathwayName, (received) => {
        assertEquals(received.payload, payload)
        handled++
      })
      await deadline(() => replacement.builder.process(pathwayName, structuredClone(events.at(-1)!)))
      assertEquals(handled, 1)
      for (const part of events) assertEquals(await replacement.state.isProcessed(part.eventId), true)
    } finally {
      if (child && !exited) {
        child.kill("SIGKILL")
        await child.status
      }
      reader?.releaseLock()
      await dropTables(original, prefix)
      await original.store.close()
      await original.state.close()
      await replacement.store.close()
      await replacement.state.close()
    }
  },
})

async function deadline(action: () => Promise<unknown>): Promise<void> {
  let timeout!: ReturnType<typeof setTimeout>
  try {
    await Promise.race([
      action(),
      new Promise((_, reject) => {
        timeout = setTimeout(() => reject(new Error("recovery timed out")), 10_000)
      }),
    ])
  } finally {
    clearTimeout(timeout)
  }
}
