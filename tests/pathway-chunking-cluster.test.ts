import { assertEquals } from "https://deno.land/std@0.224.0/assert/mod.ts"
import { Buffer } from "node:buffer"
import { z } from "zod"
import {
  buildChunkParts,
  type FlowcoreEvent,
  InternalPathwayChunkStore,
  PATHWAY_CHUNKED_METADATA_KEY,
  PathwaysBuilder,
} from "../src/mod.ts"
import { InMemoryCoordinator } from "./helpers/in-memory-coordinator.ts"

const eventSchema = z.object({
  id: z.string(),
  content: z.string(),
})

function largeContent(bytes: number): string {
  let out = ""
  while (Buffer.byteLength(out, "utf8") < bytes) {
    out += "chunk-æøå-"
  }
  return out
}

function createBuilder(port: number) {
  return new PathwaysBuilder({
    baseUrl: `http://localhost:${port}`,
    tenant: "test-tenant",
    dataCore: "test-data-core",
    apiKey: "test-api-key",
  }).withPathwayChunkStore(new InternalPathwayChunkStore())
}

function createPartEvent(part: unknown, eventId: string): FlowcoreEvent {
  return {
    eventId,
    timeBucket: "202609140000",
    tenant: "test-tenant",
    dataCoreId: "test-data-core",
    flowType: "big-flow",
    eventType: "created",
    metadata: {
      "some/unrelated-key": "keep-me",
      [PATHWAY_CHUNKED_METADATA_KEY]: "true",
    },
    validTime: new Date().toISOString(),
    payload: part,
  }
}

const plaintext = { id: "1", content: largeContent(100_000) }

Deno.test({
  name: "chunked event is reassembled once when the cluster re-enters process()",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: async () => {
    const builder = createBuilder(8041)
    const pathway = builder.register({
      flowType: "big-flow",
      eventType: "created",
      schema: eventSchema,
    })

    const handled: unknown[] = []
    pathway.handle("big-flow/created", (event) => {
      handled.push(event.payload)
    })

    await builder.startCluster({
      coordinator: new InMemoryCoordinator(),
      advertisedAddress: "ws://localhost:19111",
      port: 19111,
    })

    try {
      const parts = buildChunkParts(plaintext, 45_000)
      const events = parts.map((part, index) => createPartEvent(part, `part-${index + 1}`))
      for (const event of events) {
        await pathway.process("big-flow/created", event)
      }

      assertEquals(handled, [plaintext])
      // The completing event was mutated into the logical event: markers gone, unrelated metadata kept.
      const last = events[events.length - 1]
      assertEquals(last.metadata, { "some/unrelated-key": "keep-me" })
      assertEquals(last.eventId, "part-1")
    } finally {
      await builder.stopCluster()
    }
  },
})

Deno.test({
  name: "reassembled event survives serialization to a cluster worker without re-entering the chunk path",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: async () => {
    const leader = createBuilder(8042)
    const leaderPathway = leader.register({ flowType: "big-flow", eventType: "created", schema: eventSchema })
    leaderPathway.handle("big-flow/created", () => {})

    const worker = createBuilder(8043)
    const workerPathway = worker.register({ flowType: "big-flow", eventType: "created", schema: eventSchema })
    const handled: unknown[] = []
    workerPathway.handle("big-flow/created", (event) => {
      handled.push(event.payload)
    })

    const parts = buildChunkParts(plaintext, 45_000)
    const events = parts.map((part, index) => createPartEvent(part, `part-${index + 1}`))
    for (const event of events) {
      await leaderPathway.process("big-flow/created", event)
    }

    const overTheWire = JSON.parse(JSON.stringify(events[events.length - 1])) as FlowcoreEvent
    await workerPathway.process("big-flow/created", overTheWire)

    assertEquals(handled, [plaintext])
  },
})
