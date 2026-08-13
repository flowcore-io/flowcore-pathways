import { assertEquals } from "https://deno.land/std@0.224.0/assert/mod.ts"
import { z } from "zod"
import {
  aesGcmEncrypt,
  deriveEncryptionKey,
  ENCRYPTED_PAYLOAD_FIELD,
  type FlowcoreEvent,
  PATHWAY_ENCRYPTED_METADATA_KEY,
  PATHWAY_ENCRYPTION_SCHEME,
  PATHWAY_ENCRYPTION_SCHEME_METADATA_KEY,
  PathwaysBuilder,
} from "../src/mod.ts"
import { InMemoryCoordinator } from "./helpers/in-memory-coordinator.ts"

const ENCRYPTION_KEY = "pathway-encryption-test-key-32-chars-ok"

const eventSchema = z.object({
  id: z.string(),
  title: z.string(),
  workspaceId: z.string(),
})

function encryptedPayload(payload: unknown): string {
  return aesGcmEncrypt(JSON.stringify(payload), deriveEncryptionKey(ENCRYPTION_KEY))
}

function createEncryptedEvent(payload: unknown): FlowcoreEvent {
  return {
    eventId: crypto.randomUUID(),
    timeBucket: "202608130000",
    tenant: "test-tenant",
    dataCoreId: "test-data-core",
    flowType: "encrypted-flow",
    eventType: "created",
    metadata: {
      "some/unrelated-key": "keep-me",
      [PATHWAY_ENCRYPTED_METADATA_KEY]: "true",
      [PATHWAY_ENCRYPTION_SCHEME_METADATA_KEY]: PATHWAY_ENCRYPTION_SCHEME,
    },
    validTime: new Date().toISOString(),
    payload: { [ENCRYPTED_PAYLOAD_FIELD]: encryptedPayload(payload) },
  }
}

function createEncryptedBuilder(port: number) {
  return new PathwaysBuilder({
    baseUrl: `http://localhost:${port}`,
    tenant: "test-tenant",
    dataCore: "test-data-core",
    apiKey: "test-api-key",
    encryption: { mode: "symmetric", key: ENCRYPTION_KEY },
  })
}

const plaintext = {
  id: "fragment-1",
  title: "Secret Title",
  workspaceId: "workspace-1",
}

Deno.test({
  name: "encrypted pathway decrypts exactly once when the cluster re-enters process()",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: async () => {
    const builder = createEncryptedBuilder(8031)
    const pathway = builder.register({
      flowType: "encrypted-flow",
      eventType: "created",
      schema: eventSchema,
      encrypted: true,
    })

    const handled: unknown[] = []
    pathway.handle("encrypted-flow/created", (event) => {
      handled.push(event.payload)
    })

    await builder.startCluster({
      coordinator: new InMemoryCoordinator(),
      advertisedAddress: "ws://localhost:19101",
      port: 19101,
    })

    try {
      const event = createEncryptedEvent(plaintext)
      await pathway.process("encrypted-flow/created", event)

      // The cluster event handler re-enters process(); the payload must be decrypted only once.
      assertEquals(handled, [plaintext])

      // The markers no longer describe the payload, so they are gone — but unrelated metadata stays.
      assertEquals(event.metadata, { "some/unrelated-key": "keep-me" })
    } finally {
      await builder.stopCluster()
    }
  },
})

Deno.test({
  name: "encrypted pathway event survives serialization to a cluster worker",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: async () => {
    // The leader decrypts, then serializes the event and ships it to a worker, whose cluster event
    // handler calls process() again. The worker must not try to decrypt the plaintext payload.
    const leader = createEncryptedBuilder(8032)
    const leaderPathway = leader.register({
      flowType: "encrypted-flow",
      eventType: "created",
      schema: eventSchema,
      encrypted: true,
    })
    leaderPathway.handle("encrypted-flow/created", () => {})

    const worker = createEncryptedBuilder(8033)
    const workerPathway = worker.register({
      flowType: "encrypted-flow",
      eventType: "created",
      schema: eventSchema,
      encrypted: true,
    })

    const handled: unknown[] = []
    workerPathway.handle("encrypted-flow/created", (event) => {
      handled.push(event.payload)
    })

    const event = createEncryptedEvent(plaintext)
    await leaderPathway.process("encrypted-flow/created", event)

    const overTheWire = JSON.parse(JSON.stringify(event)) as FlowcoreEvent
    await workerPathway.process("encrypted-flow/created", overTheWire)

    assertEquals(handled, [plaintext])
  },
})

Deno.test({
  name: "application-level encrypted envelopes keep their metadata markers",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: async () => {
    // A pathway that is not registered with `encrypted: true` owns its envelope in the handler, so
    // the library must leave both the payload and the markers alone.
    const builder = createEncryptedBuilder(8034)
    const pathway = builder.register({
      flowType: "encrypted-flow",
      eventType: "created",
      schema: z.object({ [ENCRYPTED_PAYLOAD_FIELD]: z.string() }),
    })

    let handledEvent: FlowcoreEvent | undefined
    pathway.handle("encrypted-flow/created", (event) => {
      handledEvent = event
    })

    const event = createEncryptedEvent(plaintext)
    const originalPayload = event.payload
    await pathway.process("encrypted-flow/created", event)

    assertEquals(handledEvent?.payload, originalPayload)
    assertEquals(handledEvent?.metadata[PATHWAY_ENCRYPTED_METADATA_KEY], "true")
    assertEquals(handledEvent?.metadata[PATHWAY_ENCRYPTION_SCHEME_METADATA_KEY], PATHWAY_ENCRYPTION_SCHEME)
  },
})
