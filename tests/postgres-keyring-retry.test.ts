import { assert, assertEquals, assertFalse, assertRejects } from "https://deno.land/std@0.224.0/assert/mod.ts"
import { z } from "zod"
import {
  aesGcmEncrypt,
  createPostgresPathwayState,
  deriveEncryptionKey,
  ENCRYPTED_PAYLOAD_FIELD,
  FlowcoreEvent,
  PATHWAY_ENCRYPTED_METADATA_KEY,
  PATHWAY_ENCRYPTION_KEY_ID_METADATA_KEY,
  PathwaysBuilder,
} from "../src/mod.ts"

const config = {
  host: Deno.env.get("POSTGRES_HOST") || "localhost",
  port: parseInt(Deno.env.get("POSTGRES_PORT") || "5432"),
  user: Deno.env.get("POSTGRES_USER") || "postgres",
  password: Deno.env.get("POSTGRES_PASSWORD") || "postgres",
  database: Deno.env.get("POSTGRES_DB") || "pathway_test",
}

const OLD_KEY = "postgres-keyring-old-key-32-chars-ok"
const ACTIVE_KEY = "postgres-keyring-active-key-32-chars-ok"

function event(
  eventId: string,
  payload: unknown,
  metadata: Record<string, string> = {},
): FlowcoreEvent {
  return {
    eventId,
    timeBucket: "202609190000",
    tenant: "test-tenant",
    dataCoreId: "test-data-core",
    flowType: "postgres-keyring-flow",
    eventType: "created",
    metadata,
    validTime: new Date().toISOString(),
    payload,
  }
}

function encryptedEvent(eventId: string, keyId: string, key: string): FlowcoreEvent {
  return event(
    eventId,
    {
      [ENCRYPTED_PAYLOAD_FIELD]: aesGcmEncrypt(
        JSON.stringify({ id: eventId, value: "retained" }),
        deriveEncryptionKey(key),
      ),
    },
    {
      [PATHWAY_ENCRYPTED_METADATA_KEY]: "true",
      [PATHWAY_ENCRYPTION_KEY_ID_METADATA_KEY]: keyId,
    },
  )
}

Deno.test({
  name: "PostgreSQL state decrypts retained key IDs and rejects unknown IDs",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: async () => {
    const tableName = "pathway_state_cr07_keyring"
    const state = createPostgresPathwayState({ ...config, tableName })
    const builder = new PathwaysBuilder({
      baseUrl: "http://localhost:8023",
      tenant: "test-tenant",
      dataCore: "test-data-core",
      apiKey: "test-api-key",
      encryption: {
        keyring: {
          activeKeyId: "v2",
          resolveKey: (keyId) => ({ v1: OLD_KEY, v2: ACTIVE_KEY })[keyId],
        },
      },
    }).withPathwayState(state)
    const pathway = builder.register({
      flowType: "postgres-keyring-flow",
      eventType: "created",
      schema: z.object({ id: z.string(), value: z.string() }),
      encrypted: true,
    })
    const handled: string[] = []
    pathway.handle("postgres-keyring-flow/created", (received) => {
      handled.push((received.payload as { id: string }).id)
    })

    try {
      const oldEvent = encryptedEvent("postgres-old-key", "v1", OLD_KEY)
      await pathway.process("postgres-keyring-flow/created", oldEvent)
      assertEquals(handled, ["postgres-old-key"])
      assert(await state.isProcessed(oldEvent.eventId))

      const unknownEvent = encryptedEvent("postgres-unknown-key", "retired", OLD_KEY)
      await assertRejects(
        () => pathway.process("postgres-keyring-flow/created", unknownEvent),
        Error,
        "Unknown encryption key ID",
      )
      assertFalse(await state.isProcessed(unknownEvent.eventId))
    } finally {
      const adapter = (state as any).postgres
      if (adapter) await adapter.execute(`DROP TABLE IF EXISTS ${tableName}`)
      await state.close()
    }
  },
})

Deno.test({
  name: "PostgreSQL state keeps terminal failures replayable",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: async () => {
    const tableName = "pathway_state_cr07_retry"
    const state = createPostgresPathwayState({ ...config, tableName })
    const builder = new PathwaysBuilder({
      baseUrl: "http://localhost:8024",
      tenant: "test-tenant",
      dataCore: "test-data-core",
      apiKey: "test-api-key",
    }).withPathwayState(state)
    const pathway = builder.register({
      flowType: "postgres-retry-flow",
      eventType: "failed",
      schema: z.object({ id: z.string() }),
      maxRetries: 0,
    })
    let attempts = 0
    pathway.handle("postgres-retry-flow/failed", () => {
      attempts++
      if (attempts === 1) throw new Error("transient projection failure")
    })

    const failedEvent = event("postgres-retry-event", { id: "retry-me" })
    try {
      await assertRejects(
        () => pathway.process("postgres-retry-flow/failed", failedEvent),
        Error,
        "transient projection failure",
      )
      assertFalse(await state.isProcessed(failedEvent.eventId))

      await pathway.process("postgres-retry-flow/failed", failedEvent)
      assertEquals(attempts, 2)
      assert(await state.isProcessed(failedEvent.eventId))
    } finally {
      const adapter = (state as any).postgres
      if (adapter) await adapter.execute(`DROP TABLE IF EXISTS ${tableName}`)
      await state.close()
    }
  },
})
