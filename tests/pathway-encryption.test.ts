// @ts-nocheck
import { assertEquals, assertExists, assertNotEquals, assertRejects } from "https://deno.land/std@0.224.0/assert/mod.ts"
import { Buffer } from "node:buffer"
import { z } from "zod"
import {
  aesGcmDecrypt,
  aesGcmEncrypt,
  deriveEncryptionKey,
  ENCRYPTED_PAYLOAD_FIELD,
  FlowcoreEvent,
  PATHWAY_ENCRYPTED_METADATA_KEY,
  PATHWAY_ENCRYPTION_SCHEME,
  PATHWAY_ENCRYPTION_SCHEME_METADATA_KEY,
  PathwayRouter,
  PathwaysBuilder,
} from "../src/mod.ts"
import { createTestServer } from "./helpers/test-server.ts"

const ENCRYPTION_KEY = "pathway-encryption-test-key-32-chars-ok"
const OTHER_KEY = "pathway-encryption-other-key-32-chars-ok"

const eventSchema = z.object({
  id: z.string(),
  title: z.string(),
  content: z.string(),
  tags: z.array(z.string()).optional(),
  workspaceId: z.string(),
})

function encryptedPayload(payload: unknown, key = ENCRYPTION_KEY): string {
  return aesGcmEncrypt(JSON.stringify(payload), deriveEncryptionKey(key))
}

function decryptEnvelope(envelope: Record<string, unknown>, key = ENCRYPTION_KEY): unknown {
  return JSON.parse(aesGcmDecrypt(envelope[ENCRYPTED_PAYLOAD_FIELD] as string, deriveEncryptionKey(key)))
}

function decodeMetadata(header: string): Record<string, unknown> {
  return JSON.parse(atob(header))
}

function createEvent(payload: unknown, metadata: Record<string, unknown> = {}): FlowcoreEvent {
  return {
    eventId: crypto.randomUUID(),
    timeBucket: "202607030000",
    tenant: "test-tenant",
    dataCoreId: "test-data-core",
    flowType: "encrypted-flow",
    eventType: "created",
    metadata,
    validTime: new Date().toISOString(),
    payload,
  }
}

Deno.test("pathway AES-GCM helpers roundtrip and reject invalid payloads", async () => {
  const key = deriveEncryptionKey(ENCRYPTION_KEY)

  const first = aesGcmEncrypt(JSON.stringify({ value: "hello" }), key)
  const second = aesGcmEncrypt(JSON.stringify({ value: "hello" }), key)
  assertNotEquals(first, second)
  assertEquals(JSON.parse(aesGcmDecrypt(first, key)), { value: "hello" })
  assertEquals(JSON.parse(aesGcmDecrypt(aesGcmEncrypt("{}", key), key)), {})
  assertEquals(JSON.parse(aesGcmDecrypt(aesGcmEncrypt(JSON.stringify({ text: "føroyskt 🔐" }), key), key)), {
    text: "føroyskt 🔐",
  })

  await assertRejects(
    async () => aesGcmDecrypt("not-a-valid-payload", key),
    Error,
    "Invalid AES-256-GCM payload format",
  )

  await assertRejects(
    async () => aesGcmDecrypt(first, deriveEncryptionKey(OTHER_KEY)),
    Error,
    "AES-256-GCM decryption failed",
  )
})

Deno.test({
  name: "encrypted pathway writes Flowcore-compatible envelope and marker metadata",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: async () => {
    const server = createTestServer(8010)
    await server.start()
    try {
      const builder = new PathwaysBuilder({
        baseUrl: `http://localhost:${server.port}`,
        tenant: "test-tenant",
        dataCore: "test-data-core",
        apiKey: "test-api-key",
        encryption: { mode: "symmetric", key: ENCRYPTION_KEY },
      })

      const pathway = builder.register({
        flowType: "encrypted-flow",
        eventType: "created",
        schema: eventSchema,
        encrypted: true,
      })

      const plaintext = {
        id: "fragment-1",
        title: "Top Secret",
        content: "Sensitive content",
        tags: ["confidential", "leaf"],
        workspaceId: "workspace-1",
      }

      const eventId = await pathway.write("encrypted-flow/created", {
        data: plaintext,
        metadata: { source: "test" },
        options: { fireAndForget: true, sessionId: "session-1" },
      })

      const stored = server.storedEvents.get(eventId as string) as {
        body: Record<string, unknown>
        headers: Record<string, string>
      }
      assertExists(stored)
      assertEquals(Object.keys(stored.body), [ENCRYPTED_PAYLOAD_FIELD])
      assertEquals(typeof stored.body[ENCRYPTED_PAYLOAD_FIELD], "string")
      assertNotEquals(stored.body[ENCRYPTED_PAYLOAD_FIELD], JSON.stringify(plaintext))
      assertEquals(decryptEnvelope(stored.body), plaintext)

      const metadata = decodeMetadata(stored.headers["x-flowcore-metadata-json"])
      assertEquals(metadata.source, "test")
      assertEquals(metadata["audit/session-id"], "session-1")
      assertEquals(metadata[PATHWAY_ENCRYPTED_METADATA_KEY], "true")
      assertEquals(metadata[PATHWAY_ENCRYPTION_SCHEME_METADATA_KEY], PATHWAY_ENCRYPTION_SCHEME)
    } finally {
      await server.stop()
    }
  },
})

Deno.test("encrypted pathway without configured key stays plaintext and unmarked", async () => {
  const builder = new PathwaysBuilder({
    baseUrl: "http://localhost:8011",
    tenant: "test-tenant",
    dataCore: "test-data-core",
    apiKey: "test-api-key",
    encryption: { mode: "symmetric" },
  })

  const pathway = builder.register({
    flowType: "encrypted-flow",
    eventType: "created",
    schema: eventSchema,
    encrypted: true,
  })

  let capturedPayload: Record<string, unknown> | undefined
  let capturedMetadata: Record<string, unknown> | undefined
  pathway["writers"]["encrypted-flow/created"] = async (payload, metadata) => {
    capturedPayload = payload
    capturedMetadata = metadata
    return "event-1"
  }

  await pathway.write("encrypted-flow/created", {
    data: {
      id: "fragment-1",
      title: "Plain Title",
      content: "Plain content",
      workspaceId: "workspace-1",
    },
    options: { fireAndForget: true },
  })

  assertEquals(capturedPayload?.title, "Plain Title")
  assertEquals(capturedMetadata?.[PATHWAY_ENCRYPTED_METADATA_KEY], undefined)
})

Deno.test("encryption config defaults to symmetric when only key is provided", async () => {
  const builder = new PathwaysBuilder({
    baseUrl: "http://localhost:8017",
    tenant: "test-tenant",
    dataCore: "test-data-core",
    apiKey: "test-api-key",
    encryption: { key: ENCRYPTION_KEY },
  })

  const pathway = builder.register({
    flowType: "encrypted-flow",
    eventType: "created",
    schema: eventSchema,
    encrypted: true,
  })

  let captured: Record<string, unknown> | undefined
  pathway["writers"]["encrypted-flow/created"] = async (payload) => {
    captured = payload
    return "event-1"
  }

  const plaintext = {
    id: "fragment-1",
    title: "Key Only",
    content: "Encrypted content",
    workspaceId: "workspace-1",
  }
  await pathway.write("encrypted-flow/created", {
    data: plaintext,
    options: { fireAndForget: true },
  })

  assertExists(captured)
  assertEquals(Object.keys(captured), [ENCRYPTED_PAYLOAD_FIELD])
  assertEquals(decryptEnvelope(captured), plaintext)
})

Deno.test({
  name: "encrypted pathway process decrypts marked envelope before schema validation and handler",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: async () => {
    const builder = new PathwaysBuilder({
      baseUrl: "http://localhost:8012",
      tenant: "test-tenant",
      dataCore: "test-data-core",
      apiKey: "test-api-key",
      encryption: { mode: "symmetric", key: ENCRYPTION_KEY },
    })

    const pathway = builder.register({
      flowType: "encrypted-flow",
      eventType: "created",
      schema: eventSchema,
      encrypted: true,
    })

    let handledPayload: Record<string, unknown> | undefined
    pathway.handle("encrypted-flow/created", (event) => {
      handledPayload = event.payload as Record<string, unknown>
    })

    const plaintext = {
      id: "fragment-1",
      title: "Secret Title",
      content: "Secret content",
      tags: ["leaf"],
      workspaceId: "workspace-1",
    }

    await pathway.process(
      "encrypted-flow/created",
      createEvent(
        { [ENCRYPTED_PAYLOAD_FIELD]: encryptedPayload(plaintext) },
        { [PATHWAY_ENCRYPTED_METADATA_KEY]: "true" },
      ),
    )

    assertEquals(handledPayload, plaintext)
  },
})

Deno.test({
  name: "encrypted pathway process accepts raw encrypted string when marker is present",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: async () => {
    const builder = new PathwaysBuilder({
      baseUrl: "http://localhost:8018",
      tenant: "test-tenant",
      dataCore: "test-data-core",
      apiKey: "test-api-key",
      encryption: { mode: "symmetric", key: ENCRYPTION_KEY },
    })

    const pathway = builder.register({
      flowType: "encrypted-flow",
      eventType: "created",
      schema: eventSchema,
      encrypted: true,
    })

    let handledPayload: Record<string, unknown> | undefined
    pathway.handle("encrypted-flow/created", (event) => {
      handledPayload = event.payload as Record<string, unknown>
    })

    const plaintext = {
      id: "fragment-1",
      title: "Manual Secret",
      content: "Manual content",
      workspaceId: "workspace-1",
    }

    await pathway.process(
      "encrypted-flow/created",
      createEvent(encryptedPayload(plaintext), { [PATHWAY_ENCRYPTED_METADATA_KEY]: true }),
    )

    assertEquals(handledPayload, plaintext)
  },
})

Deno.test({
  name: "encrypted pathway process leaves markerless legacy plaintext unchanged",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: async () => {
    const builder = new PathwaysBuilder({
      baseUrl: "http://localhost:8019",
      tenant: "test-tenant",
      dataCore: "test-data-core",
      apiKey: "test-api-key",
      encryption: { mode: "symmetric", key: ENCRYPTION_KEY },
    })

    const pathway = builder.register({
      flowType: "encrypted-flow",
      eventType: "created",
      schema: eventSchema,
      encrypted: true,
    })

    let handledPayload: Record<string, unknown> | undefined
    pathway.handle("encrypted-flow/created", (event) => {
      handledPayload = event.payload as Record<string, unknown>
    })

    const plaintext = {
      id: "fragment-1",
      title: "Legacy Plain",
      content: "Legacy content",
      workspaceId: "workspace-1",
    }
    await pathway.process("encrypted-flow/created", createEvent(plaintext))

    assertEquals(handledPayload, plaintext)
  },
})

Deno.test("encrypted pathway process throws on wrong key ciphertext", async () => {
  const builder = new PathwaysBuilder({
    baseUrl: "http://localhost:8013",
    tenant: "test-tenant",
    dataCore: "test-data-core",
    apiKey: "test-api-key",
    encryption: { mode: "symmetric", key: ENCRYPTION_KEY },
  })

  const pathway = builder.register({
    flowType: "encrypted-flow",
    eventType: "created",
    schema: eventSchema,
    encrypted: true,
  })

  await assertRejects(
    async () =>
      pathway.process(
        "encrypted-flow/created",
        createEvent(
          {
            [ENCRYPTED_PAYLOAD_FIELD]: encryptedPayload({
              id: "fragment-1",
              title: "Foreign title",
              content: "Foreign content",
              workspaceId: "workspace-1",
            }, OTHER_KEY),
          },
          { [PATHWAY_ENCRYPTED_METADATA_KEY]: "true" },
        ),
      ),
    Error,
    "AES-256-GCM decryption failed",
  )
})

Deno.test("encrypted pathway batch write encrypts each item independently", async () => {
  const builder = new PathwaysBuilder({
    baseUrl: "http://localhost:8014",
    tenant: "test-tenant",
    dataCore: "test-data-core",
    apiKey: "test-api-key",
    encryption: { mode: "symmetric", key: ENCRYPTION_KEY },
  })

  const pathway = builder.register({
    flowType: "encrypted-flow",
    eventType: "created",
    schema: eventSchema,
    encrypted: true,
  })

  let captured: Record<string, unknown>[] | undefined
  let capturedMetadata: Record<string, unknown> | undefined
  pathway["batchWriters"]["encrypted-flow/created"] = async (payload, metadata) => {
    captured = payload
    capturedMetadata = metadata
    return ["event-1", "event-2"]
  }

  const first = { id: "fragment-1", title: "One", content: "First", workspaceId: "workspace-1" }
  const second = { id: "fragment-2", title: "Two", content: "Second", workspaceId: "workspace-1" }
  await pathway.write("encrypted-flow/created", {
    batch: true,
    data: [first, second],
    options: { fireAndForget: true },
  })

  assertExists(captured)
  assertEquals(captured.length, 2)
  assertEquals(Object.keys(captured[0]), [ENCRYPTED_PAYLOAD_FIELD])
  assertEquals(Object.keys(captured[1]), [ENCRYPTED_PAYLOAD_FIELD])
  assertNotEquals(captured[0][ENCRYPTED_PAYLOAD_FIELD], captured[1][ENCRYPTED_PAYLOAD_FIELD])
  assertEquals(decryptEnvelope(captured[0]), first)
  assertEquals(decryptEnvelope(captured[1]), second)
  assertEquals(capturedMetadata?.[PATHWAY_ENCRYPTED_METADATA_KEY], "true")
})

Deno.test({
  name: "router path decrypts marked encrypted envelope through PathwaysBuilder.process",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: async () => {
    const builder = new PathwaysBuilder({
      baseUrl: "http://localhost:8015",
      tenant: "test-tenant",
      dataCore: "test-data-core",
      apiKey: "test-api-key",
      encryption: { mode: "symmetric", key: ENCRYPTION_KEY },
    })

    const pathway = builder.register({
      flowType: "encrypted-flow",
      eventType: "created",
      schema: eventSchema,
      encrypted: true,
    })

    let handledPayload: Record<string, unknown> | undefined
    pathway.handle("encrypted-flow/created", (event) => {
      handledPayload = event.payload as Record<string, unknown>
    })

    const plaintext = {
      id: "fragment-1",
      title: "Router Secret",
      content: "Router content",
      workspaceId: "workspace-1",
    }

    const router = new PathwayRouter(pathway, "secret")
    await router.processEvent(
      createEvent(
        { [ENCRYPTED_PAYLOAD_FIELD]: encryptedPayload(plaintext) },
        { [PATHWAY_ENCRYPTED_METADATA_KEY]: "true" },
      ),
      "secret",
    )

    assertEquals(handledPayload, plaintext)
  },
})

Deno.test("non-encrypted pathway unchanged and encrypted file pathways are rejected", async () => {
  const builder = new PathwaysBuilder({
    baseUrl: "http://localhost:8016",
    tenant: "test-tenant",
    dataCore: "test-data-core",
    apiKey: "test-api-key",
    encryption: { mode: "symmetric", key: ENCRYPTION_KEY },
  })

  const plain = builder.register({
    flowType: "plain-flow",
    eventType: "created",
    schema: eventSchema,
  })

  let captured: Record<string, unknown> | undefined
  plain["writers"]["plain-flow/created"] = async (payload) => {
    captured = payload
    return "event-1"
  }
  await plain.write("plain-flow/created", {
    data: { id: "fragment-1", title: "Plain", content: "Plain content", workspaceId: "workspace-1" },
    options: { fireAndForget: true },
  })
  assertEquals(captured?.title, "Plain")

  assertRejects(
    async () =>
      plain.register({
        flowType: "file-flow",
        eventType: "uploaded",
        schema: z.object({}),
        isFilePathway: true,
        encrypted: true,
      }),
    Error,
    "file pathway and cannot be encrypted",
  )

  const file = plain.register({
    flowType: "file-flow",
    eventType: "uploaded",
    schema: z.object({}),
    isFilePathway: true,
  })

  await assertRejects(
    async () =>
      file.write("file-flow/uploaded", {
        batch: true,
        data: [
          {
            fileId: "file-1",
            fileName: "test.txt",
            fileContent: Buffer.from("hello"),
          },
        ],
      }),
    Error,
    "Batch is not possible for file pathways",
  )
})
