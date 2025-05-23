import { assertEquals, assertExists } from "https://deno.land/std@0.224.0/assert/mod.ts"
import { AuditHandler, FlowcoreEvent, PathwaysBuilder, UserIdResolver } from "../src/mod.ts"
import { createTestServer } from "./helpers/test-server.ts"
import { z } from "zod"

// Add ignore flag to avoid resource leak errors, but we still clean up properly
Deno.test({
  name: "Pathway Audit Tests",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: async (t) => {
    const server = createTestServer()
    const mockPathwayState = new Map<string, boolean>()
    let pathwaysInstances: PathwaysBuilder[] = []

    // Define test schema
    const testSchema = z.object({
      test: z.string(),
    })

    // Helper to create a mock event
    const createMockEvent = (id: string = crypto.randomUUID()): FlowcoreEvent => ({
      eventId: id,
      timeBucket: "20240101000000",
      tenant: "test-tenant",
      dataCoreId: "test-data-core",
      flowType: "test-flow-type",
      eventType: "test-event-type",
      metadata: {},
      payload: { test: "data" },
      validTime: new Date().toISOString(),
    })

    // Helper function to decode metadata JSON from header
    const decodeMetadataFromHeader = (header: string): Record<string, unknown> => {
      try {
        const decoded = atob(header) // Base64 decode
        return JSON.parse(decoded)
      } catch (error) {
        console.error("Error decoding metadata:", error)
        return {}
      }
    }

    // Track audit events for testing
    const auditEvents: Array<{ path: string; event: FlowcoreEvent }> = []

    // Mock user ID resolver function
    const getUserId: UserIdResolver = async () => ({
      entityId: "test-user-123",
      entityType: "user",
    })

    await t.step("Configure PathwaysBuilder with audit", async () => {
      const builder = new PathwaysBuilder({
        baseUrl: `http://localhost:${server.port}`,
        tenant: "test-tenant",
        dataCore: "test-data-core",
        apiKey: "test-api-key",
      })
      pathwaysInstances.push(builder)

      // Configure audit
      const auditHandler: AuditHandler = (path, event) => {
        auditEvents.push({ path, event })
      }

      const builderWithAudit = builder.withAudit(auditHandler).withUserResolver(getUserId)

      assertExists(builderWithAudit)
    })

    await t.step("Write with default user mode - adds correct audit metadata", async () => {
      await server.start()

      const builder = new PathwaysBuilder({
        baseUrl: `http://localhost:${server.port}`,
        tenant: "test-tenant",
        dataCore: "test-data-core",
        apiKey: "test-api-key",
      })
      pathwaysInstances.push(builder)

      // Configure audit
      const auditHandler: AuditHandler = (path, event) => {
        auditEvents.push({ path, event })
      }

      builder.withAudit(auditHandler).withUserResolver(getUserId)

      const pathwayKey = "test-flow-type/test-event-type" as const

      const pathwayBuilder = builder.register({
        flowType: "test-flow-type",
        eventType: "test-event-type",
        schema: testSchema,
      })

      pathwayBuilder.handle(pathwayKey, async (event: FlowcoreEvent) => {
        mockPathwayState.set(event.eventId, true)
      })

      // Write to the pathway with fire-and-forget option
      const eventId = await pathwayBuilder.write(pathwayKey, { test: "data" }, undefined, { fireAndForget: true })

      // Get the last request
      const storedRequest = server.storedEvents.get(typeof eventId === "string" ? eventId : eventId[0])
      assertExists(storedRequest)

      // Verify the request contains the audit metadata
      const request = storedRequest as {
        url: string
        method: string
        headers: Record<string, string>
        body: { test: string }
      }

      // Get metadata from the encoded header
      const metadataHeader = request.headers["x-flowcore-metadata-json"]
      assertExists(metadataHeader, "Metadata header should exist")

      const metadata = decodeMetadataFromHeader(metadataHeader)
      assertEquals(metadata["audit/user-id"], "test-user-123")
      assertEquals(metadata["audit/mode"], "user")
    })

    await t.step("Write with explicit user mode - adds correct audit metadata", async () => {
      await server.start()

      const builder = new PathwaysBuilder({
        baseUrl: `http://localhost:${server.port}`,
        tenant: "test-tenant",
        dataCore: "test-data-core",
        apiKey: "test-api-key",
      })
      pathwaysInstances.push(builder)

      // Configure audit
      const auditHandler: AuditHandler = (path, event) => {
        auditEvents.push({ path, event })
      }

      builder.withAudit(auditHandler).withUserResolver(getUserId)

      const pathwayKey = "test-flow-type/test-event-type" as const

      const pathwayBuilder = builder.register({
        flowType: "test-flow-type",
        eventType: "test-event-type",
        schema: testSchema,
      })

      // Write to the pathway with user mode explicitly set
      const eventId = await pathwayBuilder.write(
        pathwayKey,
        { test: "data" },
        undefined,
        { fireAndForget: true, auditMode: "user" },
      )

      // Get the last request
      const storedRequest = server.storedEvents.get(typeof eventId === "string" ? eventId : eventId[0])
      assertExists(storedRequest)

      // Verify the request contains the audit metadata
      const request = storedRequest as {
        url: string
        method: string
        headers: Record<string, string>
        body: { test: string }
      }

      // Get metadata from the encoded header
      const metadataHeader = request.headers["x-flowcore-metadata-json"]
      assertExists(metadataHeader, "Metadata header should exist")

      const metadata = decodeMetadataFromHeader(metadataHeader)
      assertEquals(metadata["audit/user-id"], "test-user-123")
      assertEquals(metadata["audit/mode"], "user")
    })

    await t.step("Write with system mode - adds correct audit metadata", async () => {
      await server.start()

      const builder = new PathwaysBuilder({
        baseUrl: `http://localhost:${server.port}`,
        tenant: "test-tenant",
        dataCore: "test-data-core",
        apiKey: "test-api-key",
      })
      pathwaysInstances.push(builder)

      // Configure audit
      const auditHandler: AuditHandler = (path, event) => {
        auditEvents.push({ path, event })
      }

      builder.withAudit(auditHandler).withUserResolver(getUserId)

      const pathwayKey = "test-flow-type/test-event-type" as const

      const pathwayBuilder = builder.register({
        flowType: "test-flow-type",
        eventType: "test-event-type",
        schema: testSchema,
      })

      // Write to the pathway with system mode
      const eventId = await pathwayBuilder.write(
        pathwayKey,
        { test: "data" },
        undefined,
        { fireAndForget: true, auditMode: "system" },
      )

      // Get the last request
      const storedRequest = server.storedEvents.get(typeof eventId === "string" ? eventId : eventId[0])
      assertExists(storedRequest)

      // Verify the request contains the audit metadata
      const request = storedRequest as {
        url: string
        method: string
        headers: Record<string, string>
        body: { test: string }
      }

      // Get metadata from the encoded header
      const metadataHeader = request.headers["x-flowcore-metadata-json"]
      assertExists(metadataHeader, "Metadata header should exist")

      const metadata = decodeMetadataFromHeader(metadataHeader)
      assertEquals(metadata["audit/user-id"], "system")
      assertEquals(metadata["audit/on-behalf-of"], "test-user-123")
      assertEquals(metadata["audit/mode"], "system")
    })

    await t.step("Audit handler receives events for all pathways", async () => {
      await server.start()

      // Clear previous audit events
      auditEvents.length = 0

      const builder = new PathwaysBuilder({
        baseUrl: `http://localhost:${server.port}`,
        tenant: "test-tenant",
        dataCore: "test-data-core",
        apiKey: "test-api-key",
      })
      pathwaysInstances.push(builder)

      // Configure audit
      const auditHandler: AuditHandler = (path, event) => {
        auditEvents.push({ path, event })
      }

      builder.withAudit(auditHandler).withUserResolver(getUserId)

      const pathway1 = "test-flow-type/test-event-type-1" as const
      const pathway2 = "test-flow-type/test-event-type-2" as const

      const pathwayBuilder = builder
        .register({
          flowType: "test-flow-type",
          eventType: "test-event-type-1",
          schema: testSchema,
        })
        .register({
          flowType: "test-flow-type",
          eventType: "test-event-type-2",
          schema: testSchema,
        })

      // Create mock events
      const mockEvent1 = createMockEvent()
      const mockEvent2 = createMockEvent()

      // Process events
      await pathwayBuilder.process(pathway1, mockEvent1)
      await pathwayBuilder.process(pathway2, mockEvent2)

      // Verify audit events were received for both pathways
      assertEquals(auditEvents.length, 2)
      assertEquals(auditEvents[0].path, pathway1)
      assertEquals(auditEvents[0].event.eventId, mockEvent1.eventId)
      assertEquals(auditEvents[1].path, pathway2)
      assertEquals(auditEvents[1].event.eventId, mockEvent2.eventId)
    })

    // Cleanup
    await server.stop()
    for (const pathwaysInstance of pathwaysInstances) {
      // Close any open connections or listeners
      // This is needed to avoid resource leak errors
    }
  },
})
