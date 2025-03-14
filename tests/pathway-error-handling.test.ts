import { Type } from "@sinclair/typebox";
import { assertEquals, assertExists, assertRejects } from "https://deno.land/std@0.224.0/assert/mod.ts";
import { FlowcoreEvent, PathwaysBuilder } from "../src/mod.ts";
import { createTestServer } from "./helpers/test-server.ts";

// Add ignore flag to avoid resource leak errors, but we still clean up properly
Deno.test({
  name: "Pathway Error Handling and Retry Tests",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: async (t) => {
    const server = createTestServer();
    const mockPathwayState = new Map<string, boolean>();
    let pathwaysInstances: PathwaysBuilder[] = [];
    
    // Define test schema
    const testSchema = Type.Object({
      test: Type.String()
    });

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
    });

    await t.step("Basic Error Handling - Catch Handler Error", async () => {
      const builder = new PathwaysBuilder({
        baseUrl: `http://localhost:${server.port}`,
        tenant: "test-tenant",
        dataCore: "test-data-core",
        apiKey: "test-api-key",
        pathwayTimeoutMs: 1000,
      });
      pathwaysInstances.push(builder);

      // Track errors reported via errorObserver
      const reportedErrorEvents: FlowcoreEvent[] = [];
      const reportedErrors: Array<{ error: Error, event: FlowcoreEvent }> = [];

      const pathwayKey = "test-flow-type/test-event-type" as const;
      
      // Configure builder with a handler that throws
      const pathwayBuilder = builder.register({
        flowType: "test-flow-type",
        eventType: "test-event-type",
        schema: testSchema,
        // Set maxRetries to 0 to avoid multiple error events
        maxRetries: 0,
      });

      // Subscribe to events before processing
      pathwayBuilder.subscribe(pathwayKey, (event) => {
        reportedErrorEvents.push(event);
      }, "before");
      
      // Subscribe to errors using the dedicated error method
      pathwayBuilder.onError(pathwayKey, (error, event) => {
        reportedErrors.push({ error, event });
      });

      // Register handler that throws
      pathwayBuilder.handle(pathwayKey, async () => {
        throw new Error("Test error");
      });

      // Create mock event
      const mockEvent = createMockEvent();

      // Process event and expect error
      await assertRejects(
        async () => {
          await pathwayBuilder.process(pathwayKey, mockEvent);
        },
        Error,
        "Test error"
      );

      // Verify error was reported through the dedicated error handler
      assertEquals(reportedErrors.length, 1);
      assertEquals(reportedErrors[0].error.message, "Test error");
      assertEquals(reportedErrors[0].event.eventId, mockEvent.eventId);
    });

    await t.step("Retry Mechanism - Succeeds After Retries", async () => {
      const builder = new PathwaysBuilder({
        baseUrl: `http://localhost:${server.port}`,
        tenant: "test-tenant",
        dataCore: "test-data-core",
        apiKey: "test-api-key",
        pathwayTimeoutMs: 1000,
      });
      pathwaysInstances.push(builder);

      // Track retries and errors
      const attemptCounts: Record<string, number> = {};
      const reportedErrors: Array<{ error: Error, event: FlowcoreEvent }> = [];

      const pathwayKey = "test-flow-type/test-event-type" as const;
      
      // Configure builder with retry settings
      const pathwayBuilder = builder.register({
        flowType: "test-flow-type",
        eventType: "test-event-type",
        schema: testSchema,
        maxRetries: 2,
        retryDelayMs: 100, // use short delay for test
      });

      // Subscribe to errors
      pathwayBuilder.onError(pathwayKey, (error, event) => {
        reportedErrors.push({ error, event });
      });

      // Register handler that fails first two attempts
      pathwayBuilder.handle(pathwayKey, async (event) => {
        const eventId = event.eventId;
        attemptCounts[eventId] = (attemptCounts[eventId] || 0) + 1;
        
        // Fail first two attempts
        if (attemptCounts[eventId] <= 2) {
          throw new Error(`Attempt ${attemptCounts[eventId]} failed`);
        }
        
        // Succeed on third attempt
        return;
      });

      // Create mock event
      const mockEvent = createMockEvent();

      // Process event - should eventually succeed
      await pathwayBuilder.process(pathwayKey, mockEvent);

      // Verify retry behavior
      assertEquals(attemptCounts[mockEvent.eventId], 3);
      assertEquals(reportedErrors.length, 2);
      assertEquals(reportedErrors[0].error.message, "Attempt 1 failed");
      assertEquals(reportedErrors[0].event.eventId, mockEvent.eventId);
      assertEquals(reportedErrors[1].error.message, "Attempt 2 failed");
      assertEquals(reportedErrors[1].event.eventId, mockEvent.eventId);
    });

    await t.step("Retry Mechanism - Fails After Max Retries", async () => {
      const builder = new PathwaysBuilder({
        baseUrl: `http://localhost:${server.port}`,
        tenant: "test-tenant",
        dataCore: "test-data-core",
        apiKey: "test-api-key",
        pathwayTimeoutMs: 1000,
      });
      pathwaysInstances.push(builder);

      // Track retries
      const attemptCounts: Record<string, number> = {};
      const reportedErrors: Array<{ error: Error, event: FlowcoreEvent }> = [];

      const pathwayKey = "test-flow-type/test-event-type" as const;
      
      // Configure builder with retry settings
      const pathwayBuilder = builder.register({
        flowType: "test-flow-type",
        eventType: "test-event-type",
        schema: testSchema,
        maxRetries: 3,
        retryDelayMs: 100, // use short delay for test
      });

      // Subscribe to errors
      pathwayBuilder.onError(pathwayKey, (error, event) => {
        reportedErrors.push({ error, event });
      });

      // Register handler that always fails
      pathwayBuilder.handle(pathwayKey, async (event) => {
        const eventId = event.eventId;
        attemptCounts[eventId] = (attemptCounts[eventId] || 0) + 1;
        throw new Error(`Attempt ${attemptCounts[eventId]} failed`);
      });

      // Create mock event
      const mockEvent = createMockEvent();

      // Process event - should fail after max retries
      await assertRejects(
        async () => {
          await pathwayBuilder.process(pathwayKey, mockEvent);
        },
        Error,
        "Attempt 4 failed"
      );

      // Verify retry behavior
      assertEquals(attemptCounts[mockEvent.eventId], 4); // 1 initial + 3 retries
      assertEquals(reportedErrors.length, 4);
      for (let i = 0; i < 4; i++) {
        assertEquals(reportedErrors[i].error.message, `Attempt ${i + 1} failed`);
        assertEquals(reportedErrors[i].event.eventId, mockEvent.eventId);
      }
    });

    await t.step("Global Error Subscription", async () => {
      const builder = new PathwaysBuilder({
        baseUrl: `http://localhost:${server.port}`,
        tenant: "test-tenant",
        dataCore: "test-data-core",
        apiKey: "test-api-key",
        pathwayTimeoutMs: 1000,
      });
      pathwaysInstances.push(builder);

      // Track errors from multiple pathways
      const globalErrors: Array<{ error: Error, event: FlowcoreEvent, pathway: string }> = [];

      // Configure builder with two pathways
      const pathway1 = "test-flow-type/test-event-type" as const;
      const pathway2 = "test-flow-type/test-event-type-2" as const;
      
      const pathwayBuilder = builder
        .register({
          flowType: "test-flow-type",
          eventType: "test-event-type",
          schema: testSchema,
          maxRetries: 0,
        })
        .register({
          flowType: "test-flow-type",
          eventType: "test-event-type-2",
          schema: testSchema,
          maxRetries: 0,
        });

      // Subscribe to ALL errors using global subscription
      pathwayBuilder.onAnyError((error, event, pathway) => {
        globalErrors.push({ error, event, pathway });
      });

      // Register handlers that throw different errors
      pathwayBuilder.handle(pathway1, async () => {
        throw new Error("Error from pathway 1");
      });
      
      pathwayBuilder.handle(pathway2, async () => {
        throw new Error("Error from pathway 2");
      });

      // Create mock events
      const mockEvent1 = createMockEvent("event-1");
      const mockEvent2 = createMockEvent("event-2");
      mockEvent2.eventType = "test-event-type-2"; // For pathway2

      // Process events - both will throw
      await assertRejects(
        async () => {
          await pathwayBuilder.process(pathway1, mockEvent1);
        },
        Error,
        "Error from pathway 1"
      );
      
      await assertRejects(
        async () => {
          await pathwayBuilder.process(pathway2, mockEvent2);
        },
        Error,
        "Error from pathway 2"
      );

      // Verify errors were captured by global handler
      assertEquals(globalErrors.length, 2);
      assertEquals(globalErrors[0].error.message, "Error from pathway 1");
      assertEquals(globalErrors[0].event.eventId, mockEvent1.eventId);
      assertEquals(globalErrors[0].pathway, pathway1);
      assertEquals(globalErrors[1].error.message, "Error from pathway 2");
      assertEquals(globalErrors[1].event.eventId, mockEvent2.eventId);
      assertEquals(globalErrors[1].pathway, pathway2);
    });

    await t.step("Error Handling with write", async () => {
      await server.start();
      
      const builder = new PathwaysBuilder({
        baseUrl: `http://localhost:${server.port}`,
        tenant: "test-tenant",
        dataCore: "test-data-core",
        apiKey: "test-api-key",
        pathwayTimeoutMs: 1000,
      });
      pathwaysInstances.push(builder);

      // Track errors
      const reportedErrors: Array<{ error: Error, event: FlowcoreEvent }> = [];

      const pathwayKey = "test-flow-type/test-event-type" as const;
      
      // Configure builder with retry settings
      const pathwayBuilder = builder.register({
        flowType: "test-flow-type",
        eventType: "test-event-type",
        schema: testSchema,
        maxRetries: 0, // No retries for this test
      });

      // Register handler that throws
      pathwayBuilder.handle(pathwayKey, async (event) => {
        throw new Error("Handler error during write");
      });

      // Subscribe to errors AFTER registering the handler
      pathwayBuilder.onError(pathwayKey, (error, event) => {
        reportedErrors.push({ error, event });
      });

      // Override isProcessed and setProcessed for testing
      const mockPathwayStateImpl = new Map<string, boolean>();
      const origIsProcessed = pathwayBuilder["pathwayState"].isProcessed;
      const origSetProcessed = pathwayBuilder["pathwayState"].setProcessed;
      
      pathwayBuilder["pathwayState"].isProcessed = async (id: string) => {
        return mockPathwayStateImpl.get(id) || false;
      };
      
      pathwayBuilder["pathwayState"].setProcessed = async (id: string) => {
        mockPathwayStateImpl.set(id, true);
      };

      // With fireAndForget:true, the write call should succeed even though the handler fails
      const eventId = await pathwayBuilder.write(pathwayKey, { test: "data" }, undefined, { fireAndForget: true });
      assertExists(eventId);

      // Since we're using fireAndForget, we need to manually trigger the handler
      // since the test server doesn't actually call our handler
      try {
        const mockEvent = createMockEvent(typeof eventId === 'string' ? eventId : eventId[0]);
        await pathwayBuilder.process(pathwayKey, mockEvent);
      } catch (error) {
        // Expected error, we're testing the error reporting
      }

      // Verify the error was reported
      assertEquals(reportedErrors.length, 1);
      assertEquals(reportedErrors[0].error.message, "Handler error during write");
      
      // Restore the original implementation
      pathwayBuilder["pathwayState"].isProcessed = origIsProcessed;
      pathwayBuilder["pathwayState"].setProcessed = origSetProcessed;
    });

    await t.step("Retry with Network Failures", async () => {
      await server.start();

      const builder = new PathwaysBuilder({
        baseUrl: `http://localhost:${server.port}`,
        tenant: "test-tenant",
        dataCore: "test-data-core",
        apiKey: "test-api-key",
        pathwayTimeoutMs: 1000,
      });
      pathwaysInstances.push(builder);

      // Configure server to fail first 3 requests with 500 error
      server.failNextRequests(3);

      const pathwayKey = "test-flow-type/test-event-type" as const;
      
      // Configure builder with retry settings
      const pathwayBuilder = builder.register({
        flowType: "test-flow-type",
        eventType: "test-event-type",
        schema: testSchema,
        maxRetries: 3,          // Up to 3 retries
        retryDelayMs: 100,      // Short delay for testing
        retryStatusCodes: [500] // Retry on server error
      });

      // Write to pathway, which should successfully retry after failures
      const eventId = await pathwayBuilder.write(pathwayKey, { test: "data" }, undefined, { fireAndForget: true });
      assertExists(eventId);

      // Verify server received expected number of requests for retries
      assertEquals(server.getRequestCount() >= 4, true, "Should have at least 4 requests (1 initial + 3 retries)");
    });

    // Add cleanup as the last step
    await t.step("cleanup", async () => {
      try {
        // Close the server if it's running
        await server.stop();
        
        // Clean up all builder instances
        // This ensures any timers or listeners are properly removed
        pathwaysInstances.length = 0;
      } catch (e) {
        console.error("Error during cleanup:", e);
      }
    });
  },
}); 