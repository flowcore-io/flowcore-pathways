import { Type } from "@sinclair/typebox";
import { assertEquals, assertRejects } from "https://deno.land/std@0.224.0/assert/mod.ts";
import { ConsoleLogger, FlowcoreEvent, PathwayRouter, PathwaysBuilder } from "../src/mod.ts";
import { createTestServer } from "./helpers/test-server.ts";

// Add ignore flag to avoid resource leak errors, but we still clean up properly
Deno.test({
  name: "Router Error Handling Tests",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: async (t) => {
    const server = createTestServer();
    
    // Define test schema
    const testSchema = Type.Object({
      test: Type.String()
    });

    const TEST_SECRET_KEY = "test-secret-key";
    // Create a test logger for the router
    const testLogger = new ConsoleLogger();

    // Helper to create a mock event
    const createMockEvent = (overrides = {}): FlowcoreEvent & Record<string, unknown> => ({
      eventId: "test-event-id",
      timeBucket: "20240101000000",
      tenant: "test-tenant",
      dataCoreId: "test-data-core",
      flowType: "test-flow-type",
      eventType: "test-event-type",
      metadata: {},
      payload: { test: "data" },
      validTime: new Date().toISOString(),
      ...overrides,
    });

    // Test for invalid secret key
    await t.step("Router - Invalid Secret Key", async () => {
      const builder = new PathwaysBuilder({
        baseUrl: `http://localhost:${server.port}`,
        tenant: "test-tenant",
        dataCore: "test-data-core",
        apiKey: "test-api-key",
        pathwayTimeoutMs: 1000,
      });

      builder.register({
        flowType: "test-flow-type",
        eventType: "test-event-type",
        schema: testSchema,
      });

      const router = new PathwayRouter(builder, TEST_SECRET_KEY, testLogger);
      const mockEvent = createMockEvent();

      await assertRejects(
        async () => {
          await router.processEvent(mockEvent, "wrong-secret-key");
        },
        Error,
        "Invalid secret key"
      );
    });

    // Test for pathway not found
    await t.step("Router - Pathway Not Found", async () => {
      const builder = new PathwaysBuilder({
        baseUrl: `http://localhost:${server.port}`,
        tenant: "test-tenant",
        dataCore: "test-data-core",
        apiKey: "test-api-key",
        pathwayTimeoutMs: 1000,
      });

      // Register a different pathway than the one we'll try to process
      builder.register({
        flowType: "other-flow-type",
        eventType: "other-event-type",
        schema: testSchema,
      });

      const router = new PathwayRouter(builder, TEST_SECRET_KEY, testLogger);
      const mockEvent = createMockEvent();

      await assertRejects(
        async () => {
          await router.processEvent(mockEvent, TEST_SECRET_KEY);
        },
        Error,
        "Pathway test-flow-type/test-event-type not found"
      );
    });

    // Test for error in pathway processing
    await t.step("Router - Error in Pathway Processing", async () => {
      // deno-lint-ignore no-explicit-any
      const builder = new PathwaysBuilder<Record<string, any>, never>({
        baseUrl: `http://localhost:${server.port}`,
        tenant: "test-tenant",
        dataCore: "test-data-core",
        apiKey: "test-api-key",
        pathwayTimeoutMs: 1000,
      });

      const pathwayKey = "test-flow-type/test-event-type";
      
      // Register the pathway - this returns a new builder instance with the pathway type included
      const pathwayBuilder = builder.register({
        flowType: "test-flow-type",
        eventType: "test-event-type",
        schema: testSchema,
        maxRetries: 0, // No retries to speed up test
      });

      // Register a handler that throws an error
      // deno-lint-ignore no-explicit-any
      pathwayBuilder.handle(pathwayKey as any, async () => {
        throw new Error("Pathway processing failed");
      });

      const router = new PathwayRouter(pathwayBuilder, TEST_SECRET_KEY, testLogger);
      const mockEvent = createMockEvent();

      await assertRejects(
        async () => {
          await router.processEvent(mockEvent, TEST_SECRET_KEY);
        },
        Error,
        "Failed to process event in pathway test-flow-type/test-event-type: Pathway processing failed"
      );
    });

    // Test for successful processing with returned success status
    await t.step("Router - Successful Processing with Return Value", async () => {
      // deno-lint-ignore no-explicit-any
      const builder = new PathwaysBuilder<Record<string, any>, never>({
        baseUrl: `http://localhost:${server.port}`,
        tenant: "test-tenant",
        dataCore: "test-data-core",
        apiKey: "test-api-key",
        pathwayTimeoutMs: 1000,
      });

      const pathwayKey = "test-flow-type/test-event-type";
      
      // Register the pathway
      const pathwayBuilder = builder.register({
        flowType: "test-flow-type",
        eventType: "test-event-type",
        schema: testSchema,
      });

      // Register a handler that succeeds
      // deno-lint-ignore no-explicit-any
      pathwayBuilder.handle(pathwayKey as any, async () => {
        // Successful processing
      });

      const router = new PathwayRouter(pathwayBuilder, TEST_SECRET_KEY, testLogger);
      const mockEvent = createMockEvent();

      const result = await router.processEvent(mockEvent, TEST_SECRET_KEY);
      
      assertEquals(result, {
        success: true,
        message: "Event processed through pathway test-flow-type/test-event-type"
      });
    });

    // Test for aggregator field compatibility
    await t.step("Router - Legacy Event with Aggregator Field", async () => {
      // deno-lint-ignore no-explicit-any
      const builder = new PathwaysBuilder<Record<string, any>, never>({
        baseUrl: `http://localhost:${server.port}`,
        tenant: "test-tenant",
        dataCore: "test-data-core",
        apiKey: "test-api-key",
        pathwayTimeoutMs: 1000,
      });

      const pathwayKey = "test-flow-type/test-event-type";
      
      // Register the pathway
      const pathwayBuilder = builder.register({
        flowType: "test-flow-type",
        eventType: "test-event-type",
        schema: testSchema,
      });

      // Track processed events
      let processedEvent: FlowcoreEvent | null = null;
      // deno-lint-ignore no-explicit-any
      pathwayBuilder.handle(pathwayKey as any, async (event) => {
        processedEvent = event;
      });

      const router = new PathwayRouter(pathwayBuilder, TEST_SECRET_KEY, testLogger);
      
      // Create event with aggregator instead of flowType
      const mockEvent = createMockEvent({
        aggregator: "test-flow-type",
        flowType: undefined,
      });

      const result = await router.processEvent(mockEvent, TEST_SECRET_KEY);
      
      assertEquals(result.success, true);
      // deno-lint-ignore no-explicit-any
      assertEquals((processedEvent as any)?.flowType, "test-flow-type");
    });

    // Clean up
    await server.stop();
  }
}); 