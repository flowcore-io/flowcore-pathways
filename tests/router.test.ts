// @ts-nocheck
import { Type } from "@sinclair/typebox";
import { assertEquals, assertExists, assertRejects, assertThrows } from "https://deno.land/std@0.224.0/assert/mod.ts";
import { FlowcoreEvent, PathwayRouter, PathwaysBuilder } from "../src/mod.ts";
import { createTestServer } from "./helpers/test-server.ts";

type FlowcoreEventWithAggregator = FlowcoreEvent & {
  aggregator?: string;
};

Deno.test("Router Tests", async (t) => {
  const server = createTestServer();
  const testSchema = Type.Object({
    id: Type.String(),
    organizationId: Type.String(),
    dataCoreId: Type.String(),
    flowTypeId: Type.String(),
    name: Type.String(),
  });

  const TEST_SECRET_KEY = "test-secret-key";

  // Create a test event with the correct structure
  const createTestEvent = (overrides = {}): Omit<FlowcoreEventWithAggregator, keyof typeof overrides> & typeof overrides => ({
    eventId: "test-event-id",
    tenant: "test-tenant",
    dataCoreId: "test-data-core",
    flowType: "event-type.1",
    eventType: "event-type.created.0",
    metadata: {},
    timeBucket: "202403201200",
    validTime: new Date().toISOString(),
    payload: {
      id: "test-id",
      organizationId: "test-org",
      dataCoreId: "test-data-core",
      flowTypeId: "test-flow-type",
      name: "test-name",
    },
    ...overrides,
  });

  await t.step("Router - Valid Pathway Key", async () => {
    // We don't need the server for this test as we're just testing the router logic
    const pathways = new PathwaysBuilder({
      baseUrl: `http://localhost:${server.port}`,
      tenant: "test-tenant",
      dataCore: "test-data-core",
      apiKey: "test-api-key",
      pathwayTimeoutMs: 1000,
    });

    const pathwayKey = "event-type.1/event-type.created.0" as const;
    pathways.register({
      flowType: "event-type.1",
      eventType: "event-type.created.0",
      schema: testSchema,
    });

    // Mock the process method
    let processedPathway = "";
    let processedEvent: FlowcoreEvent | null = null;
    const originalProcess = pathways.process;
    // deno-lint-ignore no-explicit-any
    pathways.process = async (pathway: any, event: FlowcoreEvent) => {
      processedPathway = pathway as string;
      processedEvent = event;
    };

    const router = new PathwayRouter(pathways, TEST_SECRET_KEY);

    // Test valid event processing
    const validEvent = createTestEvent();

    await router.processEvent(validEvent as FlowcoreEvent, TEST_SECRET_KEY);

    // Verify the router processed the event correctly
    assertEquals(processedPathway, pathwayKey);
    assertExists(processedEvent, "processedEvent should not be null");
    
    // Type assertions to appease the linter
    if (processedEvent) {
      assertEquals(processedEvent.eventId, validEvent.eventId);
      
      const processedPayload = processedEvent.payload as Record<string, unknown>;
      const validPayload = validEvent.payload as Record<string, unknown>;
      assertEquals(processedPayload.id, validPayload.id);
    }
    
    // Restore original method
    pathways.process = originalProcess;
  });

  await t.step("Router - Unknown Pathway", async () => {
    const pathways = new PathwaysBuilder({
      baseUrl: `http://localhost:${server.port}`,
      tenant: "test-tenant",
      dataCore: "test-data-core",
      apiKey: "test-api-key",
      pathwayTimeoutMs: 1000,
    });

    const router = new PathwayRouter(pathways, TEST_SECRET_KEY);

    // Test event with unknown pathway
    const unknownEvent = createTestEvent({
      flowType: "unknown-flow-type",
      eventType: "unknown-event-type",
    });

    // Assert that processing an unknown pathway throws an error
    await assertRejects(
      async () => {
        await router.processEvent(unknownEvent as FlowcoreEvent, TEST_SECRET_KEY);
      },
      Error,
      "Pathway unknown-flow-type/unknown-event-type not found"
    );
  });

  await t.step("Router - Multiple Pathways", async () => {
    const pathways = new PathwaysBuilder({
      baseUrl: `http://localhost:${server.port}`,
      tenant: "test-tenant",
      dataCore: "test-data-core",
      apiKey: "test-api-key",
      pathwayTimeoutMs: 1000,
    });

    // Register multiple pathways
    pathways
      .register({
        flowType: "event-type.1",
        eventType: "event-type.created.0",
        schema: testSchema,
      })
      .register({
        flowType: "event-type.1",
        eventType: "event-type.updated.0",
        schema: testSchema,
      });

    // Mock the process method
    const processedPathways: string[] = [];
    const originalProcess = pathways.process;
    // deno-lint-ignore no-explicit-any
    pathways.process = async (pathway: any, event: FlowcoreEvent) => {
      processedPathways.push(pathway as string);
    };

    const router = new PathwayRouter(pathways, TEST_SECRET_KEY);

    // Process multiple events
    const events = [
      createTestEvent({
        eventId: "test-event-id-1",
        eventType: "event-type.created.0",
      }),
      createTestEvent({
        eventId: "test-event-id-2",
        eventType: "event-type.updated.0",
      }),
    ];

    for (const event of events) {
      await router.processEvent(event as FlowcoreEvent, TEST_SECRET_KEY);
    }

    // Verify both events were processed with correct pathways
    assertEquals(processedPathways.length, 2);
    assertEquals(processedPathways[0], "event-type.1/event-type.created.0");
    assertEquals(processedPathways[1], "event-type.1/event-type.updated.0");
    
    // Restore original method
    pathways.process = originalProcess;
  });

  await t.step("Router - Aggregator Field Support", async () => {
    const pathways = new PathwaysBuilder({
      baseUrl: `http://localhost:${server.port}`,
      tenant: "test-tenant",
      dataCore: "test-data-core",
      apiKey: "test-api-key",
      pathwayTimeoutMs: 1000,
    });

    const pathwayKey = "legacy-flow/event-type.created.0" as const;
    pathways.register({
      flowType: "legacy-flow",
      eventType: "event-type.created.0",
      schema: testSchema,
    });

    // Mock the process method
    let processedPathway = "";
    let processedEvent: FlowcoreEvent | null = null;
    const originalProcess = pathways.process;
    // deno-lint-ignore no-explicit-any
    pathways.process = async (pathway: any, event: FlowcoreEvent) => {
      processedPathway = pathway as string;
      processedEvent = event;
    };

    const router = new PathwayRouter(pathways, TEST_SECRET_KEY);

    // Test event with aggregator field instead of flowType
    const legacyEvent = createTestEvent({
      flowType: undefined,
      aggregator: "legacy-flow", // Legacy field
      eventType: "event-type.created.0",
    });

    await router.processEvent(legacyEvent as FlowcoreEvent, TEST_SECRET_KEY);

    // Verify the router processed the event correctly using the aggregator field
    assertEquals(processedPathway, pathwayKey);
    assertExists(processedEvent, "processedEvent should not be null");
    
    // Type assertions to appease the linter
    if (processedEvent) {
      assertEquals(processedEvent.flowType, "legacy-flow");
    }
    
    // Restore original method
    pathways.process = originalProcess;
  });

  await t.step("Router - Invalid Secret Key", async () => {
    const pathways = new PathwaysBuilder({
      baseUrl: `http://localhost:${server.port}`,
      tenant: "test-tenant",
      dataCore: "test-data-core",
      apiKey: "test-api-key",
      pathwayTimeoutMs: 1000,
    });

    const router = new PathwayRouter(pathways, TEST_SECRET_KEY);

    // Test valid event with wrong secret key
    const validEvent = createTestEvent();

    // Assert that processing with wrong secret key throws an error
    await assertRejects(
      async () => {
        await router.processEvent(validEvent as FlowcoreEvent, "wrong-secret-key");
      },
      Error,
      "Invalid secret key"
    );
  });

  await t.step("Router - Constructor Requires Secret Key", async () => {
    const pathways = new PathwaysBuilder({
      baseUrl: `http://localhost:${server.port}`,
      tenant: "test-tenant",
      dataCore: "test-data-core",
      apiKey: "test-api-key",
      pathwayTimeoutMs: 1000,
    });

    // Assert that creating router with empty secret key throws an error
    assertThrows(
      () => {
        new PathwayRouter(pathways, "");
      },
      Error,
      "Secret key is required for PathwayRouter"
    );
  });
}); 