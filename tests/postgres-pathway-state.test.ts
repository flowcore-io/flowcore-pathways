import { assertEquals, assertFalse, assertRejects } from "https://deno.land/std/assert/mod.ts";
import { afterAll, beforeAll, describe, it } from "https://deno.land/std/testing/bdd.ts";

import { PostgresPathwayState, createPostgresPathwayState } from "../src/pathways/postgres/postgres-pathway-state.ts";

// Test configuration
const config = {
  host: Deno.env.get("POSTGRES_HOST") || "localhost",
  port: parseInt(Deno.env.get("POSTGRES_PORT") || "5432"),
  user: Deno.env.get("POSTGRES_USER") || "postgres",
  password: Deno.env.get("POSTGRES_PASSWORD") || "postgres",
  database: Deno.env.get("POSTGRES_DB") || "pathway_test",
  tableName: "pathway_state_test",
};

describe("PostgresPathwayState", () => {
  let state: PostgresPathwayState;

  beforeAll(async () => {
    try {
      state = createPostgresPathwayState(config);
    } catch (error) {
      console.error("Failed to create PostgresPathwayState:", error);
      throw error;
    }
  });

  afterAll(async () => {
    if (state) {
      // Clean up the test table
      try {
        const adapter = (state as any).postgres;
        if (adapter) {
          await adapter.execute(`DROP TABLE IF EXISTS ${config.tableName}`);
          await state.close();
        }
      } catch (error) {
        console.error("Error cleaning up:", error);
      }
    }
  });

  it("should correctly report unprocessed events", async () => {
    const eventId = `test-event-${Date.now()}`;
    const isProcessed = await state.isProcessed(eventId);
    assertFalse(isProcessed);
  });

  it("should mark events as processed", async () => {
    const eventId = `test-event-${Date.now()}`;
    
    // Initially not processed
    let isProcessed = await state.isProcessed(eventId);
    assertFalse(isProcessed);
    
    // Mark as processed
    await state.setProcessed(eventId);
    
    // Now should be processed
    isProcessed = await state.isProcessed(eventId);
    assertEquals(isProcessed, true);
  });

  it("should handle expiration of processed events", async () => {
    const shortTtlState = createPostgresPathwayState({
      ...config,
      tableName: "pathway_state_short_ttl",
      ttlMs: 1000, // 1 second TTL
    });

    try {
      const eventId = `test-event-${Date.now()}`;
      
      // Mark as processed
      await shortTtlState.setProcessed(eventId);
      
      // Immediately should be processed
      let isProcessed = await shortTtlState.isProcessed(eventId);
      assertEquals(isProcessed, true);
      
      // Wait for TTL to expire
      await new Promise(resolve => setTimeout(resolve, 1500));
      
      // Now should not be processed anymore
      isProcessed = await shortTtlState.isProcessed(eventId);
      assertFalse(isProcessed);
    } finally {
      // Clean up
      const adapter = (shortTtlState as any).postgres;
      if (adapter) {
        await adapter.execute(`DROP TABLE IF EXISTS pathway_state_short_ttl`);
        await shortTtlState.close();
      }
    }
  });

  it("should handle missing connection information", async () => {
    const badConfig = {
      ...config,
      host: "nonexistent-host",
      port: 54321,
    };

    await assertRejects(
      async () => {
        const badState = createPostgresPathwayState(badConfig);
        await badState.isProcessed("some-event");
      },
      Error,
      "ENOTFOUND"
    );
  });
}); 