import { assert, assertEquals, assertFalse, assertRejects } from "https://deno.land/std@0.224.0/assert/mod.ts"
import { createPostgresPathwayState, PostgresPathwayState } from "../src/pathways/postgres/postgres-pathway-state.ts"

// Test configuration
const config = {
  host: Deno.env.get("POSTGRES_HOST") || "localhost",
  port: parseInt(Deno.env.get("POSTGRES_PORT") || "5432"),
  user: Deno.env.get("POSTGRES_USER") || "postgres",
  password: Deno.env.get("POSTGRES_PASSWORD") || "postgres",
  database: Deno.env.get("POSTGRES_DB") || "pathway_test",
  tableName: "pathway_state_test",
}

// Connection string for the same config
const connectionString = `postgres://${config.user}:${config.password}@${config.host}:${config.port}/${config.database}`

// Add ignore flag to avoid resource leak errors, but we still clean up properly
Deno.test({
  name: "PostgresPathwayState Tests",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: async (t) => {
    let state: PostgresPathwayState

    // Setup - similar to beforeAll
    try {
      state = createPostgresPathwayState(config)
    } catch (error) {
      console.error("Failed to create PostgresPathwayState:", error)
      throw error
    }

    await t.step("should correctly report unprocessed events", async () => {
      const eventId = `test-event-${Date.now()}`
      const isProcessed = await state.isProcessed(eventId)
      assertFalse(isProcessed)
    })

    await t.step("should mark events as processed", async () => {
      const eventId = `test-event-${Date.now()}`

      // Initially not processed
      let isProcessed = await state.isProcessed(eventId)
      assertFalse(isProcessed)

      // Mark as processed
      await state.setProcessed(eventId)

      // Now should be processed
      isProcessed = await state.isProcessed(eventId)
      assertEquals(isProcessed, true)
    })

    await t.step("should handle expiration of processed events", async () => {
      const shortTtlState = createPostgresPathwayState({
        ...config,
        tableName: "pathway_state_short_ttl",
        ttlMs: 1000, // 1 second TTL
      })

      try {
        const eventId = `test-event-${Date.now()}`

        // Mark as processed
        await shortTtlState.setProcessed(eventId)

        // Immediately should be processed
        let isProcessed = await shortTtlState.isProcessed(eventId)
        assertEquals(isProcessed, true)

        // Wait for TTL to expire
        await new Promise((resolve) => setTimeout(resolve, 1500))

        // Now should not be processed anymore
        isProcessed = await shortTtlState.isProcessed(eventId)
        assertFalse(isProcessed)
      } finally {
        // Clean up
        const adapter = (shortTtlState as any).postgres
        if (adapter) {
          await adapter.execute(`DROP TABLE IF EXISTS pathway_state_short_ttl`)
          await shortTtlState.close()
        }
      }
    })

    await t.step("should sweep expired rows at most once per cleanup interval", async () => {
      const throttledState = createPostgresPathwayState({
        ...config,
        tableName: "pathway_state_throttled",
        cleanupIntervalMs: 60_000,
      })

      try {
        // First call initializes and performs the first sweep.
        await throttledState.isProcessed("warm-up")

        const adapter = (throttledState as any).postgres
        const originalExecute = adapter.execute.bind(adapter)
        let deletes = 0
        adapter.execute = async (sql: string, params?: unknown[]) => {
          if (/DELETE FROM/i.test(sql)) deletes++
          return await originalExecute(sql, params)
        }

        for (let i = 0; i < 50; i++) {
          await throttledState.isProcessed(`throttled-${i}`)
        }
        assertEquals(deletes, 0)
      } finally {
        const adapter = (throttledState as any).postgres
        if (adapter) {
          await adapter.execute(`DROP TABLE IF EXISTS pathway_state_throttled`)
          await throttledState.close()
        }
      }
    })

    await t.step("cleanupIntervalMs of 0 sweeps on every lookup", async () => {
      const eagerState = createPostgresPathwayState({
        ...config,
        tableName: "pathway_state_eager",
        cleanupIntervalMs: 0,
      })

      try {
        await eagerState.isProcessed("warm-up")

        const adapter = (eagerState as any).postgres
        const originalExecute = adapter.execute.bind(adapter)
        let deletes = 0
        adapter.execute = async (sql: string, params?: unknown[]) => {
          if (/DELETE FROM/i.test(sql)) deletes++
          return await originalExecute(sql, params)
        }

        for (let i = 0; i < 5; i++) {
          await eagerState.isProcessed(`eager-${i}`)
        }
        assertEquals(deletes, 5)
      } finally {
        const adapter = (eagerState as any).postgres
        if (adapter) {
          await adapter.execute(`DROP TABLE IF EXISTS pathway_state_eager`)
          await eagerState.close()
        }
      }
    })

    await t.step("expired rows are removed once the cleanup interval elapses", async () => {
      const sweepState = createPostgresPathwayState({
        ...config,
        tableName: "pathway_state_sweep",
        ttlMs: 1000,
        cleanupIntervalMs: 1500,
      })

      try {
        await sweepState.setProcessed("expires-soon")
        // The first lookup after start sweeps; the row is still live so it survives.
        assertEquals(await sweepState.isProcessed("expires-soon"), true)

        await new Promise((resolve) => setTimeout(resolve, 1600))

        // Expired and past the interval: the lookup reports false and the sweep deletes the row.
        assertFalse(await sweepState.isProcessed("expires-soon"))
        const adapter = (sweepState as any).postgres
        const rows = await adapter.query(`SELECT event_id FROM pathway_state_sweep WHERE event_id = $1`, [
          "expires-soon",
        ])
        assertEquals(rows.length, 0)
      } finally {
        const adapter = (sweepState as any).postgres
        if (adapter) {
          await adapter.execute(`DROP TABLE IF EXISTS pathway_state_sweep`)
          await sweepState.close()
        }
      }
    })

    await t.step("should work with connection string configuration", async () => {
      const connectionStringState = createPostgresPathwayState({
        connectionString,
        tableName: "pathway_state_conn_str",
      })

      try {
        const eventId = `test-event-connection-string-${Date.now()}`

        // Initially not processed
        let isProcessed = await connectionStringState.isProcessed(eventId)
        assertFalse(isProcessed)

        // Mark as processed
        await connectionStringState.setProcessed(eventId)

        // Now should be processed
        isProcessed = await connectionStringState.isProcessed(eventId)
        assertEquals(isProcessed, true)
      } finally {
        // Clean up
        const adapter = (connectionStringState as any).postgres
        if (adapter) {
          await adapter.execute(`DROP TABLE IF EXISTS pathway_state_conn_str`)
          await connectionStringState.close()
        }
      }
    })

    await t.step("should handle missing connection information", async () => {
      const badConfig = {
        ...config,
        host: "nonexistent-host",
        port: 54321,
      }

      const error = await assertRejects(
        async () => {
          const badState = createPostgresPathwayState(badConfig)
          await badState.isProcessed("some-event")
        },
        Error,
      )
      // An unresolvable host is reported differently across resolvers/runners
      // (ENOTFOUND vs EAI_AGAIN); both mean the host did not resolve.
      assert(
        /ENOTFOUND|EAI_AGAIN/.test(error.message),
        `expected a DNS resolution failure (ENOTFOUND/EAI_AGAIN), got: ${error.message}`,
      )
    })

    // Cleanup - similar to afterAll
    await t.step("cleanup", async () => {
      if (state) {
        try {
          const adapter = (state as any).postgres
          if (adapter) {
            await adapter.execute(`DROP TABLE IF EXISTS ${config.tableName}`)
            await state.close()
          }
        } catch (error) {
          console.error("Error cleaning up:", error)
        }
      }
    })
  },
})
