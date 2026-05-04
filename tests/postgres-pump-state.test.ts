import { assertEquals, assertExists } from "https://deno.land/std@0.224.0/assert/mod.ts"
import { PostgresJsAdapter } from "../src/pathways/postgres/index.ts"
import { createPostgresPumpStateManagerFactory } from "../src/pathways/pump/state.ts"

const config = {
  host: Deno.env.get("POSTGRES_HOST") || "localhost",
  port: parseInt(Deno.env.get("POSTGRES_PORT") || "5432"),
  user: Deno.env.get("POSTGRES_USER") || "postgres",
  password: Deno.env.get("POSTGRES_PASSWORD") || "postgres",
  database: Deno.env.get("POSTGRES_DB") || "pathway_test",
}

const TABLE_NEW = "pump_state_new_test"
const TABLE_MIGRATE = "pump_state_migrate_test"

Deno.test({
  name: "PostgresPumpStateManager — composite-PK schema + migration",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: async (t) => {
    await t.step("greenfield schema accepts composite (flow_type, pump_group)", async () => {
      const factory = await createPostgresPumpStateManagerFactory({ ...config, tableName: TABLE_NEW })
      const adapter = new PostgresJsAdapter(config)
      await adapter.connect()
      try {
        const ordersHot = factory("orders.0", "hot")
        const ordersDefault = factory("orders.0", "default")

        await ordersHot.setState({ timeBucket: "20260101000000", eventId: "evt-hot-1" })
        await ordersDefault.setState({ timeBucket: "20260101000010", eventId: "evt-def-1" })

        const hotState = await ordersHot.getState()
        const defaultState = await ordersDefault.getState()
        assertExists(hotState)
        assertExists(defaultState)
        assertEquals(hotState!.eventId, "evt-hot-1")
        assertEquals(defaultState!.eventId, "evt-def-1")

        // Independent updates do not bleed across groups.
        await ordersHot.setState({ timeBucket: "20260101000100", eventId: "evt-hot-2" })
        const refreshedDefault = await ordersDefault.getState()
        assertEquals(refreshedDefault!.eventId, "evt-def-1")
      } finally {
        await adapter.execute(`DROP TABLE IF EXISTS ${TABLE_NEW}`)
        await adapter.disconnect()
      }
    })

    await t.step(
      "migrates a pre-existing single-PK table without losing rows; existing rows land under pump_group='default'",
      async () => {
        const adapter = new PostgresJsAdapter(config)
        await adapter.connect()
        try {
          // Simulate a pre-2.4 schema with single-column PK.
          await adapter.execute(`DROP TABLE IF EXISTS ${TABLE_MIGRATE}`)
          await adapter.execute(`
            CREATE TABLE ${TABLE_MIGRATE} (
              flow_type TEXT PRIMARY KEY,
              time_bucket TEXT NOT NULL,
              event_id TEXT
            )
          `)
          await adapter.execute(
            `INSERT INTO ${TABLE_MIGRATE} (flow_type, time_bucket, event_id)
             VALUES ($1, $2, $3)`,
            ["legacy.0", "20251231000000", "evt-legacy-1"],
          )

          const factory = await createPostgresPumpStateManagerFactory({
            ...config,
            tableName: TABLE_MIGRATE,
          })

          // First call triggers the idempotent migration.
          const legacyDefault = factory("legacy.0", "default")
          const state = await legacyDefault.getState()

          assertExists(state, "row inserted under pre-migration schema must be preserved as 'default' group")
          assertEquals(state!.eventId, "evt-legacy-1")
          assertEquals(state!.timeBucket, "20251231000000")

          // New pump group on same flow type lives in its own row, not colliding.
          const legacyHot = factory("legacy.0", "hot")
          const hotStateBefore = await legacyHot.getState()
          assertEquals(hotStateBefore, null)
          await legacyHot.setState({ timeBucket: "20260601000000", eventId: "evt-hot-1" })
          const hotStateAfter = await legacyHot.getState()
          assertExists(hotStateAfter)
          assertEquals(hotStateAfter!.eventId, "evt-hot-1")

          // Default group untouched.
          const refreshedDefault = await legacyDefault.getState()
          assertEquals(refreshedDefault!.eventId, "evt-legacy-1")

          // Verify the table is now composite-PK by inspecting information_schema.
          const pkCols = await adapter.query<Array<{ count: string }>>(
            `SELECT count(*)::text AS count
               FROM information_schema.table_constraints tc
               JOIN information_schema.key_column_usage kcu
                 ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
              WHERE tc.table_name = $1
                AND tc.constraint_type = 'PRIMARY KEY'`,
            [TABLE_MIGRATE],
          )
          assertEquals(pkCols[0].count, "2", "primary key must now span (flow_type, pump_group)")
        } finally {
          await adapter.execute(`DROP TABLE IF EXISTS ${TABLE_MIGRATE}`)
          await adapter.disconnect()
        }
      },
    )
  },
})
