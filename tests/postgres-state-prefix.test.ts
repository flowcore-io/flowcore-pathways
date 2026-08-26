import { assertEquals } from "https://deno.land/std@0.224.0/assert/mod.ts"
import { PostgresJsAdapter } from "../src/pathways/postgres/index.ts"
import { createPostgresPathwayState } from "../src/pathways/postgres/postgres-pathway-state.ts"
import { PostgresPathwayCoordinator } from "../src/pathways/cluster/postgres-coordinator.ts"
import { createPostgresPumpStateManagerFactory } from "../src/pathways/pump/state.ts"

/**
 * Live-database cover for the production incident of 2026-08-26.
 *
 * `compute-api` and `compute-reconciler` are two deployables of one service that
 * share ONE connection string by design. Before the `statePrefix` option they
 * shared the single `pathway_leases` row keyed `pathway-cluster-leader`, so only
 * one of them ever ran a pump.
 */

const config = {
  host: Deno.env.get("POSTGRES_HOST") || "localhost",
  port: parseInt(Deno.env.get("POSTGRES_PORT") || "5432"),
  user: Deno.env.get("POSTGRES_USER") || "postgres",
  password: Deno.env.get("POSTGRES_PASSWORD") || "postgres",
  database: Deno.env.get("POSTGRES_DB") || "pathway_test",
}

const PREFIX_A = "sp_compute_api"
const PREFIX_B = "sp_compute_reconciler"

const CREATED_TABLES = [
  `${PREFIX_A}_pathway_leases`,
  `${PREFIX_A}_pathway_instances`,
  `${PREFIX_A}_pathway_pump_state`,
  `${PREFIX_A}_pathway_state`,
  `${PREFIX_B}_pathway_leases`,
  `${PREFIX_B}_pathway_instances`,
  "sp_default_pathway_leases",
  "sp_default_pathway_instances",
]

async function withAdapter<T>(fn: (adapter: PostgresJsAdapter) => Promise<T>): Promise<T> {
  const adapter = new PostgresJsAdapter(config)
  await adapter.connect()
  try {
    return await fn(adapter)
  } finally {
    await adapter.disconnect()
  }
}

async function tableExists(adapter: PostgresJsAdapter, table: string): Promise<boolean> {
  const result = await adapter.query<Array<{ exists: boolean }>>(
    `SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)`,
    [table],
  )
  return result[0]?.exists === true
}

async function dropCreatedTables(): Promise<void> {
  await withAdapter(async (adapter) => {
    for (const table of CREATED_TABLES) {
      await adapter.execute(`DROP TABLE IF EXISTS ${table}`)
    }
  })
}

Deno.test({
  name: "Postgres state prefix — two prefixes on one database do not contend",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: async (t) => {
    await dropCreatedTables()

    try {
      await t.step("each prefixed coordinator acquires its own leader lease", async () => {
        await withAdapter(async (adapter) => {
          const api = new PostgresPathwayCoordinator(adapter, { statePrefix: PREFIX_A })
          const reconciler = new PostgresPathwayCoordinator(adapter, { statePrefix: PREFIX_B })

          assertEquals(api.leaseKey, `${PREFIX_A}_pathway-cluster-leader`)
          assertEquals(reconciler.leaseKey, `${PREFIX_B}_pathway-cluster-leader`)

          const apiAcquired = await api.acquireLease("instance-api", api.leaseKey, 30_000)
          const reconcilerAcquired = await reconciler.acquireLease(
            "instance-reconciler",
            reconciler.leaseKey,
            30_000,
          )

          // Before this feature the second call returned false and that pump never started.
          assertEquals(apiAcquired, true)
          assertEquals(reconcilerAcquired, true)
        })
      })

      await t.step("each lease lands in its own table with its own key", async () => {
        await withAdapter(async (adapter) => {
          const apiRows = await adapter.query<Array<{ key: string; instance_id: string }>>(
            `SELECT key, instance_id FROM ${PREFIX_A}_pathway_leases`,
          )
          const reconcilerRows = await adapter.query<Array<{ key: string; instance_id: string }>>(
            `SELECT key, instance_id FROM ${PREFIX_B}_pathway_leases`,
          )

          assertEquals(apiRows.length, 1)
          assertEquals(apiRows[0].key, `${PREFIX_A}_pathway-cluster-leader`)
          assertEquals(apiRows[0].instance_id, "instance-api")

          assertEquals(reconcilerRows.length, 1)
          assertEquals(reconcilerRows[0].key, `${PREFIX_B}_pathway-cluster-leader`)
          assertEquals(reconcilerRows[0].instance_id, "instance-reconciler")
        })
      })

      await t.step("instance registries stay separate, so neither leader dials the other's pods", async () => {
        // This is the second production symptom: the reconciler leader logged
        // "Worker WS error" every 10s against a compute-api address, because
        // both deployables registered in ONE `pathway_instances` table.
        await withAdapter(async (adapter) => {
          const api = new PostgresPathwayCoordinator(adapter, { statePrefix: PREFIX_A })
          const reconciler = new PostgresPathwayCoordinator(adapter, { statePrefix: PREFIX_B })

          await api.register("instance-api", "ws://10.0.0.1:9090")
          await reconciler.register("instance-reconciler", "ws://10.0.0.2:9091")

          const apiInstances = await api.getInstances(60_000)
          const reconcilerInstances = await reconciler.getInstances(60_000)

          assertEquals(apiInstances.map((i) => i.address), ["ws://10.0.0.1:9090"])
          assertEquals(reconcilerInstances.map((i) => i.address), ["ws://10.0.0.2:9091"])
        })
      })

      await t.step("an unprefixed coordinator keeps the historical names and key", async () => {
        await withAdapter(async (adapter) => {
          // Explicit table names keep this step off the shared `pathway_leases`
          // table, which other tests and local databases may already own.
          const legacy = new PostgresPathwayCoordinator(adapter, {
            leasesTable: "sp_default_pathway_leases",
            instancesTable: "sp_default_pathway_instances",
          })

          assertEquals(legacy.leaseKey, "pathway-cluster-leader")

          const acquired = await legacy.acquireLease("instance-legacy", legacy.leaseKey, 30_000)
          assertEquals(acquired, true)

          const rows = await adapter.query<Array<{ key: string }>>(
            `SELECT key FROM sp_default_pathway_leases`,
          )
          assertEquals(rows.map((r) => r.key), ["pathway-cluster-leader"])
        })
      })

      await t.step("the pump state table is namespaced too", async () => {
        const factory = await createPostgresPumpStateManagerFactory({ ...config, statePrefix: PREFIX_A })
        const manager = factory("orders.0", "default")
        await manager.setState({ timeBucket: "20260826000000", eventId: "evt-1" })

        await withAdapter(async (adapter) => {
          assertEquals(await tableExists(adapter, `${PREFIX_A}_pathway_pump_state`), true)
          const rows = await adapter.query<Array<{ flow_type: string; event_id: string }>>(
            `SELECT flow_type, event_id FROM ${PREFIX_A}_pathway_pump_state`,
          )
          assertEquals(rows.length, 1)
          assertEquals(rows[0].flow_type, "orders.0")
          assertEquals(rows[0].event_id, "evt-1")
        })
      })

      await t.step("the pathway state table is namespaced too", async () => {
        const state = createPostgresPathwayState({ ...config, statePrefix: PREFIX_A })
        try {
          await state.setProcessed("evt-prefixed-1")
          assertEquals(await state.isProcessed("evt-prefixed-1"), true)

          await withAdapter(async (adapter) => {
            assertEquals(await tableExists(adapter, `${PREFIX_A}_pathway_state`), true)
            const rows = await adapter.query<Array<{ event_id: string }>>(
              `SELECT event_id FROM ${PREFIX_A}_pathway_state`,
            )
            assertEquals(rows.map((r) => r.event_id), ["evt-prefixed-1"])
          })
        } finally {
          await state.close()
        }
      })
    } finally {
      await dropCreatedTables()
    }
  },
})
