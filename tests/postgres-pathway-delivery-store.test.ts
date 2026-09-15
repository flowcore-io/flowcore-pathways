import { assertEquals } from "https://deno.land/std@0.224.0/assert/mod.ts"
import {
  createPostgresPathwayDeliveryStore,
  PostgresPathwayDeliveryStore,
} from "../src/pathways/postgres/postgres-pathway-delivery-store.ts"

const config = {
  host: Deno.env.get("POSTGRES_HOST") || "localhost",
  port: parseInt(Deno.env.get("POSTGRES_PORT") || "5432"),
  user: Deno.env.get("POSTGRES_USER") || "postgres",
  password: Deno.env.get("POSTGRES_PASSWORD") || "postgres",
  database: Deno.env.get("POSTGRES_DB") || "pathway_test",
  tableName: "pathway_delivery_state_test",
}

Deno.test({
  name: "PostgresPathwayDeliveryStore Tests",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: async (t) => {
    const store: PostgresPathwayDeliveryStore = createPostgresPathwayDeliveryStore(config)

    await t.step("an unknown pathway has no pause", async () => {
      assertEquals(await store.getPausedPumps("unknown-pathway"), [])
    })

    await t.step("a pause survives a new store instance, as it must across a restart", async () => {
      await store.setPausedPumps("svc-a", ["orders.0::hot", "orders.0::default"])

      // A different instance stands in for the process that comes back after a redeploy
      // or the node that wins the next leader election.
      const other = createPostgresPathwayDeliveryStore(config)
      assertEquals((await other.getPausedPumps("svc-a")).sort(), [
        "orders.0::default",
        "orders.0::hot",
      ])
      await other.close()
    })

    await t.step("pathways are isolated from each other", async () => {
      await store.setPausedPumps("svc-b", ["invoices.0::default"])
      assertEquals(await store.getPausedPumps("svc-b"), ["invoices.0::default"])
      assertEquals((await store.getPausedPumps("svc-a")).length, 2)
    })

    await t.step("writing the set again replaces it", async () => {
      await store.setPausedPumps("svc-a", ["orders.0::hot"])
      assertEquals(await store.getPausedPumps("svc-a"), ["orders.0::hot"])
    })

    await t.step("an empty set clears the pause", async () => {
      await store.setPausedPumps("svc-a", [])
      assertEquals(await store.getPausedPumps("svc-a"), [])
    })

    await t.step("the state prefix namespaces the table", () => {
      const prefixed = createPostgresPathwayDeliveryStore({
        ...config,
        tableName: undefined,
        statePrefix: "compute_api",
      })
      assertEquals(prefixed.table, "compute_api_pathway_delivery_state")
    })

    await store.setPausedPumps("svc-b", [])
    await store.close()
  },
})
