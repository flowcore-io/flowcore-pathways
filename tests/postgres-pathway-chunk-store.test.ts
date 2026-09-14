import { assertEquals, assertRejects } from "https://deno.land/std@0.224.0/assert/mod.ts"
import {
  createPostgresPathwayChunkStore,
  PostgresPathwayChunkStore,
} from "../src/pathways/postgres/postgres-pathway-chunk-store.ts"

const config = {
  host: Deno.env.get("POSTGRES_HOST") || "localhost",
  port: parseInt(Deno.env.get("POSTGRES_PORT") || "5432"),
  user: Deno.env.get("POSTGRES_USER") || "postgres",
  password: Deno.env.get("POSTGRES_PASSWORD") || "postgres",
  database: Deno.env.get("POSTGRES_DB") || "pathway_test",
  tableName: "pathway_chunks_test",
}

Deno.test({
  name: "PostgresPathwayChunkStore Tests",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: async (t) => {
    const store: PostgresPathwayChunkStore = createPostgresPathwayChunkStore(config)

    await t.step("stores parts, reports duplicates and completes exactly once", async () => {
      const base = { chunkId: crypto.randomUUID(), totalParts: 3, digest: "digest" }

      assertEquals((await store.storePart({ ...base, part: 3, data: "c", eventId: "e3" })).status, "stored")
      assertEquals((await store.storePart({ ...base, part: 3, data: "c", eventId: "e3" })).status, "duplicate")
      assertEquals((await store.storePart({ ...base, part: 1, data: "a", eventId: "e1" })).status, "stored")

      const complete = await store.storePart({ ...base, part: 2, data: "b", eventId: "e2" })
      assertEquals(complete.status, "complete")
      assertEquals(complete.parts, ["a", "b", "c"])
      assertEquals(complete.partEventIds, ["e1", "e2", "e3"])

      // Replays after assembly never complete a second time.
      assertEquals((await store.storePart({ ...base, part: 2, data: "b", eventId: "e2" })).status, "duplicate")

      await store.deleteChunk(base.chunkId)
      assertEquals((await store.storePart({ ...base, part: 1, data: "a", eventId: "e1" })).status, "stored")
      await store.deleteChunk(base.chunkId)
    })

    await t.step("rejects a conflicting part", async () => {
      const base = { chunkId: crypto.randomUUID(), totalParts: 2, digest: "digest" }
      await store.storePart({ ...base, part: 1, data: "a", eventId: "e1" })
      await assertRejects(
        () => store.storePart({ ...base, part: 1, data: "different", eventId: "e1" }),
        Error,
        "Conflicting pathway chunk part",
      )
      await store.deleteChunk(base.chunkId)
    })

    await t.step("rejects a part whose digest or part count disagrees with stored parts", async () => {
      const base = { chunkId: crypto.randomUUID(), totalParts: 2, digest: "digest" }
      await store.storePart({ ...base, part: 1, data: "a", eventId: "e1" })
      await assertRejects(
        () => store.storePart({ ...base, part: 2, data: "b", eventId: "e2", totalParts: 3 }),
        Error,
        "Inconsistent pathway chunk part",
      )
      await assertRejects(
        () => store.storePart({ ...base, part: 2, data: "b", eventId: "e2", digest: "other" }),
        Error,
        "Inconsistent pathway chunk part",
      )
      await store.deleteChunk(base.chunkId)
    })

    await t.step("concurrent writers observe exactly one completion", async () => {
      const total = 8
      const base = { chunkId: crypto.randomUUID(), totalParts: total, digest: "digest" }
      const results = await Promise.all(
        Array.from(
          { length: total },
          (_, index) => store.storePart({ ...base, part: index + 1, data: `d${index + 1}`, eventId: `e${index + 1}` }),
        ),
      )
      const completions = results.filter((result) => result.status === "complete")
      assertEquals(completions.length, 1)
      assertEquals(completions[0].parts, Array.from({ length: total }, (_, index) => `d${index + 1}`))
      await store.deleteChunk(base.chunkId)
    })

    await t.step("expired parts are removed on the next write", async () => {
      const shortLived = createPostgresPathwayChunkStore({ ...config, ttlMs: 1000 })
      const base = { chunkId: crypto.randomUUID(), totalParts: 2, digest: "digest" }
      await shortLived.storePart({ ...base, part: 1, data: "a", eventId: "e1" })
      await new Promise((resolve) => setTimeout(resolve, 1500))
      // Part 1 expired, so adding part 2 does not complete the chunk.
      assertEquals((await shortLived.storePart({ ...base, part: 2, data: "b", eventId: "e2" })).status, "stored")
      await shortLived.deleteChunk(base.chunkId)
      await shortLived.close()
    })

    await t.step("statePrefix namespaces the table", () => {
      const prefixed = createPostgresPathwayChunkStore({ ...config, tableName: undefined, statePrefix: "svc" })
      assertEquals(prefixed.table, "svc_pathway_chunks")
      const plain = createPostgresPathwayChunkStore({ ...config, tableName: undefined })
      assertEquals(plain.table, "pathway_chunks")
    })

    await store.close()
  },
})
