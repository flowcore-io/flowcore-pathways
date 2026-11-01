import { afterEach, describe, expect, spyOn, test } from "bun:test"
import { resolve } from "node:path"
import { z } from "../npm/node_modules/zod/index.js"

// Exercise both published runtime paths, not a reimplementation of the adapter.
const root = resolve(import.meta.dir, "../npm")
const stores: { close(): void }[] = []
let clock: ReturnType<typeof spyOn> | undefined
afterEach(() => {
  clock?.mockRestore()
  clock = undefined
  for (const store of stores.splice(0)) store.close()
})

for (const format of ["esm", "script"]) {
  describe(`Pathways ${format} Bun completion expiry`, () => {
    async function state() {
      const { InternalPathwayState } = await import(`${root}/${format}/pathways/internal-pathway.state.js`)
      const result = new InternalPathwayState()
      await result.isProcessed("initial-probe")
      expect(result.kv.constructor.name).toBe("BunKvAdapter")
      stores.push(result.kv.store)
      return result
    }

    test("physically reclaims unique expired IDs and reuses SQLite pages", async () => {
      let now = 1_000_000
      clock = spyOn(Date, "now").mockImplementation(() => now)
      const s = await state()
      const pages: number[] = []
      const rows: number[] = []
      for (let cycle = 0; cycle < 12; cycle++) {
        for (let i = 0; i < 5000; i++) {
          await s.setProcessed(`event-${String(cycle).padStart(2, "0")}-${String(i).padStart(5, "0")}`)
        }
        rows.push(s.kv.store.getCount())
        pages.push(s.kv.store.db.query("PRAGMA page_count").get().page_count)
        now += 360_001
      }
      console.log(JSON.stringify({ format, rows, pages, rss: process.memoryUsage().rss }))
      expect(Math.max(...rows)).toBe(5000)
      expect(Math.max(...pages.slice(3)) - Math.min(...pages.slice(3))).toBeLessThanOrEqual(3)
      await s.isProcessed("never-seen")
      expect(s.kv.store.getCount()).toBe(0)
      expect(s.kv.store.db.query("PRAGMA freelist_count").get().freelist_count).toBeGreaterThan(0)
    })

    test("bounds a continuous stream across overlapping TTL windows without early eviction", async () => {
      let now = 1_000_000
      clock = spyOn(Date, "now").mockImplementation(() => now)
      const s = await state()
      let highWater = 0
      for (let second = 0; second < 1800; second++) {
        now = 1_000_000 + second * 1000
        for (let i = 0; i < 10; i++) await s.setProcessed(`stream-${second}-${i}`)
        if (second >= 300) expect(await s.isProcessed(`stream-${second - 300}-0`)).toBe(true)
        if (second > 300) expect(await s.isProcessed(`stream-${second - 301}-0`)).toBe(false)
        highWater = Math.max(highWater, s.kv.store.getCount())
      }
      expect(highWater).toBeLessThanOrEqual(3610)
      console.log(JSON.stringify({ format, continuousStreamHighWater: highWater }))
    })

    test("retries a rejected initialization and handles clock rollback", async () => {
      let now = 1_000_000
      clock = spyOn(Date, "now").mockImplementation(() => now)
      const { InternalPathwayState } = await import(`${root}/${format}/pathways/internal-pathway.state.js`)
      const s = new InternalPathwayState()
      s.kvPromise = Promise.reject(new Error("synthetic initialization failure"))
      const rejected = await Promise.allSettled(Array.from({ length: 32 }, () => s.getKv()))
      expect(rejected.every((result) => result.status === "rejected")).toBe(true)
      expect(s.kvPromise).toBeUndefined()
      const recovered = await Promise.all(Array.from({ length: 32 }, () => s.getKv()))
      expect(new Set(recovered).size).toBe(1)
      await s.setProcessed("retained")
      stores.push(s.kv.store)
      const purge = spyOn(s.kv.store, "deleteExpired")
      try {
        now -= 1000
        expect(await s.isProcessed("retained")).toBe(true)
        expect(purge).toHaveBeenCalledTimes(1)
      } finally {
        purge.mockRestore()
      }
    })

    test("creates no background timer requiring shutdown", async () => {
      const interval = spyOn(globalThis, "setInterval")
      const timeout = spyOn(globalThis, "setTimeout")
      try {
        const s = await state()
        await s.setProcessed("local-only")
        expect(interval).not.toHaveBeenCalled()
        expect(timeout).not.toHaveBeenCalled()
      } finally {
        interval.mockRestore()
        timeout.mockRestore()
      }
    })

    test("keeps the existing strict expiry boundary and full five-minute TTL", async () => {
      let now = 1_000_000
      clock = spyOn(Date, "now").mockImplementation(() => now)
      const s = await state()
      await s.setProcessed("duplicate")
      s.kv.set("permanent", true)
      now += 299_999
      expect(await s.isProcessed("duplicate")).toBe(true)
      now++
      expect(await s.isProcessed("duplicate")).toBe(true)
      now++
      expect(await s.isProcessed("duplicate")).toBe(false)
      expect(s.kv.get("permanent")).toBe(true)
      await s.setProcessed("duplicate")
      expect(await s.isProcessed("duplicate")).toBe(true)
    })

    test("throttles scans and reclaims on first access after idle without timers", async () => {
      let now = 1_000_000
      clock = spyOn(Date, "now").mockImplementation(() => now)
      const s = await state()
      const purge = spyOn(s.kv.store, "deleteExpired")
      try {
        for (let i = 0; i < 100; i++) await s.setProcessed(`id-${i}`)
        expect(purge).toHaveBeenCalledTimes(0)
        now += 60_000
        await s.isProcessed("unknown")
        expect(purge).toHaveBeenCalledTimes(1)
        now += 600_000
        expect(s.kv.store.getCount()).toBe(100)
        await s.setProcessed("new")
        expect(s.kv.store.getCount()).toBe(1)
        expect(purge).toHaveBeenCalledTimes(2)
      } finally {
        purge.mockRestore()
      }
    })

    test("concurrent initialization shares one adapter and loses no completions", async () => {
      const { InternalPathwayState } = await import(`${root}/${format}/pathways/internal-pathway.state.js`)
      const s = new InternalPathwayState()
      const adapters = await Promise.all(Array.from({ length: 32 }, () => s.getKv()))
      for (const adapter of new Set(adapters)) stores.push(adapter.store)
      expect(new Set(adapters).size).toBe(1)
      await Promise.all(Array.from({ length: 32 }, (_, i) => s.setProcessed(`parallel-${i}`)))
      for (let i = 0; i < 32; i++) expect(await s.isProcessed(`parallel-${i}`)).toBe(true)
    })

    test("cleanup failure preserves reads and writes, throttles failures and later recovers", async () => {
      let now = 1_000_000
      clock = spyOn(Date, "now").mockImplementation(() => now)
      const s = await state()
      await s.setProcessed("old")
      await s.setProcessed("untouched-expired")
      now += 360_001
      const purge = spyOn(s.kv.store, "deleteExpired").mockImplementationOnce(() => {
        throw new Error("synthetic cleanup failure")
      })
      try {
        await s.setProcessed("new")
        expect(s.kv.store.getCount()).toBe(3)
        expect(await s.isProcessed("new")).toBe(true)
        expect(await s.isProcessed("old")).toBe(false)
        for (let i = 0; i < 100; i++) await s.setProcessed(`during-failure-${i}`)
        expect(purge).toHaveBeenCalledTimes(1)
        now += 60_000
        expect(await s.isProcessed("new")).toBe(true)
        expect(s.kv.store.getCount()).toBe(101)
        expect(purge).toHaveBeenCalledTimes(2)
      } finally {
        purge.mockRestore()
      }
    })

    for (const handlerFails of [false, true]) {
      test(`real builder preserves handler retry behavior during persistent cleanup failure (handlerFails=${handlerFails})`, async () => {
        let now = 1_000_000
        clock = spyOn(Date, "now").mockImplementation(() => now)
        const s = await state()
        // The published CJS builder cannot load its ESM-only file-type dependency.
        // Exercise the application's ESM builder with each real state/adapter format.
        const { PathwaysBuilder } = await import(`${root}/esm/pathways/builder.js`)
        const builder = new PathwaysBuilder({
          tenant: "synthetic",
          dataCore: "synthetic",
          apiKey: "fc_synthetic_only",
          baseUrl: "https://example.invalid",
          runtimeEnv: "test",
          pathwayMode: "virtual",
          autoProvision: false,
        }).withPathwayState(s)
        builder.register({
          flowType: "test.0",
          eventType: "event.0",
          schema: z.object({}),
          writable: false,
          subscribe: false,
          maxRetries: 1,
          retryDelayMs: 0,
        })
        let deliveries = 0
        const handlerError = new Error("synthetic handler failure")
        builder.handle("test.0/event.0", async () => {
          await Promise.resolve()
          deliveries++
          // Cleanup becomes due after delivery, exactly where completion is written.
          now += 60_000
          if (handlerFails) throw handlerError
        })
        const purge = spyOn(s.kv.store, "deleteExpired").mockImplementation(() => {
          throw new Error("synthetic cleanup failure")
        })
        try {
          const pending = builder.process("test.0/event.0", { eventId: "delivery", payload: {}, metadata: {} })
          if (handlerFails) await expect(pending).rejects.toBe(handlerError)
          else await pending
          expect(deliveries).toBe(handlerFails ? 2 : 1)
          // Current main retains terminal failures for redelivery; only success is marked.
          expect(await s.isProcessed("delivery")).toBe(!handlerFails)
          expect(purge).toHaveBeenCalledTimes(1)
          now += 60_000
          expect(await s.isProcessed("delivery")).toBe(!handlerFails)
          expect(purge).toHaveBeenCalledTimes(2)
          if (handlerFails) {
            await expect(builder.process("test.0/event.0", {
              eventId: "delivery",
              payload: {},
              metadata: {},
            })).rejects.toBe(handlerError)
            expect(deliveries).toBe(4)
            expect(await s.isProcessed("delivery")).toBe(false)
          }
        } finally {
          purge.mockRestore()
        }
      })
    }

    test("real KV read and completion write failures still reject", async () => {
      let now = 1_000_000
      clock = spyOn(Date, "now").mockImplementation(() => now)
      const s = await state()
      now += 60_000
      const purge = spyOn(s.kv.store, "deleteExpired").mockImplementation(() => {
        throw new Error("synthetic cleanup failure")
      })
      const write = spyOn(s.kv.store, "set").mockImplementation(() => {
        throw new Error("synthetic write failure")
      })
      const read = spyOn(s.kv.store, "get").mockImplementation(() => {
        throw new Error("synthetic read failure")
      })
      try {
        await expect(s.setProcessed("not-completed")).rejects.toThrow("synthetic write failure")
        await expect(s.isProcessed("not-completed")).rejects.toThrow("synthetic read failure")
      } finally {
        purge.mockRestore()
        write.mockRestore()
        read.mockRestore()
      }
      expect(await s.isProcessed("not-completed")).toBe(false)
    })
  })
}
