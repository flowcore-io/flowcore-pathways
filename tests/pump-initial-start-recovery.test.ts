import { assertEquals } from "https://deno.land/std@0.224.0/assert/mod.ts"
import { FakeTime } from "https://deno.land/std@0.224.0/testing/time.ts"
import { FlowcoreDataPump } from "@flowcore/data-pump"
import { PathwayPump } from "../src/pathways/pump/pathway-pump.ts"

async function flushStartupPromises() {
  for (let step = 0; step < 20; step++) await Promise.resolve()
}

Deno.test({
  name: "initial source startup failure stops the half-started pump, starts peers, and retries to recovery",
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    const time = new FakeTime()
    const original = FlowcoreDataPump.create
    const created: Array<{ flow: string; stopped: boolean }> = []
    let failures = 0
    FlowcoreDataPump.create = ((options: { dataSource: { flowType: string } }) => {
      const record = { flow: options.dataSource.flowType, stopped: false }
      created.push(record)
      return {
        async start() {
          if (record.flow === "first.0" && failures++ < 2) throw new Error("DataCoreFetchCommand: transient 502")
        },
        async stop() {
          record.stopped = true
        },
      }
    }) as unknown as typeof FlowcoreDataPump.create
    const pump = new PathwayPump({ stateManagerFactory: () => ({ getState: () => null, setState: () => {} }) })
    pump.configure({
      tenant: "synthetic",
      dataCore: "synthetic",
      apiKey: "fc_synthetic_synthetic",
      baseUrl: "https://invalid.example",
      processEvent: async () => {},
    })
    try {
      await pump.start([{ flowType: "first.0", eventType: "first.created.0" }, {
        flowType: "second.0",
        eventType: "second.created.0",
      }])
      assertEquals(created.map((item) => item.flow), ["first.0", "second.0"])
      assertEquals(created[0].stopped, true)
      await time.tickAsync(1_001)
      await flushStartupPromises()
      assertEquals(created.length, 3)
      assertEquals(created[2].stopped, true)
      await time.tickAsync(2_001)
      await flushStartupPromises()
      assertEquals(created.length, 4)
      assertEquals(created[3].stopped, false)
      await pump.stop()
      await time.tickAsync(60_000)
      assertEquals(created.length, 4)
      assertEquals(created.every((item) => item.stopped), true)
    } finally {
      await pump.stop()
      FlowcoreDataPump.create = original
      time.restore()
    }
  },
})

Deno.test({
  name: "stop cancels the initial-start retry before it creates another source pump",
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    const time = new FakeTime()
    const original = FlowcoreDataPump.create
    let starts = 0
    FlowcoreDataPump.create = (() => ({
      async start() {
        starts++
        throw new Error("temporary 401")
      },
      async stop() {},
    })) as unknown as typeof FlowcoreDataPump.create
    const pump = new PathwayPump({ stateManagerFactory: () => ({ getState: () => null, setState: () => {} }) })
    pump.configure({
      tenant: "synthetic",
      dataCore: "synthetic",
      apiKey: "fc_synthetic_synthetic",
      baseUrl: "https://invalid.example",
      processEvent: async () => {},
    })
    try {
      await pump.start([{ flowType: "first.0", eventType: "first.created.0" }])
      await pump.stop()
      await time.tickAsync(60_000)
      assertEquals(starts, 1)
    } finally {
      await pump.stop()
      FlowcoreDataPump.create = original
      time.restore()
    }
  },
})
