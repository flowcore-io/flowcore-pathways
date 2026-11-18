import { FlowcoreDataSource, type FlowcoreDataSourceOptions } from "@flowcore/data-pump"
import { assertEquals, assertRejects, assertStrictEquals } from "https://deno.land/std@0.224.0/assert/mod.ts"
import { FakeTime } from "https://deno.land/std@0.224.0/testing/time.ts"
import { PathwayPump } from "../src/pathways/pump/pathway-pump.ts"
import type { PathwayPumpOptions } from "../src/pathways/pump/types.ts"

const meta = { flowType: "facility.0", pumpGroup: "default", eventTypes: ["created.0"] }
const sourceOptions = {
  auth: { apiKey: "fc_fixture_secret" },
  dataSource: { tenant: "tenant", dataCore: "core", flowType: "facility.0", eventTypes: ["created.0"] },
  baseUrlOverride: "http://127.0.0.1:1",
}
const fixture = (dataSourceFactory?: PathwayPumpOptions["dataSourceFactory"]) => {
  const pump = new PathwayPump({
    stateManagerFactory: (_flow, _group) => ({ getState: () => ({ timeBucket: "20261117230000" }), setState() {} }),
    notifier: { type: "poller", pollerIntervalMs: 1000 },
    dataSourceFactory,
  })
  pump.configure({
    tenant: "tenant",
    dataCore: "core",
    apiKey: "fc_fixture_secret",
    baseUrl: "http://127.0.0.1:1",
    processEvent: async () => {},
  })
  const calls: Array<{ source?: FlowcoreDataSource; options: Record<string, unknown> }> = []
  const callbacks: Array<(error?: Error) => void> = []
  let stopped = 0
  const internal = pump as unknown as {
    running: boolean
    dataPumpConstructor: { create(options: Record<string, unknown>, source?: FlowcoreDataSource): unknown }
    startPumpForGroup(group: typeof meta): Promise<void>
  }
  internal.dataPumpConstructor = {
    create(options, source) {
      calls.push({ options, source })
      return {
        start: (callback: (error?: Error) => void) => {
          callbacks.push(callback)
        },
        stop: () => {
          stopped++
        },
      }
    },
  }
  return { pump, internal, calls, callbacks, stopped: () => stopped }
}

Deno.test("omitted source factory preserves default data-pump source and options", async () => {
  const f = fixture()
  await f.internal.startPumpForGroup(meta)
  assertEquals(f.calls.length, 1)
  assertStrictEquals(f.calls[0].source, undefined)
  assertEquals(f.calls[0].options.auth, sourceOptions.auth)
  assertEquals(f.calls[0].options.dataSource, sourceOptions.dataSource)
  assertEquals(f.calls[0].options.baseUrlOverride, sourceOptions.baseUrlOverride)
  await f.pump.stop()
})

Deno.test("source factory receives exact group context and creates a fresh source on scheduled restart", async () => {
  const time = new FakeTime()
  const options: FlowcoreDataSourceOptions[] = []
  const sources: FlowcoreDataSource[] = []
  const f = fixture((input) => {
    options.push(input)
    const source = new FlowcoreDataSource(input)
    sources.push(source)
    return source
  })
  f.internal.running = true
  try {
    await f.internal.startPumpForGroup(meta)
    assertEquals(options, [sourceOptions])
    assertStrictEquals(f.calls[0].source, sources[0])
    f.callbacks[0](new Error("synthetic source failure"))
    await time.tickAsync(1000)
    assertEquals(f.calls.length, 2)
    assertEquals(options, [sourceOptions, sourceOptions])
    assertStrictEquals(f.calls[1].source, sources[1])
    assertEquals(sources[0] === sources[1], false)
    assertEquals(f.stopped(), 1)
    await f.internal.startPumpForGroup({ flowType: "other.0", pumpGroup: "audit", eventTypes: ["updated.0"] })
    assertEquals(options[2].dataSource, {
      tenant: "tenant",
      dataCore: "core",
      flowType: "other.0",
      eventTypes: ["updated.0"],
    })
  } finally {
    await f.pump.stop()
    time.restore()
  }
})

Deno.test("source factory failure propagates without falling back to the default source", async () => {
  const f = fixture(() => {
    throw new Error("source construction refused")
  })
  await assertRejects(() => f.internal.startPumpForGroup(meta), Error, "source construction refused")
  assertEquals(f.calls, [])
})
