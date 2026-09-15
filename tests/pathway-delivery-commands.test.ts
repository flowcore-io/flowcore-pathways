import { assertEquals, assertRejects } from "https://deno.land/std@0.224.0/assert/mod.ts"
import { InMemoryPathwayDeliveryStore, PathwaysBuilder } from "../src/mod.ts"
import type { PathwayDeliveryStore } from "../src/mod.ts"

interface Command {
  id: string
  type: string
  position: Record<string, unknown> | null
  sourceFlowTypes: string[] | null
  reason: string | null
  stopAt: string | null
}

function command(overrides: Partial<Command> = {}): Command {
  return {
    id: "cmd-1",
    type: "datapumpPause",
    position: null,
    sourceFlowTypes: null,
    reason: null,
    stopAt: null,
    ...overrides,
  }
}

/** Records what the builder asks the pump to do. */
function fakePump(pausedKeys: string[] = []) {
  const calls: string[] = []
  const paused = new Set(pausedKeys)
  return {
    calls,
    pump: {
      pause: (filter?: { flowTypes?: string[]; keys?: string[] }) => {
        calls.push(`pause:${JSON.stringify(filter ?? null)}`)
        if (filter?.keys?.includes("missing.0::default")) return []
        if (filter?.flowTypes?.includes("nope.0")) return []
        paused.add("orders.0::default")
        return ["orders.0::default"]
      },
      resume: (filter?: unknown) => {
        calls.push(`resume:${JSON.stringify(filter ?? null)}`)
        paused.delete("orders.0::default")
        return ["orders.0::default"]
      },
      reset: (position?: unknown, filter?: unknown, stopAt?: Date) => {
        calls.push(
          `reset:${JSON.stringify(position ?? null)}:${JSON.stringify(filter ?? null)}:${
            stopAt?.toISOString() ?? "none"
          }`,
        )
        return Promise.resolve(["orders.0::default"])
      },
      get pausedGroups() {
        return [...paused]
      },
      registeredPumpKeys: ["orders.0::default"],
    },
  }
}

function builderWithPump(pump: unknown, store?: PathwayDeliveryStore) {
  const builder = new PathwaysBuilder({
    baseUrl: "http://localhost:9999",
    tenant: "test-tenant",
    dataCore: "test-data-core",
    apiKey: "test-api-key",
    pathwayTimeoutMs: 1000,
  })
  if (store) {
    builder.withPathwayDeliveryStore(store)
  }
  ;(builder as unknown as { pathwayPump: unknown }).pathwayPump = pump
  return builder as unknown as {
    executePathwayCommand(cmd: Command): Promise<void>
  }
}

Deno.test("control-plane command dispatch", async (t) => {
  await t.step("an unknown command type fails instead of reporting success", async () => {
    const { pump } = fakePump()
    const builder = builderWithPump(pump)

    await assertRejects(
      () => builder.executePathwayCommand(command({ type: "somethingElse" })),
      Error,
      "Unknown command type",
    )
  })

  await t.step("a filter that matches no pump fails", async () => {
    const { pump } = fakePump()
    const builder = builderWithPump(pump)

    await assertRejects(
      () => builder.executePathwayCommand(command({ sourceFlowTypes: ["nope.0"] })),
      Error,
      "matched no pump",
    )
  })

  await t.step("pause targets are parsed from sourceFlowTypes", async () => {
    const { pump, calls } = fakePump()
    const builder = builderWithPump(pump)

    await builder.executePathwayCommand(command({ sourceFlowTypes: ["orders.0", "invoices.0::hot"] }))

    assertEquals(calls[0], 'pause:{"flowTypes":["orders.0"],"keys":["invoices.0::hot"]}')
  })

  await t.step("restart passes the same filter, so pump groups are honoured", async () => {
    const { pump, calls } = fakePump()
    const builder = builderWithPump(pump)

    await builder.executePathwayCommand(command({
      type: "datapumpRestart",
      position: { timeBucket: "20260101000000" },
      sourceFlowTypes: ["orders.0::hot"],
    }))

    assertEquals(
      calls[0],
      'reset:{"timeBucket":"20260101000000","eventId":undefined}:{"keys":["orders.0::hot"]}:none'
        .replace(',"eventId":undefined', ""),
    )
  })

  await t.step("restart passes stopAt through", async () => {
    const { pump, calls } = fakePump()
    const builder = builderWithPump(pump)

    await builder.executePathwayCommand(command({
      type: "datapumpRestart",
      position: { timeBucket: "20260101000000" },
      stopAt: "2026-02-01T00:00:00.000Z",
    }))

    assertEquals(calls[0].endsWith(":2026-02-01T00:00:00.000Z"), true)
  })

  await t.step("a pause is persisted so it survives a restart", async () => {
    const store = new InMemoryPathwayDeliveryStore()
    const { pump } = fakePump()
    const builder = builderWithPump(pump, store)

    await builder.executePathwayCommand(command())

    assertEquals(await store.getPausedPumps("test-data-core"), ["orders.0::default"])
  })

  await t.step("a resume clears the persisted pause", async () => {
    const store = new InMemoryPathwayDeliveryStore()
    const { pump } = fakePump(["orders.0::default"])
    const builder = builderWithPump(pump, store)

    await builder.executePathwayCommand(command({ type: "datapumpResume" }))

    assertEquals(await store.getPausedPumps("test-data-core"), [])
  })

  await t.step("a store that cannot be written fails the command", async () => {
    const failing: PathwayDeliveryStore = {
      getPausedPumps: () => Promise.resolve([]),
      setPausedPumps: () => Promise.reject(new Error("database is down")),
    }
    const { pump } = fakePump()
    const builder = builderWithPump(pump, failing)

    await assertRejects(
      () => builder.executePathwayCommand(command()),
      Error,
      "could not be persisted",
    )
  })
})

Deno.test("InMemoryPathwayDeliveryStore", async (t) => {
  await t.step("returns an empty set for an unknown pathway", async () => {
    const store = new InMemoryPathwayDeliveryStore()
    assertEquals(await store.getPausedPumps("nothing"), [])
  })

  await t.step("round-trips and clears", async () => {
    const store = new InMemoryPathwayDeliveryStore()
    await store.setPausedPumps("p", ["a::default"])
    assertEquals(await store.getPausedPumps("p"), ["a::default"])
    await store.setPausedPumps("p", [])
    assertEquals(await store.getPausedPumps("p"), [])
  })
})
