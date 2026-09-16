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

Deno.test("restoring delivery state prefers the control plane", async (t) => {
  interface Internals {
    pathwayMode: string
    runtimeEnv: string
    pathwayName?: string
    tenant: string
    pulseUrl: string
    deliveryStore: PathwayDeliveryStore
    fetchDeliveryStateFromControlPlane(): Promise<string[] | null>
    buildPumpRegistrations(): Array<{ flowType: string; eventType: string; pumpGroup: string }>
  }

  /** A builder wired to a fake control plane, with two pump groups registered. */
  function builderWithControlPlane(
    response: { status: number; body?: unknown },
    store?: PathwayDeliveryStore,
  ) {
    const builder = new PathwaysBuilder({
      baseUrl: "http://localhost:9999",
      tenant: "test-tenant",
      dataCore: "test-data-core",
      apiKey: "test-api-key",
      pathwayTimeoutMs: 1000,
    })
    if (store) builder.withPathwayDeliveryStore(store)

    const internals = builder as unknown as Internals
    internals.pathwayMode = "virtual"
    // Production: a development boot must never reach the control plane.
    internals.runtimeEnv = "production"
    internals.pathwayName = "svc"
    internals.buildPumpRegistrations = () => [
      { flowType: "orders.0", eventType: "e.0", pumpGroup: "default" },
      { flowType: "orders.0", eventType: "e.0", pumpGroup: "hot" },
    ]

    const originalFetch = globalThis.fetch
    globalThis.fetch = () =>
      Promise.resolve(
        new Response(response.body === undefined ? "" : JSON.stringify(response.body), {
          status: response.status,
          headers: { "Content-Type": "application/json" },
        }),
      )

    return {
      internals,
      restore: () => {
        globalThis.fetch = originalFetch
      },
    }
  }

  await t.step("a paused pathway with no targets expands to every pump", async () => {
    const { internals, restore } = builderWithControlPlane({
      status: 200,
      body: { deliveryState: "paused", deliveryPauseTargets: null },
    })
    try {
      const keys = await internals.fetchDeliveryStateFromControlPlane()
      assertEquals(keys?.sort(), ["orders.0::default", "orders.0::hot"])
    } finally {
      restore()
    }
  })

  await t.step("a bare flow-type target expands to every pump group on it", async () => {
    const { internals, restore } = builderWithControlPlane({
      status: 200,
      body: { deliveryState: "paused", deliveryPauseTargets: ["orders.0"] },
    })
    try {
      const keys = await internals.fetchDeliveryStateFromControlPlane()
      assertEquals(keys?.sort(), ["orders.0::default", "orders.0::hot"])
    } finally {
      restore()
    }
  })

  await t.step("a composite target resolves to exactly one pump", async () => {
    const { internals, restore } = builderWithControlPlane({
      status: 200,
      body: { deliveryState: "paused", deliveryPauseTargets: ["orders.0::hot"] },
    })
    try {
      assertEquals(await internals.fetchDeliveryStateFromControlPlane(), ["orders.0::hot"])
    } finally {
      restore()
    }
  })

  await t.step("an active pathway resolves to nothing paused", async () => {
    const { internals, restore } = builderWithControlPlane({
      status: 200,
      body: { deliveryState: "active", deliveryPauseTargets: null },
    })
    try {
      assertEquals(await internals.fetchDeliveryStateFromControlPlane(), [])
    } finally {
      restore()
    }
  })

  await t.step("an older control plane that omits the field defers to the cache", async () => {
    const { internals, restore } = builderWithControlPlane({
      status: 200,
      body: { id: "x", tenant: "test-tenant" },
    })
    try {
      // null means "no opinion", so the caller falls back rather than assuming active.
      assertEquals(await internals.fetchDeliveryStateFromControlPlane(), null)
    } finally {
      restore()
    }
  })

  await t.step("an unreachable control plane defers to the cache", async () => {
    const { internals, restore } = builderWithControlPlane({ status: 503 })
    try {
      assertEquals(await internals.fetchDeliveryStateFromControlPlane(), null)
    } finally {
      restore()
    }
  })
})
