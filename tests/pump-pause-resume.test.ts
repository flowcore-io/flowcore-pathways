import { assertEquals, assertThrows } from "https://deno.land/std@0.224.0/assert/mod.ts"
import { parsePumpTarget, PathwayPump, pumpFilterFromTargets } from "../src/pathways/pump/pathway-pump.ts"
import type { PumpStateManager } from "../src/pathways/pump/types.ts"

/**
 * Stand-in for `@flowcore/data-pump`'s FlowcoreDataPump. Records the lifecycle calls that
 * PathwayPump makes, so the pause wiring is testable without any network.
 */
class FakeDataPump {
  public paused = false
  public calls: string[] = []
  pause(): void {
    this.calls.push("pause")
    this.paused = true
  }
  resume(): void {
    this.calls.push("resume")
    this.paused = false
  }
  restart(state?: unknown): Promise<void> {
    this.calls.push(`restart:${JSON.stringify(state ?? null)}`)
    return Promise.resolve()
  }
}

interface PumpInternals {
  running: boolean
  pumps: Map<string, FakeDataPump>
  groupMeta: Map<string, { flowType: string; pumpGroup: string; eventTypes: string[] }>
  stateManagers: Map<string, PumpStateManager>
}

/**
 * Build a PathwayPump that believes it is running the given `(flowType, pumpGroup)` pairs.
 * Going through `start()` would dynamically import the real data pump, so the group maps
 * are seeded directly instead.
 */
function pumpWithGroups(groups: Array<[string, string]>): {
  pump: PathwayPump
  fakes: Map<string, FakeDataPump>
} {
  const pump = new PathwayPump({
    stateManagerFactory: () => ({ getState: () => null, setState: () => {} }) as PumpStateManager,
  })
  const internals = pump as unknown as PumpInternals
  const fakes = new Map<string, FakeDataPump>()

  internals.running = true
  for (const [flowType, pumpGroup] of groups) {
    const key = `${flowType}::${pumpGroup}`
    const fake = new FakeDataPump()
    fakes.set(key, fake)
    internals.pumps.set(key, fake)
    internals.groupMeta.set(key, { flowType, pumpGroup, eventTypes: ["e.0"] })
    internals.stateManagers.set(key, { getState: () => null, setState: () => {} })
  }

  return { pump, fakes }
}

Deno.test("pump target parsing", async (t) => {
  await t.step("a bare name targets the whole flow type", () => {
    assertEquals(parsePumpTarget("orders.0"), { flowType: "orders.0" })
  })

  await t.step("a composite name targets one pump group", () => {
    assertEquals(parsePumpTarget("orders.0::hot"), { flowType: "orders.0", pumpGroup: "hot" })
  })

  await t.step("a mixed target list becomes one filter", () => {
    assertEquals(pumpFilterFromTargets(["orders.0", "invoices.0::hot"]), {
      flowTypes: ["orders.0"],
      keys: ["invoices.0::hot"],
    })
  })

  await t.step("an empty list means every pump", () => {
    assertEquals(pumpFilterFromTargets([]), undefined)
    assertEquals(pumpFilterFromTargets(null), undefined)
  })
})

Deno.test("PathwayPump pause and resume", async (t) => {
  await t.step("pause with no filter pauses every pump", () => {
    const { pump, fakes } = pumpWithGroups([["orders.0", "default"], ["orders.0", "hot"], [
      "invoices.0",
      "default",
    ]])

    const paused = pump.pause()

    assertEquals(paused.sort(), ["invoices.0::default", "orders.0::default", "orders.0::hot"])
    for (const fake of fakes.values()) {
      assertEquals(fake.paused, true)
    }
  })

  await t.step("pause one flow type leaves the others delivering", () => {
    const { pump, fakes } = pumpWithGroups([["orders.0", "default"], ["orders.0", "hot"], [
      "invoices.0",
      "default",
    ]])

    const paused = pump.pause(pumpFilterFromTargets(["orders.0"]))

    assertEquals(paused.sort(), ["orders.0::default", "orders.0::hot"])
    assertEquals(fakes.get("invoices.0::default")!.paused, false)
  })

  await t.step("pause one pump group leaves its sibling delivering", () => {
    const { pump, fakes } = pumpWithGroups([["orders.0", "default"], ["orders.0", "hot"]])

    const paused = pump.pause(pumpFilterFromTargets(["orders.0::hot"]))

    assertEquals(paused, ["orders.0::hot"])
    assertEquals(fakes.get("orders.0::hot")!.paused, true)
    assertEquals(fakes.get("orders.0::default")!.paused, false)
  })

  await t.step("a target list mixing both forms is a union, not an intersection", () => {
    const { pump } = pumpWithGroups([["orders.0", "default"], ["orders.0", "hot"], [
      "invoices.0",
      "cold",
    ]])

    const paused = pump.pause(pumpFilterFromTargets(["orders.0", "invoices.0::cold"]))

    assertEquals(paused.sort(), ["invoices.0::cold", "orders.0::default", "orders.0::hot"])
  })

  await t.step("a filter that matches nothing returns an empty array", () => {
    const { pump } = pumpWithGroups([["orders.0", "default"]])

    assertEquals(pump.pause(pumpFilterFromTargets(["nope.0"])), [])
    assertEquals(pump.pause(pumpFilterFromTargets(["orders.0::missing"])), [])
  })

  await t.step("resume restores only the targeted pumps", () => {
    const { pump, fakes } = pumpWithGroups([["orders.0", "default"], ["orders.0", "hot"]])
    pump.pause()

    const resumed = pump.resume(pumpFilterFromTargets(["orders.0::hot"]))

    assertEquals(resumed, ["orders.0::hot"])
    assertEquals(fakes.get("orders.0::hot")!.paused, false)
    assertEquals(fakes.get("orders.0::default")!.paused, true)
    assertEquals(pump.pausedGroups, ["orders.0::default"])
  })

  await t.step("pause and resume are idempotent", () => {
    const { pump, fakes } = pumpWithGroups([["orders.0", "default"]])
    const fake = fakes.get("orders.0::default")!

    pump.pause()
    pump.pause()
    assertEquals(fake.calls.filter((c) => c === "pause").length, 1)

    pump.resume()
    pump.resume()
    assertEquals(fake.calls.filter((c) => c === "resume").length, 1)
  })

  await t.step("isPaused reports per pump group", () => {
    const { pump } = pumpWithGroups([["orders.0", "default"], ["orders.0", "hot"]])
    pump.pause(pumpFilterFromTargets(["orders.0::hot"]))

    assertEquals(pump.isPaused("orders.0", "hot"), true)
    assertEquals(pump.isPaused("orders.0"), false)
  })

  await t.step("pause and resume refuse to run on a stopped pump", () => {
    const { pump } = pumpWithGroups([["orders.0", "default"]])
    ;(pump as unknown as PumpInternals).running = false

    assertThrows(() => pump.pause(), Error, "not running")
    assertThrows(() => pump.resume(), Error, "not running")
  })
})

Deno.test("PathwayPump reset honours pump groups", async (t) => {
  await t.step("a composite target resets only that pump", async () => {
    const { pump, fakes } = pumpWithGroups([["orders.0", "default"], ["orders.0", "hot"]])

    const reset = await pump.reset(
      { timeBucket: "20260101000000" },
      pumpFilterFromTargets(["orders.0::hot"]),
    )

    assertEquals(reset, ["orders.0::hot"])
    assertEquals(fakes.get("orders.0::default")!.calls.length, 0)
  })

  await t.step("a bare target resets every group on the flow type", async () => {
    const { pump } = pumpWithGroups([["orders.0", "default"], ["orders.0", "hot"], [
      "invoices.0",
      "default",
    ]])

    const reset = await pump.reset(
      { timeBucket: "20260101000000" },
      pumpFilterFromTargets(["orders.0"]),
    )

    assertEquals(reset.sort(), ["orders.0::default", "orders.0::hot"])
  })

  await t.step("a filter that matches nothing resets nothing", async () => {
    const { pump } = pumpWithGroups([["orders.0", "default"]])

    assertEquals(await pump.reset({ timeBucket: "20260101000000" }, pumpFilterFromTargets(["nope.0"])), [])
  })

  await t.step("legacy string-array filters still work", async () => {
    const { pump } = pumpWithGroups([["orders.0", "default"], ["invoices.0", "default"]])

    const reset = await pump.reset({ timeBucket: "20260101000000" }, ["orders.0"])

    assertEquals(reset, ["orders.0::default"])
  })
})

Deno.test("PathwayPump restores a pause before the pumps start", async (t) => {
  await t.step("seeded keys make each pump born paused", () => {
    const pump = new PathwayPump({
      stateManagerFactory: () => ({ getState: () => null, setState: () => {} }) as PumpStateManager,
    })

    pump.setInitialPausedPumps(["orders.0::hot"])

    assertEquals(pump.isPaused("orders.0", "hot"), true)
    assertEquals(pump.pausedGroups, ["orders.0::hot"])
  })

  await t.step("seeding a running pump is refused", () => {
    const { pump } = pumpWithGroups([["orders.0", "default"]])

    assertThrows(() => pump.setInitialPausedPumps(["orders.0::default"]), Error, "already running")
  })

  await t.step("stale keys are pruned once the registrations are known", () => {
    const { pump } = pumpWithGroups([["orders.0", "default"]])
    const internals = pump as unknown as { paused: Set<string> }
    internals.paused = new Set(["orders.0::default", "removed.0::default"])

    const live = pump.prunePausedPumps()

    assertEquals(live, ["orders.0::default"])
  })
})
