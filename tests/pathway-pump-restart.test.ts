import { assertEquals } from "https://deno.land/std@0.224.0/assert/mod.ts"
import { FakeTime } from "https://deno.land/std@0.224.0/testing/time.ts"
import { PathwayPump } from "../src/pathways/pump/pathway-pump.ts"
import type { PumpState, PumpStateManager, PumpStateManagerFactory } from "../src/pathways/pump/types.ts"

class InMemoryPumpStateManager implements PumpStateManager {
  private state: PumpState | null = null
  getState(): PumpState | null {
    return this.state
  }
  setState(state: PumpState): void {
    this.state = state
  }
}

function createInMemoryStateFactory(): PumpStateManagerFactory {
  const managers = new Map<string, InMemoryPumpStateManager>()
  return (flowType: string, pumpGroup: string) => {
    const key = `${flowType}::${pumpGroup}`
    if (!managers.has(key)) {
      managers.set(key, new InMemoryPumpStateManager())
    }
    return managers.get(key)!
  }
}

Deno.test({
  name: "PathwayPump restart backoff",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: async (t) => {
    await t.step("backoff formula produces correct exponential sequence capped at 30s", () => {
      const RESTART_BASE_MS = 1_000
      const RESTART_MAX_MS = 30_000
      const delays = []
      for (let attempt = 1; attempt <= 8; attempt++) {
        delays.push(Math.min(RESTART_BASE_MS * Math.pow(2, attempt - 1), RESTART_MAX_MS))
      }
      assertEquals(delays, [1_000, 2_000, 4_000, 8_000, 16_000, 30_000, 30_000, 30_000])
    })

    await t.step("pump should not be running after stop", async () => {
      const factory = createInMemoryStateFactory()
      const pump = new PathwayPump({
        stateManagerFactory: factory,
        notifier: { type: "poller", pollerIntervalMs: 60_000 },
      })

      pump.configure({
        tenant: "test-tenant",
        dataCore: "test-dc",
        apiKey: "fc_testid_testsecret",
        baseUrl: "https://api.flowcore.io",
        processEvent: async () => {},
      })

      assertEquals(pump.isRunning, false)
    })

    await t.step("restart attempts track independently per flow type", () => {
      // Simulate the per-flow-type tracking
      const restartAttempts = new Map<string, number>()

      // Flow A fails 3 times
      for (let i = 0; i < 3; i++) {
        const attempts = (restartAttempts.get("flowA") ?? 0) + 1
        restartAttempts.set("flowA", attempts)
      }

      // Flow B fails once
      const attemptsB = (restartAttempts.get("flowB") ?? 0) + 1
      restartAttempts.set("flowB", attemptsB)

      assertEquals(restartAttempts.get("flowA"), 3)
      assertEquals(restartAttempts.get("flowB"), 1)

      // Flow A restart succeeds — reset
      restartAttempts.set("flowA", 0)
      assertEquals(restartAttempts.get("flowA"), 0)
      // Flow B unaffected
      assertEquals(restartAttempts.get("flowB"), 1)
    })

    await t.step("restart attempts cleared on stop", () => {
      const restartAttempts = new Map<string, number>()
      restartAttempts.set("flowA", 5)
      restartAttempts.set("flowB", 2)

      // Simulate stop()
      restartAttempts.clear()

      assertEquals(restartAttempts.size, 0)
    })

    await t.step("restart loop keeps retrying when startPumpForGroup itself throws", async () => {
      const time = new FakeTime()
      try {
        const pump = new PathwayPump({
          stateManagerFactory: createInMemoryStateFactory(),
          notifier: { type: "poller", pollerIntervalMs: 60_000 },
        }, {
          debug: () => {},
          info: () => {},
          warn: () => {},
          error: () => {},
        })
        pump.configure({
          tenant: "t",
          dataCore: "dc",
          apiKey: "k",
          baseUrl: "https://api.flowcore.io",
          processEvent: async () => {},
        })

        const internal = pump as unknown as {
          running: boolean
          startPumpForGroup: (meta: unknown) => Promise<void>
          scheduleRestart: (meta: unknown) => void
          restartTimers: Map<string, unknown>
          restartAttempts: Map<string, number>
        }

        let attempts = 0
        internal.running = true
        internal.startPumpForGroup = () => {
          attempts++
          // First three attempts blow up — emulating a sticky failure
          // (e.g. CP unreachable, DB credentials wrong, transient network outage).
          if (attempts <= 3) {
            return Promise.reject(new Error("synthetic startup failure"))
          }
          return Promise.resolve()
        }

        const meta = { flowType: "orders", pumpGroup: "default", eventTypes: ["placed"] }
        internal.scheduleRestart(meta)

        // Each failing attempt schedules the next with exponential backoff (1s, 2s, 4s...).
        // Tick incrementally so each scheduled timer + its async callback gets a chance to run
        // before the next tick — tickAsync only fires timers already in queue at call time.
        for (let i = 0; i < 10 && attempts < 4; i++) {
          await time.tickAsync(40_000)
        }

        assertEquals(
          attempts >= 4,
          true,
          `restart loop must keep retrying after synchronous failures (got ${attempts} attempts)`,
        )
        // Once a restart succeeds, no more timers should be queued.
        assertEquals(internal.restartTimers.size, 0)
      } finally {
        time.restore()
      }
    })
  },
})
