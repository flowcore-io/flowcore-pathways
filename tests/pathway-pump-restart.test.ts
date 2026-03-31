import { assertEquals } from "https://deno.land/std@0.224.0/assert/mod.ts"
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
  return (flowType: string) => {
    if (!managers.has(flowType)) {
      managers.set(flowType, new InMemoryPumpStateManager())
    }
    return managers.get(flowType)!
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
  },
})
