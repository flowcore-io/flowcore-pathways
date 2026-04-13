import { assertEquals, assertExists } from "https://deno.land/std@0.224.0/assert/mod.ts"
import { PathwayPump } from "../src/pathways/pump/pathway-pump.ts"
import type { PumpState, PumpStateManager, PumpStateManagerFactory } from "../src/pathways/pump/types.ts"
import type { FlowcoreEvent } from "../src/contracts/event.ts"

/**
 * In-memory pump state manager for testing
 */
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
  name: "PathwayPump Tests",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: async (t) => {
    await t.step("should create pump with options", () => {
      const factory = createInMemoryStateFactory()
      const pump = new PathwayPump({
        stateManagerFactory: factory,
        notifier: { type: "websocket" },
        bufferSize: 500,
        maxRedeliveryCount: 5,
      })

      assertExists(pump)
      assertEquals(pump.isRunning, false)
    })

    await t.step("should throw if not configured before start", async () => {
      const factory = createInMemoryStateFactory()
      const pump = new PathwayPump({
        stateManagerFactory: factory,
      })

      try {
        await pump.start([{ flowType: "user", eventType: "created" }])
        throw new Error("Expected error was not thrown")
      } catch (err) {
        assertEquals(err instanceof Error, true)
        assertEquals((err as Error).message, "PathwayPump not configured — call configure() before start()")
      }
    })

    await t.step("should configure pump correctly", () => {
      const factory = createInMemoryStateFactory()
      const pump = new PathwayPump({
        stateManagerFactory: factory,
      })

      pump.configure({
        tenant: "test-tenant",
        dataCore: "test-dc",
        apiKey: "test-key",
        baseUrl: "https://api.flowcore.io",
        processEvent: async () => {},
      })

      // Should not throw — configuration is valid
      assertExists(pump)
    })

    await t.step("InMemoryPumpStateManager - get/set state", () => {
      const manager = new InMemoryPumpStateManager()

      assertEquals(manager.getState(), null)

      manager.setState({ timeBucket: "20260319120000", eventId: "evt-1" })
      const state = manager.getState()
      assertExists(state)
      assertEquals(state!.timeBucket, "20260319120000")
      assertEquals(state!.eventId, "evt-1")
    })

    await t.step("InMemoryStateFactory - creates separate managers per flowType", () => {
      const factory = createInMemoryStateFactory()

      const mgr1 = factory("user")
      const mgr2 = factory("order")
      const mgr1Again = factory("user")

      // Same instance for same flowType
      assertEquals(mgr1, mgr1Again)

      // Different instances for different flowTypes
      assertEquals(mgr1 !== mgr2, true)

      mgr1.setState({ timeBucket: "20260319120000" })
      assertEquals(mgr2.getState(), null)
    })

    await t.step("should group pathways by flowType", () => {
      // Test the grouping logic conceptually
      const pathways = [
        { flowType: "user", eventType: "created" },
        { flowType: "user", eventType: "updated" },
        { flowType: "order", eventType: "placed" },
        { flowType: "order", eventType: "shipped" },
        { flowType: "payment", eventType: "received" },
      ]

      const groups = new Map<string, string[]>()
      for (const pw of pathways) {
        const eventTypes = groups.get(pw.flowType) ?? []
        eventTypes.push(pw.eventType)
        groups.set(pw.flowType, eventTypes)
      }

      assertEquals(groups.size, 3)
      assertEquals(groups.get("user"), ["created", "updated"])
      assertEquals(groups.get("order"), ["placed", "shipped"])
      assertEquals(groups.get("payment"), ["received"])
    })

    await t.step("setPulseConfig recreates running pumps with the new pulse configuration", async () => {
      const factory = createInMemoryStateFactory()
      const pump = new PathwayPump({
        stateManagerFactory: factory,
        notifier: { type: "poller", pollerIntervalMs: 1000 },
      })

      pump.configure({
        tenant: "test-tenant",
        dataCore: "test-dc",
        apiKey: "test-key",
        baseUrl: "https://api.flowcore.io",
        processEvent: async (_pathway: string, _event: FlowcoreEvent) => {},
      })

      const stoppedFlowTypes: string[] = []
      const createdFlowTypes: string[] = []
      const createdPulsePathwayIds: string[] = []
      ;(pump as unknown as {
        running: boolean
        pumps: Map<string, { stop(): Promise<void> }>
        flowTypeEventTypes: Map<string, string[]>
        dataPumpConstructor: {
          create(options: Record<string, unknown>): Promise<{ start(cb?: unknown): Promise<void> }>
        }
      }).running = true
      ;(pump as unknown as { pumps: Map<string, { stop(): Promise<void> }> }).pumps = new Map([
        ["user", {
          stop: async () => {
            stoppedFlowTypes.push("user")
          },
        }],
        ["order", {
          stop: async () => {
            stoppedFlowTypes.push("order")
          },
        }],
      ])
      ;(pump as unknown as { flowTypeEventTypes: Map<string, string[]> }).flowTypeEventTypes = new Map([
        ["user", ["created", "updated"]],
        ["order", ["placed"]],
      ])
      ;(pump as unknown as {
        dataPumpConstructor: {
          create(options: Record<string, unknown>): Promise<{ start(cb?: unknown): Promise<void> }>
        }
      }).dataPumpConstructor = {
        create: async (options: Record<string, unknown>) => {
          const dataSource = options.dataSource as { flowType: string }
          const pulse = options.pulse as { pathwayId: string }
          createdFlowTypes.push(dataSource.flowType)
          createdPulsePathwayIds.push(pulse.pathwayId)

          return {
            start: async () => {},
          }
        },
      }

      await pump.setPulseConfig({
        url: "http://localhost:3000",
        pathwayId: "pathway-123",
      })

      assertEquals(stoppedFlowTypes.sort(), ["order", "user"])
      assertEquals(createdFlowTypes.sort(), ["order", "user"])
      assertEquals(createdPulsePathwayIds, ["pathway-123", "pathway-123"])
      assertEquals(pump.isRunning, true)
      assertEquals(pump.registeredFlowTypes.sort(), ["order", "user"])
    })
  },
})
