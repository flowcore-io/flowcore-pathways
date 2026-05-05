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
  return (flowType: string, pumpGroup: string) => {
    const key = `${flowType}::${pumpGroup}`
    if (!managers.has(key)) {
      managers.set(key, new InMemoryPumpStateManager())
    }
    return managers.get(key)!
  }
}

interface GroupMeta {
  flowType: string
  pumpGroup: string
  eventTypes: string[]
}

interface InternalPump {
  dataPumpConstructor: {
    create(options: Record<string, unknown>): Promise<{ start(cb?: unknown): Promise<void> }>
  }
  startPumpForGroup(meta: GroupMeta): Promise<void>
  running: boolean
  pumps: Map<string, { stop(): Promise<void> }>
  groupMeta: Map<string, GroupMeta>
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

    await t.step("InMemoryStateFactory - creates separate managers per (flowType, pumpGroup)", () => {
      const factory = createInMemoryStateFactory()

      const userDefault = factory("user", "default")
      const userHot = factory("user", "hot")
      const userDefaultAgain = factory("user", "default")

      // Same instance for same (flowType, pumpGroup)
      assertEquals(userDefault, userDefaultAgain)
      // Different instances for different pumpGroups on the same flowType
      assertEquals(userDefault !== userHot, true)

      userDefault.setState({ timeBucket: "20260319120000" })
      assertEquals(userHot.getState(), null)
    })

    await t.step("concurrency defaults to 1 per pump when unset", async () => {
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

      const created: Record<string, number> = {}
      const internal = pump as unknown as InternalPump
      internal.dataPumpConstructor = {
        create: (options: Record<string, unknown>) => {
          const dataSource = options.dataSource as { flowType: string }
          const processor = options.processor as { concurrency: number }
          created[dataSource.flowType] = processor.concurrency
          return Promise.resolve({ start: async () => {} })
        },
      }

      await internal.startPumpForGroup({ flowType: "user", pumpGroup: "default", eventTypes: ["created"] })
      await internal.startPumpForGroup({ flowType: "order", pumpGroup: "default", eventTypes: ["placed"] })

      assertEquals(created.user, 1)
      assertEquals(created.order, 1)
    })

    await t.step("numeric concurrency sets a shared default for every pump", async () => {
      const factory = createInMemoryStateFactory()
      const pump = new PathwayPump({
        stateManagerFactory: factory,
        notifier: { type: "poller", pollerIntervalMs: 1000 },
        concurrency: 4,
      })

      pump.configure({
        tenant: "test-tenant",
        dataCore: "test-dc",
        apiKey: "test-key",
        baseUrl: "https://api.flowcore.io",
        processEvent: async () => {},
      })

      const created: Record<string, number> = {}
      const internal = pump as unknown as InternalPump
      internal.dataPumpConstructor = {
        create: (options: Record<string, unknown>) => {
          const dataSource = options.dataSource as { flowType: string }
          const processor = options.processor as { concurrency: number }
          created[dataSource.flowType] = processor.concurrency
          return Promise.resolve({ start: async () => {} })
        },
      }

      await internal.startPumpForGroup({ flowType: "user", pumpGroup: "default", eventTypes: ["created"] })
      await internal.startPumpForGroup({ flowType: "order", pumpGroup: "default", eventTypes: ["placed"] })

      assertEquals(created.user, 4)
      assertEquals(created.order, 4)
    })

    await t.step("byPumpGroup wins over byFlowType wins over default", async () => {
      const factory = createInMemoryStateFactory()
      const pump = new PathwayPump({
        stateManagerFactory: factory,
        notifier: { type: "poller", pollerIntervalMs: 1000 },
        concurrency: {
          default: 2,
          byFlowType: { orders: 5 },
          byPumpGroup: { "orders::hot": 9 },
        },
      })

      pump.configure({
        tenant: "test-tenant",
        dataCore: "test-dc",
        apiKey: "test-key",
        baseUrl: "https://api.flowcore.io",
        processEvent: async () => {},
      })

      const seenConcurrencies: number[] = []
      const internal = pump as unknown as InternalPump
      internal.dataPumpConstructor = {
        create: (options: Record<string, unknown>) => {
          const processor = options.processor as { concurrency: number }
          seenConcurrencies.push(processor.concurrency)
          return Promise.resolve({ start: async () => {} })
        },
      }

      await internal.startPumpForGroup({ flowType: "orders", pumpGroup: "hot", eventTypes: ["placed.fast"] })
      await internal.startPumpForGroup({ flowType: "orders", pumpGroup: "default", eventTypes: ["placed"] })
      await internal.startPumpForGroup({ flowType: "users", pumpGroup: "default", eventTypes: ["created"] })

      assertEquals(
        seenConcurrencies,
        [9, 5, 2],
        "byPumpGroup wins, then byFlowType, then default",
      )
    })

    await t.step(
      "pulse pathwayId is forwarded unmodified (CP requires UUID, no ::flowType::pumpGroup suffix)",
      async () => {
        const factory = createInMemoryStateFactory()
        const pump = new PathwayPump({
          stateManagerFactory: factory,
          notifier: { type: "poller", pollerIntervalMs: 1000 },
          pulse: { url: "http://cp.test", pathwayId: "p-123" },
        })

        pump.configure({
          tenant: "t",
          dataCore: "dc",
          apiKey: "k",
          baseUrl: "https://api.flowcore.io",
          processEvent: async () => {},
        })

        const seen: string[] = []
        const internal = pump as unknown as InternalPump
        internal.dataPumpConstructor = {
          create: (options: Record<string, unknown>) => {
            const pulse = options.pulse as { pathwayId: string }
            seen.push(pulse.pathwayId)
            return Promise.resolve({ start: async () => {} })
          },
        }

        await internal.startPumpForGroup({ flowType: "orders", pumpGroup: "hot", eventTypes: ["placed.fast"] })
        await internal.startPumpForGroup({ flowType: "orders", pumpGroup: "default", eventTypes: ["placed"] })

        // Same pathwayId on every group — the data-pathways CP pulse route validates
        // pathwayId as a UUID and rejects anything else with 400 BAD_REQUEST. Per-group
        // health visibility will return as a proper additive `pumpGroup` field through
        // SDK + CP, not by mangling the pathwayId here.
        assertEquals(seen, ["p-123", "p-123"])
      },
    )

    await t.step("notifier dataSource.eventTypes is restricted to the group's subset", async () => {
      const factory = createInMemoryStateFactory()
      const pump = new PathwayPump({
        stateManagerFactory: factory,
        notifier: { type: "poller", pollerIntervalMs: 1000 },
      })

      pump.configure({
        tenant: "t",
        dataCore: "dc",
        apiKey: "k",
        baseUrl: "https://api.flowcore.io",
        processEvent: async () => {},
      })

      const seenNotifierEventTypes: string[][] = []
      const internal = pump as unknown as InternalPump
      internal.dataPumpConstructor = {
        create: (options: Record<string, unknown>) => {
          const notifier = options.notifier as { dataSource: { eventTypes: string[] } }
          seenNotifierEventTypes.push([...notifier.dataSource.eventTypes])
          return Promise.resolve({ start: async () => {} })
        },
      }

      await internal.startPumpForGroup({
        flowType: "orders",
        pumpGroup: "hot",
        eventTypes: ["placed.fast", "shipped.fast"],
      })
      await internal.startPumpForGroup({
        flowType: "orders",
        pumpGroup: "default",
        eventTypes: ["placed", "shipped"],
      })

      assertEquals(seenNotifierEventTypes[0], ["placed.fast", "shipped.fast"])
      assertEquals(seenNotifierEventTypes[1], ["placed", "shipped"])
    })

    await t.step("registeredPumpGroups exposes every (flowType, pumpGroup) pair", async () => {
      const factory = createInMemoryStateFactory()
      const pump = new PathwayPump({
        stateManagerFactory: factory,
        notifier: { type: "poller", pollerIntervalMs: 1000 },
      })

      pump.configure({
        tenant: "t",
        dataCore: "dc",
        apiKey: "k",
        baseUrl: "https://api.flowcore.io",
        processEvent: async () => {},
      })

      const internal = pump as unknown as InternalPump & { groupMeta: Map<string, GroupMeta> }
      internal.dataPumpConstructor = {
        create: (_options: Record<string, unknown>) => Promise.resolve({ start: async () => {} }),
      }

      // Bypass start() to avoid the dynamic import of @flowcore/data-pump (which would
      // overwrite our stubbed dataPumpConstructor and try to authenticate against a real CP).
      const groupsToStart: GroupMeta[] = [
        { flowType: "orders", pumpGroup: "default", eventTypes: ["placed"] },
        { flowType: "orders", pumpGroup: "hot", eventTypes: ["placed.fast"] },
        { flowType: "users", pumpGroup: "default", eventTypes: ["created"] },
      ]
      for (const meta of groupsToStart) {
        internal.groupMeta.set(`${meta.flowType}::${meta.pumpGroup}`, meta)
        await internal.startPumpForGroup(meta)
      }

      const groups = pump.registeredPumpGroups.map((g) => `${g.flowType}::${g.pumpGroup}`).sort()
      assertEquals(groups, ["orders::default", "orders::hot", "users::default"])
      // Unique flow types preserved for back-compat.
      assertEquals([...pump.registeredFlowTypes].sort(), ["orders", "users"])
    })

    await t.step(
      "arity-2 state factory receives both (flowType, pumpGroup) so each group gets its own manager",
      async () => {
        // Regression: a factory declared with a default value on `pumpGroup`
        // (e.g. `(flowType, pumpGroup = "default") => …`) has `.length === 1`,
        // is detected as legacy, and is called with `flowType` only — collapsing
        // every group onto a shared state manager. The factory MUST declare
        // arity 2 (no defaults) so per-(flowType, pumpGroup) isolation works.
        const calls: Array<{ flowType: string; pumpGroup: string | undefined }> = []
        const stateByKey = new Map<string, InMemoryPumpStateManager>()
        // Explicit arity 2 — `.length === 2`.
        const factory = (flowType: string, pumpGroup: string): PumpStateManager => {
          calls.push({ flowType, pumpGroup })
          const key = `${flowType}::${pumpGroup}`
          if (!stateByKey.has(key)) {
            stateByKey.set(key, new InMemoryPumpStateManager())
          }
          return stateByKey.get(key)!
        }

        assertEquals(factory.length, 2, "factory must declare arity 2 — sanity check")

        const warns: string[] = []
        const pump = new PathwayPump({
          stateManagerFactory: factory,
          notifier: { type: "poller", pollerIntervalMs: 1000 },
        }, {
          debug: () => {},
          info: () => {},
          warn: (msg: string) => warns.push(msg),
          error: () => {},
        })

        pump.configure({
          tenant: "t",
          dataCore: "dc",
          apiKey: "k",
          baseUrl: "https://api.flowcore.io",
          processEvent: async () => {},
        })

        const internal = pump as unknown as InternalPump
        internal.dataPumpConstructor = {
          create: (_options: Record<string, unknown>) => Promise.resolve({ start: async () => {} }),
        }

        await internal.startPumpForGroup({ flowType: "orders", pumpGroup: "hot", eventTypes: ["placed.fast"] })
        await internal.startPumpForGroup({ flowType: "orders", pumpGroup: "default", eventTypes: ["placed"] })

        // Both args forwarded — legacy fallback was NOT taken.
        assertEquals(calls, [
          { flowType: "orders", pumpGroup: "hot" },
          { flowType: "orders", pumpGroup: "default" },
        ])
        // Two distinct state manager instances exist, one per (flowType, pumpGroup).
        assertEquals(stateByKey.size, 2)
        // No legacy-arity warning.
        assertEquals(
          warns.filter((m) => m.includes("legacy single-arg signature")).length,
          0,
          "arity-2 factory must not trigger the legacy deprecation warning",
        )
      },
    )

    await t.step("legacy single-arg state factory is accepted with a deprecation warning", async () => {
      const created = new Map<string, InMemoryPumpStateManager>()
      // Arity 1 — old factory signature.
      const legacyFactory = (flowType: string): PumpStateManager => {
        if (!created.has(flowType)) {
          created.set(flowType, new InMemoryPumpStateManager())
        }
        return created.get(flowType)!
      }

      const warns: Array<{ msg: string; meta?: Record<string, unknown> }> = []
      const pump = new PathwayPump({
        stateManagerFactory: legacyFactory as unknown as PumpStateManagerFactory,
        notifier: { type: "poller", pollerIntervalMs: 1000 },
      }, {
        debug: () => {},
        info: () => {},
        warn: (msg: string, meta?: Record<string, unknown>) => {
          warns.push({ msg, meta })
        },
        error: () => {},
      })

      pump.configure({
        tenant: "t",
        dataCore: "dc",
        apiKey: "k",
        baseUrl: "https://api.flowcore.io",
        processEvent: async () => {},
      })

      const internal = pump as unknown as InternalPump
      internal.dataPumpConstructor = {
        create: (_options: Record<string, unknown>) => Promise.resolve({ start: async () => {} }),
      }

      // Two pump groups on same flow type with a legacy factory → warning fires once.
      await internal.startPumpForGroup({ flowType: "orders", pumpGroup: "hot", eventTypes: ["placed.fast"] })
      await internal.startPumpForGroup({ flowType: "orders", pumpGroup: "default", eventTypes: ["placed"] })

      const legacyWarns = warns.filter((w) => w.msg.includes("legacy single-arg signature"))
      assertEquals(legacyWarns.length, 1, "deprecation warning should be emitted exactly once")
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

      const internal = pump as unknown as InternalPump
      internal.running = true
      internal.pumps = new Map([
        ["user::default", { stop: async () => {} }],
        ["order::default", { stop: async () => {} }],
      ])
      internal.groupMeta = new Map([
        ["user::default", { flowType: "user", pumpGroup: "default", eventTypes: ["created", "updated"] }],
        ["order::default", { flowType: "order", pumpGroup: "default", eventTypes: ["placed"] }],
      ])

      const createdFlowTypes: string[] = []
      const createdPulsePathwayIds: string[] = []
      internal.dataPumpConstructor = {
        create: async (options: Record<string, unknown>) => {
          const dataSource = options.dataSource as { flowType: string }
          const pulse = options.pulse as { pathwayId: string }
          createdFlowTypes.push(dataSource.flowType)
          createdPulsePathwayIds.push(pulse.pathwayId)
          return { start: async () => {} }
        },
      }

      await pump.setPulseConfig({
        url: "http://localhost:3000",
        pathwayId: "pathway-123",
      })

      assertEquals(createdFlowTypes.sort(), ["order", "user"])
      assertEquals(
        createdPulsePathwayIds.sort(),
        ["pathway-123", "pathway-123"],
      )
      assertEquals(pump.isRunning, true)
      assertEquals(pump.registeredFlowTypes.sort(), ["order", "user"])
    })
  },
})
