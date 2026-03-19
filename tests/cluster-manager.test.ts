import { assertEquals, assertExists } from "https://deno.land/std@0.224.0/assert/mod.ts"
import type { PathwayCoordinator } from "../src/pathways/cluster/types.ts"
import { ClusterManager } from "../src/pathways/cluster/cluster-manager.ts"
import type { FlowcoreEvent } from "../src/contracts/event.ts"

/**
 * In-memory coordinator for testing
 */
class InMemoryCoordinator implements PathwayCoordinator {
  private leases: Map<string, { instanceId: string; expiresAt: number }> = new Map()
  private instances: Map<string, { address: string; lastHeartbeat: number }> = new Map()

  async acquireLease(instanceId: string, key: string, ttlMs: number): Promise<boolean> {
    const existing = this.leases.get(key)
    if (existing && existing.expiresAt > Date.now() && existing.instanceId !== instanceId) {
      return false
    }
    this.leases.set(key, { instanceId, expiresAt: Date.now() + ttlMs })
    return true
  }

  async renewLease(instanceId: string, key: string, ttlMs: number): Promise<boolean> {
    const existing = this.leases.get(key)
    if (!existing || existing.instanceId !== instanceId) return false
    this.leases.set(key, { instanceId, expiresAt: Date.now() + ttlMs })
    return true
  }

  async releaseLease(instanceId: string, key: string): Promise<void> {
    const existing = this.leases.get(key)
    if (existing && existing.instanceId === instanceId) {
      this.leases.delete(key)
    }
  }

  async register(instanceId: string, address: string): Promise<void> {
    this.instances.set(instanceId, { address, lastHeartbeat: Date.now() })
  }

  async heartbeat(instanceId: string): Promise<void> {
    const existing = this.instances.get(instanceId)
    if (existing) {
      existing.lastHeartbeat = Date.now()
    }
  }

  async unregister(instanceId: string): Promise<void> {
    this.instances.delete(instanceId)
  }

  async getInstances(staleThresholdMs: number): Promise<Array<{ instanceId: string; address: string }>> {
    const now = Date.now()
    const result: Array<{ instanceId: string; address: string }> = []
    for (const [instanceId, { address, lastHeartbeat }] of this.instances) {
      if (now - lastHeartbeat < staleThresholdMs) {
        result.push({ instanceId, address })
      }
    }
    return result
  }
}

function createTestEvent(overrides?: Partial<FlowcoreEvent>): FlowcoreEvent {
  return {
    eventId: crypto.randomUUID(),
    timeBucket: "20260319120000",
    tenant: "test-tenant",
    dataCoreId: "test-dc",
    flowType: "user",
    eventType: "created",
    metadata: {},
    payload: { name: "test" },
    validTime: new Date().toISOString(),
    ...overrides,
  }
}

Deno.test({
  name: "ClusterManager Tests",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: async (t) => {
    await t.step("should create cluster manager and become leader", async () => {
      const coordinator = new InMemoryCoordinator()
      const manager = new ClusterManager(
        {
          coordinator,
          advertisedAddress: "ws://localhost:19001",
          port: 19001,
          leaseTtlMs: 5000,
          leaseRenewIntervalMs: 2000,
          heartbeatIntervalMs: 1000,
        },
      )

      const processedEvents: FlowcoreEvent[] = []
      manager.setEventHandler(async (_pathway, event) => {
        processedEvents.push(event)
      })

      await manager.start()

      // Wait for leader election
      await new Promise((r) => setTimeout(r, 500))

      assertEquals(manager.isLeader, true)
      assertEquals(manager.isRunning, true)

      await manager.stop()
      assertEquals(manager.isRunning, false)
    })

    await t.step("should process event locally when leader has no workers", async () => {
      const coordinator = new InMemoryCoordinator()
      const manager = new ClusterManager(
        {
          coordinator,
          advertisedAddress: "ws://localhost:19002",
          port: 19002,
          leaseTtlMs: 5000,
          leaseRenewIntervalMs: 2000,
          heartbeatIntervalMs: 1000,
        },
      )

      const processedEvents: FlowcoreEvent[] = []
      manager.setEventHandler(async (_pathway, event) => {
        processedEvents.push(event)
      })

      await manager.start()
      await new Promise((r) => setTimeout(r, 500))

      const event = createTestEvent()
      await manager.processEvent("user/created", event)

      assertEquals(processedEvents.length, 1)
      assertEquals(processedEvents[0].eventId, event.eventId)

      await manager.stop()
    })

    await t.step("should have unique instance ID", async () => {
      const coordinator = new InMemoryCoordinator()
      const manager1 = new ClusterManager(
        {
          coordinator,
          advertisedAddress: "ws://localhost:19003",
          port: 19003,
        },
      )
      const manager2 = new ClusterManager(
        {
          coordinator,
          advertisedAddress: "ws://localhost:19004",
          port: 19004,
        },
      )

      assertExists(manager1.currentInstanceId)
      assertExists(manager2.currentInstanceId)
      assertEquals(manager1.currentInstanceId !== manager2.currentInstanceId, true)
    })

    await t.step("second instance should become worker when first is leader", async () => {
      const coordinator = new InMemoryCoordinator()

      const leader = new ClusterManager(
        {
          coordinator,
          advertisedAddress: "ws://localhost:19005",
          port: 19005,
          leaseTtlMs: 30000,
          leaseRenewIntervalMs: 10000,
          heartbeatIntervalMs: 1000,
        },
      )
      leader.setEventHandler(async () => {})
      await leader.start()
      await new Promise((r) => setTimeout(r, 500))

      assertEquals(leader.isLeader, true)

      const worker = new ClusterManager(
        {
          coordinator,
          advertisedAddress: "ws://localhost:19006",
          port: 19006,
          leaseTtlMs: 30000,
          leaseRenewIntervalMs: 10000,
          heartbeatIntervalMs: 1000,
        },
      )
      worker.setEventHandler(async () => {})
      await worker.start()
      await new Promise((r) => setTimeout(r, 500))

      assertEquals(worker.isWorker, true)

      await worker.stop()
      await leader.stop()
    })

    await t.step("InMemoryCoordinator - lease conflict", async () => {
      const coordinator = new InMemoryCoordinator()

      const acquired1 = await coordinator.acquireLease("instance-1", "test-key", 30000)
      assertEquals(acquired1, true)

      const acquired2 = await coordinator.acquireLease("instance-2", "test-key", 30000)
      assertEquals(acquired2, false)

      // Same instance can re-acquire
      const acquired3 = await coordinator.acquireLease("instance-1", "test-key", 30000)
      assertEquals(acquired3, true)
    })

    await t.step("InMemoryCoordinator - renew lease", async () => {
      const coordinator = new InMemoryCoordinator()

      await coordinator.acquireLease("instance-1", "test-key", 30000)

      const renewed = await coordinator.renewLease("instance-1", "test-key", 30000)
      assertEquals(renewed, true)

      const renewedWrong = await coordinator.renewLease("instance-2", "test-key", 30000)
      assertEquals(renewedWrong, false)
    })

    await t.step("InMemoryCoordinator - instance registration", async () => {
      const coordinator = new InMemoryCoordinator()

      await coordinator.register("inst-1", "ws://localhost:1000")
      await coordinator.register("inst-2", "ws://localhost:2000")

      const instances = await coordinator.getInstances(60000)
      assertEquals(instances.length, 2)

      await coordinator.unregister("inst-1")
      const instances2 = await coordinator.getInstances(60000)
      assertEquals(instances2.length, 1)
      assertEquals(instances2[0].instanceId, "inst-2")
    })
  },
})
