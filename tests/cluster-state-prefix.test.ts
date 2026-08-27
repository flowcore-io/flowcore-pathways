import { assertEquals } from "https://deno.land/std@0.224.0/assert/mod.ts"
import { ClusterManager } from "../src/pathways/cluster/cluster-manager.ts"
import type { PathwayCoordinator } from "../src/pathways/cluster/types.ts"
import { InMemoryCoordinator } from "./helpers/in-memory-coordinator.ts"

/**
 * Regression cover for the production incident of 2026-08-26: two deployables
 * sharing ONE connection string contended for the single `pathway-cluster-leader`
 * lease. The loser stayed a worker forever and its pump never started.
 */

/** Wraps a coordinator so it advertises its own namespaced lease key. */
function withLeaseKey(coordinator: PathwayCoordinator, leaseKey: string): PathwayCoordinator {
  return {
    leaseKey,
    acquireLease: (id, key, ttl) => coordinator.acquireLease(id, key, ttl),
    renewLease: (id, key, ttl) => coordinator.renewLease(id, key, ttl),
    releaseLease: (id, key) => coordinator.releaseLease(id, key),
    register: (id, address) => coordinator.register(id, address),
    heartbeat: (id) => coordinator.heartbeat(id),
    unregister: (id) => coordinator.unregister(id),
    getInstances: (stale) => coordinator.getInstances(stale),
  }
}

Deno.test({
  name: "Cluster state prefix — lease key isolation",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: async (t) => {
    await t.step("no prefix keeps the historical lease key", () => {
      const manager = new ClusterManager({
        coordinator: new InMemoryCoordinator(),
        advertisedAddress: "ws://localhost:19201",
        port: 19201,
      })
      assertEquals(manager.currentLeaseKey, "pathway-cluster-leader")
    })

    await t.step("statePrefix namespaces the lease key", () => {
      const manager = new ClusterManager({
        coordinator: new InMemoryCoordinator(),
        advertisedAddress: "ws://localhost:19202",
        port: 19202,
        statePrefix: "compute_api",
      })
      assertEquals(manager.currentLeaseKey, "compute_api_pathway-cluster-leader")
    })

    await t.step("the coordinator's own lease key is used when the options set none", () => {
      const manager = new ClusterManager({
        coordinator: withLeaseKey(new InMemoryCoordinator(), "compute_reconciler_pathway-cluster-leader"),
        advertisedAddress: "ws://localhost:19203",
        port: 19203,
      })
      assertEquals(manager.currentLeaseKey, "compute_reconciler_pathway-cluster-leader")
    })

    await t.step("an explicit leaseKey wins over statePrefix and the coordinator", () => {
      const manager = new ClusterManager({
        coordinator: withLeaseKey(new InMemoryCoordinator(), "from_coordinator"),
        advertisedAddress: "ws://localhost:19204",
        port: 19204,
        statePrefix: "from_prefix",
        leaseKey: "explicit-key",
      })
      assertEquals(manager.currentLeaseKey, "explicit-key")
    })

    await t.step("an invalid statePrefix is rejected at construction", () => {
      let threw = false
      try {
        new ClusterManager({
          coordinator: new InMemoryCoordinator(),
          advertisedAddress: "ws://localhost:19205",
          port: 19205,
          statePrefix: "compute-api",
        })
      } catch (err) {
        threw = true
        assertEquals((err as Error).message.includes("Invalid statePrefix"), true)
      }
      assertEquals(threw, true)
    })
  },
})

Deno.test({
  name: "Cluster state prefix — two prefixes on one coordinator do not contend",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: async (t) => {
    await t.step("both instances become leader and both signal leadership", async () => {
      // ONE coordinator stands in for ONE shared database.
      const coordinator = new InMemoryCoordinator()

      const api = new ClusterManager({
        coordinator,
        advertisedAddress: "ws://localhost:19211",
        port: 19211,
        statePrefix: "compute_api",
        leaseTtlMs: 5000,
        leaseRenewIntervalMs: 2000,
        heartbeatIntervalMs: 1000,
      })
      const reconciler = new ClusterManager({
        coordinator,
        advertisedAddress: "ws://localhost:19212",
        port: 19212,
        statePrefix: "compute_reconciler",
        leaseTtlMs: 5000,
        leaseRenewIntervalMs: 2000,
        heartbeatIntervalMs: 1000,
      })

      // `onLeadershipChange(true)` is what makes PathwaysBuilder start the pump.
      const leadershipSignals: string[] = []
      api.onLeadershipChange((isLeader) => isLeader && leadershipSignals.push("compute_api"))
      reconciler.onLeadershipChange((isLeader) => isLeader && leadershipSignals.push("compute_reconciler"))

      api.setEventHandler(() => Promise.resolve())
      reconciler.setEventHandler(() => Promise.resolve())

      try {
        await api.start()
        await reconciler.start()
        await new Promise((r) => setTimeout(r, 500))

        assertEquals(api.isLeader, true, "compute_api must lead its own cluster")
        assertEquals(reconciler.isLeader, true, "compute_reconciler must lead its own cluster")
        assertEquals(leadershipSignals.sort(), ["compute_api", "compute_reconciler"])
      } finally {
        await api.stop()
        await reconciler.stop()
      }
    })

    await t.step("without a prefix the two instances still contend, as before", async () => {
      const coordinator = new InMemoryCoordinator()

      const first = new ClusterManager({
        coordinator,
        advertisedAddress: "ws://localhost:19213",
        port: 19213,
        leaseTtlMs: 30000,
        leaseRenewIntervalMs: 10000,
        heartbeatIntervalMs: 1000,
      })
      const second = new ClusterManager({
        coordinator,
        advertisedAddress: "ws://localhost:19214",
        port: 19214,
        leaseTtlMs: 30000,
        leaseRenewIntervalMs: 10000,
        heartbeatIntervalMs: 1000,
      })

      first.setEventHandler(() => Promise.resolve())
      second.setEventHandler(() => Promise.resolve())

      try {
        await first.start()
        await new Promise((r) => setTimeout(r, 200))
        await second.start()
        await new Promise((r) => setTimeout(r, 300))

        assertEquals(first.isLeader, true)
        assertEquals(second.isWorker, true)
        assertEquals(first.currentLeaseKey, second.currentLeaseKey)
      } finally {
        await first.stop()
        await second.stop()
      }
    })
  },
})
