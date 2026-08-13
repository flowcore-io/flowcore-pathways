import type { PathwayCoordinator } from "../../src/pathways/cluster/types.ts"

/**
 * In-memory coordinator for testing
 */
export class InMemoryCoordinator implements PathwayCoordinator {
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
