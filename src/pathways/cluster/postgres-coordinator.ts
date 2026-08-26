import type { PostgresAdapter, PostgresConfig } from "../postgres/index.ts"
import { PostgresJsAdapter } from "../postgres/index.ts"
import { DEFAULT_STATE_NAMES, prefixStateName, type StatePrefixConfig } from "../state-prefix.ts"
import type { PathwayCoordinator } from "./types.ts"

/**
 * Options for {@link PostgresPathwayCoordinator}.
 *
 * Set `statePrefix` when several deployables share ONE connection string. An
 * explicit `leasesTable` or `instancesTable` always wins over the prefix.
 */
export interface PostgresPathwayCoordinatorOptions extends StatePrefixConfig {
  /** Explicit lease table name. Overrides `statePrefix`. Default: `"pathway_leases"`. */
  leasesTable?: string
  /** Explicit instance table name. Overrides `statePrefix`. Default: `"pathway_instances"`. */
  instancesTable?: string
}

/**
 * PostgreSQL-backed implementation of PathwayCoordinator.
 * Uses two tables:
 * - `pathway_leases`: distributed locks for leader election
 * - `pathway_instances`: instance registration and heartbeating
 *
 * Both table names, and the leader lease key, are namespaced by the optional
 * `statePrefix`. Without a prefix the historical names are used unchanged.
 */
export class PostgresPathwayCoordinator implements PathwayCoordinator {
  private adapter: PostgresAdapter
  private initialized = false
  private readonly leasesTable: string
  private readonly instancesTable: string

  /**
   * Leader lease key for this coordinator, namespaced by `statePrefix`.
   *
   * `ClusterManager` reads this so a prefix set here alone is enough — the same
   * value does not have to be repeated in the cluster options.
   */
  readonly leaseKey: string

  constructor(adapter: PostgresAdapter, options?: PostgresPathwayCoordinatorOptions) {
    this.adapter = adapter
    this.leasesTable = options?.leasesTable ?? prefixStateName(options?.statePrefix, DEFAULT_STATE_NAMES.leases)
    this.instancesTable = options?.instancesTable ??
      prefixStateName(options?.statePrefix, DEFAULT_STATE_NAMES.instances)
    this.leaseKey = prefixStateName(options?.statePrefix, DEFAULT_STATE_NAMES.leaseKey)
  }

  private async ensureInitialized(): Promise<void> {
    if (this.initialized) return
    await this.adapter.execute(`
      CREATE TABLE IF NOT EXISTS ${this.leasesTable} (
        key TEXT PRIMARY KEY,
        instance_id TEXT NOT NULL,
        expires_at TIMESTAMPTZ NOT NULL
      )
    `)
    await this.adapter.execute(`
      CREATE TABLE IF NOT EXISTS ${this.instancesTable} (
        instance_id TEXT PRIMARY KEY,
        address TEXT NOT NULL,
        last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `)
    this.initialized = true
  }

  async acquireLease(instanceId: string, key: string, ttlMs: number): Promise<boolean> {
    await this.ensureInitialized()
    const expiresAt = new Date(Date.now() + ttlMs).toISOString()

    // Try to insert or update if expired
    const result = await this.adapter.query<Array<{ acquired: boolean }>>(
      `INSERT INTO ${this.leasesTable} (key, instance_id, expires_at)
       VALUES ($1, $2, $3::timestamptz)
       ON CONFLICT (key) DO UPDATE
       SET instance_id = $2, expires_at = $3::timestamptz
       WHERE ${this.leasesTable}.expires_at < NOW()
          OR ${this.leasesTable}.instance_id = $2
       RETURNING TRUE as acquired`,
      [key, instanceId, expiresAt],
    )

    return Array.isArray(result) && result.length > 0
  }

  async renewLease(instanceId: string, key: string, ttlMs: number): Promise<boolean> {
    await this.ensureInitialized()
    const expiresAt = new Date(Date.now() + ttlMs).toISOString()

    const result = await this.adapter.query<Array<{ renewed: boolean }>>(
      `UPDATE ${this.leasesTable}
       SET expires_at = $1::timestamptz
       WHERE key = $2 AND instance_id = $3
       RETURNING TRUE as renewed`,
      [expiresAt, key, instanceId],
    )

    return Array.isArray(result) && result.length > 0
  }

  async releaseLease(instanceId: string, key: string): Promise<void> {
    await this.ensureInitialized()
    await this.adapter.execute(
      `DELETE FROM ${this.leasesTable} WHERE key = $1 AND instance_id = $2`,
      [key, instanceId],
    )
  }

  async register(instanceId: string, address: string): Promise<void> {
    await this.ensureInitialized()
    await this.adapter.execute(
      `INSERT INTO ${this.instancesTable} (instance_id, address, last_heartbeat)
       VALUES ($1, $2, NOW())
       ON CONFLICT (instance_id) DO UPDATE
       SET address = $2, last_heartbeat = NOW()`,
      [instanceId, address],
    )
  }

  async heartbeat(instanceId: string): Promise<void> {
    await this.ensureInitialized()
    await this.adapter.execute(
      `UPDATE ${this.instancesTable} SET last_heartbeat = NOW() WHERE instance_id = $1`,
      [instanceId],
    )
  }

  async unregister(instanceId: string): Promise<void> {
    await this.ensureInitialized()
    await this.adapter.execute(
      `DELETE FROM ${this.instancesTable} WHERE instance_id = $1`,
      [instanceId],
    )
  }

  async getInstances(staleThresholdMs: number): Promise<Array<{ instanceId: string; address: string }>> {
    await this.ensureInitialized()
    const result = await this.adapter.query<Array<{ instance_id: string; address: string }>>(
      `SELECT instance_id, address FROM ${this.instancesTable}
       WHERE last_heartbeat > NOW() - INTERVAL '1 millisecond' * $1`,
      [staleThresholdMs],
    )

    if (!Array.isArray(result)) return []
    return result.map((row) => ({
      instanceId: row.instance_id,
      address: row.address,
    }))
  }
}

/**
 * Factory function to create a PostgresPathwayCoordinator
 *
 * @param config PostgreSQL connection configuration
 * @param options Table naming options, including the optional `statePrefix`
 *
 * @example
 * ```typescript
 * // Two deployables that share one connection string
 * const coordinator = await createPostgresPathwayCoordinator(
 *   { connectionString: process.env.DATABASE_URL! },
 *   { statePrefix: "compute_api" },
 * )
 * ```
 */
export async function createPostgresPathwayCoordinator(
  config: PostgresConfig,
  options?: PostgresPathwayCoordinatorOptions,
): Promise<PostgresPathwayCoordinator> {
  const adapter = new PostgresJsAdapter(config)
  await adapter.connect()
  return new PostgresPathwayCoordinator(adapter, options)
}
