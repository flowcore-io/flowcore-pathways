import { DEFAULT_STATE_NAMES, prefixStateName, type StatePrefixConfig } from "../state-prefix.ts"
import type { PathwayDeliveryStore } from "../types.ts"
import type { PostgresAdapter, PostgresPoolConfig } from "./postgres-adapter.ts"
import { createPostgresAdapter } from "./postgres-adapter.ts"

/** Configuration for the PostgreSQL delivery store using a connection string */
export interface PostgresPathwayDeliveryStoreConnectionStringConfig extends StatePrefixConfig {
  /** Complete PostgreSQL connection string */
  connectionString: string

  /** These properties are not used when a connection string is provided */
  host?: never
  port?: never
  user?: never
  password?: never
  database?: never
  ssl?: never

  /** Explicit table name. Overrides `statePrefix`. Default: `"pathway_delivery_state"`. */
  tableName?: string
  /** Connection pool configuration */
  pool?: PostgresPoolConfig
}

/** Configuration for the PostgreSQL delivery store using individual parameters */
export interface PostgresPathwayDeliveryStoreParametersConfig extends StatePrefixConfig {
  /** Not used when individual parameters are provided */
  connectionString?: never

  /** PostgreSQL server hostname */
  host: string
  /** PostgreSQL server port */
  port: number
  /** PostgreSQL username */
  user: string
  /** PostgreSQL password */
  password: string
  /** PostgreSQL database name */
  database: string
  /** Whether to use SSL for the connection */
  ssl?: boolean

  /** Explicit table name. Overrides `statePrefix`. Default: `"pathway_delivery_state"`. */
  tableName?: string
  /** Connection pool configuration */
  pool?: PostgresPoolConfig
}

/** Configuration options for the PostgreSQL delivery store */
export type PostgresPathwayDeliveryStoreConfig =
  | PostgresPathwayDeliveryStoreConnectionStringConfig
  | PostgresPathwayDeliveryStoreParametersConfig

interface DeliveryStateRow {
  paused_pumps: string[] | null
}

/**
 * PostgreSQL implementation of {@link PathwayDeliveryStore}.
 *
 * Holds the delivery pause of a pathway so it outlives the process. Without a durable
 * store a redeploy, a pod restart or a cluster leader change brings the pathway back
 * delivering, silently, while the operator still sees "paused".
 *
 * The row is shared by every instance of a deployable, so a new cluster leader reads the
 * same pause the old leader wrote.
 *
 * @example
 * ```typescript
 * const deliveryStore = createPostgresPathwayDeliveryStore({
 *   connectionString: "postgres://user:password@localhost:5432/mydb",
 *   statePrefix: "compute_api", // optional, yields compute_api_pathway_delivery_state
 * })
 *
 * pathways.withPathwayDeliveryStore(deliveryStore)
 * ```
 */
export class PostgresPathwayDeliveryStore implements PathwayDeliveryStore {
  private static readonly DEFAULT_TABLE_NAME = DEFAULT_STATE_NAMES.deliveryState

  private postgres: PostgresAdapter | null = null
  private readonly tableName: string
  private initialized = false

  constructor(private config: PostgresPathwayDeliveryStoreConfig) {
    this.tableName = config.tableName ||
      prefixStateName(config.statePrefix, PostgresPathwayDeliveryStore.DEFAULT_TABLE_NAME)
  }

  /** The resolved table name */
  get table(): string {
    return this.tableName
  }

  private async initialize(): Promise<PostgresAdapter> {
    if (this.initialized && this.postgres) {
      return this.postgres
    }

    if ("connectionString" in this.config && this.config.connectionString) {
      this.postgres = await createPostgresAdapter({
        connectionString: this.config.connectionString,
        pool: this.config.pool,
      })
    } else {
      this.postgres = await createPostgresAdapter({
        host: this.config.host as string,
        port: this.config.port as number,
        user: this.config.user as string,
        password: this.config.password as string,
        database: this.config.database as string,
        ssl: this.config.ssl,
        pool: this.config.pool,
      })
    }

    await this.postgres.execute(`
      CREATE TABLE IF NOT EXISTS ${this.tableName} (
        pathway_key TEXT PRIMARY KEY,
        paused_pumps TEXT[] NOT NULL DEFAULT '{}',
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `)

    this.initialized = true
    return this.postgres
  }

  async getPausedPumps(pathwayKey: string): Promise<string[]> {
    const postgres = await this.initialize()
    const rows = await postgres.query<DeliveryStateRow[]>(
      `SELECT paused_pumps FROM ${this.tableName} WHERE pathway_key = $1`,
      [pathwayKey],
    )
    return rows[0]?.paused_pumps ?? []
  }

  async setPausedPumps(pathwayKey: string, pumpKeys: string[]): Promise<void> {
    const postgres = await this.initialize()
    if (!pumpKeys.length) {
      await postgres.execute(`DELETE FROM ${this.tableName} WHERE pathway_key = $1`, [pathwayKey])
      return
    }
    await postgres.execute(
      `INSERT INTO ${this.tableName} (pathway_key, paused_pumps, updated_at)
       VALUES ($1, $2, NOW())
       ON CONFLICT (pathway_key)
       DO UPDATE SET paused_pumps = EXCLUDED.paused_pumps, updated_at = NOW()`,
      [pathwayKey, pumpKeys],
    )
  }

  /** Close the underlying connection pool */
  async close(): Promise<void> {
    await this.postgres?.disconnect()
    this.postgres = null
    this.initialized = false
  }
}

/** Creates a {@link PostgresPathwayDeliveryStore} */
export function createPostgresPathwayDeliveryStore(
  config: PostgresPathwayDeliveryStoreConfig,
): PostgresPathwayDeliveryStore {
  return new PostgresPathwayDeliveryStore(config)
}
