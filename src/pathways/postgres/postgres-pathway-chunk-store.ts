import { sha256Hex } from "../chunking.ts"
import { DEFAULT_STATE_NAMES, prefixStateName, type StatePrefixConfig } from "../state-prefix.ts"
import type { PathwayChunkStore, PathwayChunkStoreResult, StorePathwayChunkPartInput } from "../types.ts"
import type { PostgresAdapter, PostgresPoolConfig } from "./postgres-adapter.ts"
import { createPostgresAdapter } from "./postgres-adapter.ts"

/**
 * Configuration for the PostgreSQL chunk store using a connection string
 */
export interface PostgresPathwayChunkStoreConnectionStringConfig extends StatePrefixConfig {
  /** Complete PostgreSQL connection string */
  connectionString: string

  /** These properties are not used when a connection string is provided */
  host?: never
  port?: never
  user?: never
  password?: never
  database?: never
  ssl?: never

  /** Explicit table name. Overrides `statePrefix`. Default: `"pathway_chunks"`. */
  tableName?: string
  /** Time-to-live in milliseconds for parts of an incomplete chunk (default: 1 hour) */
  ttlMs?: number
  /**
   * Minimum time between two sweeps of expired rows, in milliseconds.
   * Defaults to 60 000. `0` sweeps on every `storePart` call (the pre-2.10.1 behaviour).
   */
  cleanupIntervalMs?: number
  /** Connection pool configuration */
  pool?: PostgresPoolConfig
}

/**
 * Configuration for the PostgreSQL chunk store using individual parameters
 */
export interface PostgresPathwayChunkStoreParametersConfig extends StatePrefixConfig {
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

  /** Explicit table name. Overrides `statePrefix`. Default: `"pathway_chunks"`. */
  tableName?: string
  /** Time-to-live in milliseconds for parts of an incomplete chunk (default: 1 hour) */
  ttlMs?: number
  /**
   * Minimum time between two sweeps of expired rows, in milliseconds.
   * Defaults to 60 000. `0` sweeps on every `storePart` call (the pre-2.10.1 behaviour).
   */
  cleanupIntervalMs?: number
  /** Connection pool configuration */
  pool?: PostgresPoolConfig
}

/**
 * Configuration options for the PostgreSQL chunk store
 */
export type PostgresPathwayChunkStoreConfig =
  | PostgresPathwayChunkStoreConnectionStringConfig
  | PostgresPathwayChunkStoreParametersConfig

interface ChunkRow {
  part: number
  total_parts: number
  digest: string
  data: string
  event_id: string
  assembled_at: string | null
}

/**
 * PostgreSQL implementation of {@link PathwayChunkStore}.
 *
 * Parts of one oversized event are collected in a shared table so any instance
 * can receive any part. Assembly is serialized per chunk with a transaction-scoped
 * advisory lock, so exactly one caller observes `complete`.
 *
 * - Exact replays of a part are accepted and reported as `duplicate`
 * - A part with the same key but different data is rejected with an error
 * - Parts of incomplete chunks expire after `ttlMs` and are removed on the next write
 *
 * @example
 * ```typescript
 * const chunkStore = createPostgresPathwayChunkStore({
 *   connectionString: "postgres://user:password@localhost:5432/mydb",
 *   statePrefix: "compute_api", // optional, yields compute_api_pathway_chunks
 * })
 *
 * pathways.withPathwayChunkStore(chunkStore)
 * ```
 */
export class PostgresPathwayChunkStore implements PathwayChunkStore {
  /** Default time-to-live for parts of an incomplete chunk (1 hour) */
  private static readonly DEFAULT_TTL_MS = 60 * 60 * 1000
  private static readonly DEFAULT_CLEANUP_INTERVAL_MS = 60 * 1000
  private static readonly DEFAULT_TABLE_NAME = DEFAULT_STATE_NAMES.chunks

  private postgres: PostgresAdapter | null = null
  private readonly tableName: string
  private readonly ttlMs: number
  private readonly cleanupIntervalMs: number
  private lastCleanupAt = 0
  private initialized = false

  constructor(private config: PostgresPathwayChunkStoreConfig) {
    this.tableName = config.tableName ||
      prefixStateName(config.statePrefix, PostgresPathwayChunkStore.DEFAULT_TABLE_NAME)
    this.ttlMs = config.ttlMs || PostgresPathwayChunkStore.DEFAULT_TTL_MS
    this.cleanupIntervalMs = config.cleanupIntervalMs ?? PostgresPathwayChunkStore.DEFAULT_CLEANUP_INTERVAL_MS
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

    if (!this.postgres.transaction) {
      throw new Error("PostgresPathwayChunkStore requires a PostgresAdapter with transaction support")
    }

    await this.postgres.execute(`
      CREATE TABLE IF NOT EXISTS ${this.tableName} (
        chunk_id TEXT NOT NULL,
        part INTEGER NOT NULL,
        total_parts INTEGER NOT NULL,
        digest TEXT NOT NULL,
        event_id TEXT NOT NULL,
        data TEXT NOT NULL,
        data_hash TEXT NOT NULL,
        assembled_at TIMESTAMP WITH TIME ZONE,
        created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
        expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
        PRIMARY KEY (chunk_id, part)
      )
    `)
    await this.postgres.execute(`
      CREATE INDEX IF NOT EXISTS ${this.tableName}_expires_at_idx ON ${this.tableName} (expires_at)
    `)

    this.initialized = true
    return this.postgres
  }

  /**
   * Records one part under a per-chunk advisory lock and reports completion
   * @param input The part to record
   * @returns The store result; exactly one caller per chunk receives `complete`
   * @throws {Error} When the same part arrives with different data
   */
  async storePart(input: StorePathwayChunkPartInput): Promise<PathwayChunkStoreResult> {
    const postgres = await this.initialize()
    await this.cleanupExpiredIfDue(postgres)

    const dataHash = sha256Hex(input.data)
    const ttlSeconds = Math.max(1, Math.floor(this.ttlMs / 1000))

    return await postgres.transaction!(async (tx) => {
      // 64-bit hash: collisions only serialize unrelated chunks, but fewer is better.
      await tx.execute(`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, [input.chunkId])

      // Expired parts of this chunk never count toward completion, whether or not the
      // table-wide sweep has run yet. This is a primary-key range delete, so it is cheap.
      await tx.execute(`DELETE FROM ${this.tableName} WHERE chunk_id = $1 AND expires_at < NOW()`, [
        input.chunkId,
      ])

      const inserted = await tx.query<{ part: number }[]>(
        `
        INSERT INTO ${this.tableName}
          (chunk_id, part, total_parts, digest, event_id, data, data_hash, expires_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, NOW() + interval '${ttlSeconds} seconds')
        ON CONFLICT (chunk_id, part) DO NOTHING
        RETURNING part
      `,
        [input.chunkId, input.part, input.totalParts, input.digest, input.eventId, input.data, dataHash],
      )

      let duplicate = false
      if (inserted.length === 0) {
        const existing = await tx.query<{ data_hash: string }[]>(
          `SELECT data_hash FROM ${this.tableName} WHERE chunk_id = $1 AND part = $2`,
          [input.chunkId, input.part],
        )
        if (existing[0]?.data_hash !== dataHash) {
          throw new Error(`Conflicting pathway chunk part ${input.part} for chunk ${input.chunkId}`)
        }
        duplicate = true
      }

      const rows = await tx.query<ChunkRow[]>(
        `SELECT part, total_parts, digest, data, event_id, assembled_at
         FROM ${this.tableName} WHERE chunk_id = $1 ORDER BY part`,
        [input.chunkId],
      )

      if (rows.some((row) => row.total_parts !== input.totalParts || row.digest !== input.digest)) {
        throw new Error(
          `Inconsistent pathway chunk part ${input.part} for chunk ${input.chunkId} (digest or part count)`,
        )
      }

      if (rows.some((row) => row.assembled_at !== null)) {
        return { status: "duplicate" }
      }
      if (rows.length < input.totalParts) {
        return { status: duplicate ? "duplicate" : "stored" }
      }

      await tx.execute(`UPDATE ${this.tableName} SET assembled_at = NOW() WHERE chunk_id = $1`, [input.chunkId])
      return {
        status: "complete",
        parts: rows.map((row) => row.data),
        partEventIds: rows.map((row) => row.event_id),
      }
    })
  }

  /**
   * Removes every part of a chunk
   * @param chunkId The chunk to remove
   */
  async deleteChunk(chunkId: string): Promise<void> {
    const postgres = await this.initialize()
    await postgres.execute(`DELETE FROM ${this.tableName} WHERE chunk_id = $1`, [chunkId])
  }

  /**
   * Closes the database connection
   */
  async close(): Promise<void> {
    if (this.postgres) {
      await this.postgres.disconnect()
      this.postgres = null
      this.initialized = false
    }
  }

  private async cleanupExpiredIfDue(postgres: PostgresAdapter): Promise<void> {
    const now = Date.now()
    if (this.cleanupIntervalMs > 0 && now - this.lastCleanupAt < this.cleanupIntervalMs) {
      return
    }
    // Claim the slot before awaiting so concurrent callers on this instance do not all sweep.
    this.lastCleanupAt = now
    await this.cleanupExpired(postgres)
  }

  private async cleanupExpired(postgres: PostgresAdapter): Promise<void> {
    await postgres.execute(`DELETE FROM ${this.tableName} WHERE expires_at < NOW()`)
  }
}

/**
 * Creates a PostgreSQL chunk store
 * @param config The PostgreSQL configuration
 * @returns A new {@link PostgresPathwayChunkStore}
 */
export function createPostgresPathwayChunkStore(config: PostgresPathwayChunkStoreConfig): PostgresPathwayChunkStore {
  return new PostgresPathwayChunkStore(config)
}
