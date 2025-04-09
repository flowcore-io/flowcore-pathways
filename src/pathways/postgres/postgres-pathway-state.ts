import type { PathwayState } from "../types.ts"
import type { PostgresAdapter } from "./postgres-adapter.ts"
import { createPostgresAdapter } from "./postgres-adapter.ts"

/**
 * Configuration for PostgreSQL pathway state storage using a connection string
 */
export interface PostgresPathwayStateConnectionStringConfig {
  /** Complete PostgreSQL connection string (e.g., postgres://user:password@host:port/database?sslmode=require) */
  connectionString: string

  /** These properties are not used when a connection string is provided */
  host?: never
  port?: never
  user?: never
  password?: never
  database?: never
  ssl?: never

  /** Table name for storing pathway state (default: "pathway_state") */
  tableName?: string
  /** Time-to-live in milliseconds for processed events (default: 5 minutes) */
  ttlMs?: number
}

/**
 * Configuration for PostgreSQL pathway state storage using individual parameters
 */
export interface PostgresPathwayStateParametersConfig {
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

  /** Table name for storing pathway state (default: "pathway_state") */
  tableName?: string
  /** Time-to-live in milliseconds for processed events (default: 5 minutes) */
  ttlMs?: number
}

/**
 * Configuration options for PostgreSQL pathway state storage
 *
 * Can provide either:
 * 1. A complete connection string, or
 * 2. Individual connection parameters (host, port, user, etc.)
 */
export type PostgresPathwayStateConfig =
  | PostgresPathwayStateConnectionStringConfig
  | PostgresPathwayStateParametersConfig

/**
 * Implementation of PathwayState that uses PostgreSQL for storage
 *
 * This class provides persistent storage of pathway state using a PostgreSQL database,
 * which allows for state to be shared across multiple instances of the application.
 *
 * Key features:
 * - Persistent storage of pathway processing state across application restarts
 * - Automatic table and index creation
 * - Configurable TTL (time-to-live) for processed events
 * - Automatic cleanup of expired records
 * - Support for horizontal scaling across multiple instances
 * - Connection pooling for efficient database usage
 *
 * Use cases:
 * - Production deployments that require durability and persistence
 * - Distributed systems where multiple instances process events
 * - Applications with high reliability requirements
 * - Scenarios where in-memory state is insufficient
 *
 * @example
 * ```typescript
 * // Create PostgreSQL pathway state with connection string
 * const postgresState = createPostgresPathwayState({
 *   connectionString: "postgres://user:password@localhost:5432/mydb",
 *   tableName: "event_processing_state", // Optional
 *   ttlMs: 24 * 60 * 60 * 1000 // 24 hours (optional)
 * });
 *
 * // Or with individual parameters
 * const postgresState = createPostgresPathwayState({
 *   host: "localhost",
 *   port: 5432,
 *   user: "postgres",
 *   password: "postgres",
 *   database: "mydb",
 *   ssl: false,
 *   tableName: "event_processing_state", // Optional
 *   ttlMs: 30 * 60 * 1000 // 30 minutes (optional)
 * });
 *
 * // Use with PathwaysBuilder
 * const pathways = new PathwaysBuilder({
 *   // ... other config
 * }).withPathwayState(postgresState);
 * ```
 */
export class PostgresPathwayState implements PathwayState {
  /**
   * Default time-to-live for processed event records (5 minutes)
   * @private
   */
  private static readonly DEFAULT_TTL_MS = 5 * 60 * 1000 // 5 minutes

  /**
   * Default table name for storing pathway state
   * @private
   */
  private static readonly DEFAULT_TABLE_NAME = "pathway_state"

  /**
   * The PostgreSQL adapter instance
   * @private
   */
  private postgres: PostgresAdapter

  /**
   * The table name for storing pathway state
   * @private
   */
  private tableName: string

  /**
   * Time-to-live in milliseconds for processed events
   * @private
   */
  private ttlMs: number

  /**
   * Whether the database has been initialized
   * @private
   */
  private initialized = false

  /**
   * Creates a new PostgresPathwayState instance
   *
   * @param {PostgresPathwayStateConfig} config The PostgreSQL configuration
   */
  constructor(private config: PostgresPathwayStateConfig) {
    this.tableName = config.tableName || PostgresPathwayState.DEFAULT_TABLE_NAME
    this.ttlMs = config.ttlMs || PostgresPathwayState.DEFAULT_TTL_MS
    this.postgres = null as unknown as PostgresAdapter
  }

  /**
   * Initializes the PostgreSQL connection and creates the necessary table and index
   *
   * @private
   * @returns {Promise<void>}
   */
  private async initialize(): Promise<void> {
    if (this.initialized) {
      return
    }

    // Create adapter using either connection string or individual parameters
    if ("connectionString" in this.config && this.config.connectionString) {
      // Use connection string if provided
      this.postgres = await createPostgresAdapter({
        connectionString: this.config.connectionString,
      })
    } else {
      // We know this must be the parameters config due to the type union
      // TypeScript just needs help with narrowing the type
      this.postgres = await createPostgresAdapter({
        host: this.config.host as string,
        port: this.config.port as number,
        user: this.config.user as string,
        password: this.config.password as string,
        database: this.config.database as string,
        ssl: this.config.ssl,
      })
    }

    // Check if the table exists
    const tableExists = await this.postgres.query<{ exists: boolean }[]>(
      `
      SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_name = $1
      )
    `,
      [this.tableName],
    )
    if (!tableExists[0]?.exists) {
      // Create table if it doesn't exist
      await this.postgres.execute(`
        CREATE TABLE IF NOT EXISTS ${this.tableName} (
          event_id TEXT PRIMARY KEY,
          processed BOOLEAN NOT NULL,
          created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
          expires_at TIMESTAMP WITH TIME ZONE NOT NULL
        )
      `)

      // Create index on expires_at to help with cleanup
      await this.postgres.execute(`
        CREATE INDEX IF NOT EXISTS ${this.tableName}_expires_at_idx ON ${this.tableName} (expires_at)
      `)
    }

    this.initialized = true
  }

  /**
   * Checks if an event has already been processed
   *
   * This method checks the PostgreSQL database to determine if an event with the given ID
   * has been marked as processed. If the event exists in the database and is marked as processed,
   * the method returns true.
   *
   * Before performing the check, this method also triggers cleanup of expired event records
   * to maintain database performance.
   *
   * @param {string} eventId - The ID of the event to check
   * @returns {Promise<boolean>} True if the event has been processed, false otherwise
   *
   * @example
   * ```typescript
   * // Check if an event has been processed
   * const processed = await postgresState.isProcessed("event-123");
   * if (processed) {
   *   console.log("Event has already been processed, skipping");
   * } else {
   *   console.log("Processing event for the first time");
   *   // Process the event
   *   await processEvent(event);
   *   // Mark as processed
   *   await postgresState.setProcessed("event-123");
   * }
   * ```
   */
  async isProcessed(eventId: string): Promise<boolean> {
    await this.initialize()

    // Clean up expired entries
    await this.cleanupExpired()

    const result = await this.postgres.query<{ processed: boolean }[]>(
      `
      SELECT processed FROM ${this.tableName}
      WHERE event_id = $1 AND expires_at > NOW()
    `,
      [eventId],
    )

    return result.length > 0 && result[0].processed
  }

  /**
   * Marks an event as processed
   *
   * This method inserts or updates a record in the PostgreSQL database to mark an event
   * as processed. If the event already exists in the database, the record is updated;
   * otherwise, a new record is created.
   *
   * Each processed event is stored with an expiration timestamp based on the configured TTL.
   * After this time elapses, the record may be automatically removed during cleanup operations.
   *
   * @param {string} eventId - The ID of the event to mark as processed
   * @returns {Promise<void>}
   *
   * @example
   * ```typescript
   * // Process an event and mark it as processed
   * async function handleEvent(event) {
   *   // Check if already processed to implement idempotency
   *   if (await postgresState.isProcessed(event.id)) {
   *     return; // Skip already processed events
   *   }
   *
   *   try {
   *     // Process the event
   *     await processEvent(event);
   *
   *     // Mark as processed after successful processing
   *     await postgresState.setProcessed(event.id);
   *   } catch (error) {
   *     console.error("Failed to process event:", error);
   *     // Not marking as processed, so it can be retried
   *   }
   * }
   * ```
   */
  async setProcessed(eventId: string): Promise<void> {
    await this.initialize()

    // Insert or update the event state
    // Using ON CONFLICT to handle the case where the event is already in the table
    await this.postgres.execute(
      `
      INSERT INTO ${this.tableName} (event_id, processed, expires_at)
      VALUES ($1, TRUE, NOW() + interval '${Math.floor(this.ttlMs / 1000)} seconds')
      ON CONFLICT (event_id)
      DO UPDATE SET
        processed = TRUE,
        expires_at = NOW() + interval '${Math.floor(this.ttlMs / 1000)} seconds'
    `,
      [eventId],
    )
  }

  /**
   * Removes expired event records from the database
   *
   * @private
   * @returns {Promise<void>}
   */
  private async cleanupExpired(): Promise<void> {
    // Delete expired entries
    await this.postgres.execute(`
      DELETE FROM ${this.tableName}
      WHERE expires_at < NOW()
    `)
  }

  /**
   * Closes the PostgreSQL connection
   *
   * @returns {Promise<void>}
   */
  async close(): Promise<void> {
    if (this.postgres) {
      await this.postgres.disconnect()
    }
  }
}

/**
 * Creates a new PostgreSQL pathway state instance
 *
 * This is a factory function that simplifies the creation of PostgresPathwayState instances.
 * It accepts either a connection string or individual connection parameters, along with
 * optional configuration for table name and TTL.
 *
 * The PostgresPathwayState is lazily initialized, meaning the database connection and
 * table creation only happen when the first operation is performed. This makes it safe
 * to create instances early in the application lifecycle.
 *
 * @param config The PostgreSQL configuration (connection string or parameters)
 * @returns A new PostgresPathwayState instance
 *
 * @example
 * ```typescript
 * // With connection string
 * const state = createPostgresPathwayState({
 *   connectionString: "postgres://user:pass@localhost:5432/db?sslmode=require"
 * });
 *
 * // With individual parameters
 * const state = createPostgresPathwayState({
 *   host: "localhost",
 *   port: 5432,
 *   user: "postgres",
 *   password: "secret",
 *   database: "events_db",
 *   ssl: true
 * });
 *
 * // With custom table name and TTL
 * const state = createPostgresPathwayState({
 *   connectionString: "postgres://user:pass@localhost:5432/db",
 *   tableName: "my_custom_event_state",
 *   ttlMs: 7 * 24 * 60 * 60 * 1000 // 1 week
 * });
 *
 * // Use with PathwaysBuilder
 * const pathways = new PathwaysBuilder({
 *   // Other config
 * }).withPathwayState(state);
 * ```
 */
export function createPostgresPathwayState(config: PostgresPathwayStateConfig): PostgresPathwayState {
  const state = new PostgresPathwayState(config)
  return state
}
