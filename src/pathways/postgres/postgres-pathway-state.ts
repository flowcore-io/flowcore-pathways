import type { PathwayState } from "../types.ts";
import type { PostgresAdapter } from "./postgres-adapter.ts";
import { createPostgresAdapter } from "./postgres-adapter.ts";

/**
 * Configuration options for PostgreSQL pathway state storage
 * 
 * @interface PostgresPathwayStateConfig
 */
export interface PostgresPathwayStateConfig {
  /** PostgreSQL server hostname */
  host: string;
  /** PostgreSQL server port */
  port: number;
  /** PostgreSQL username */
  user: string;
  /** PostgreSQL password */
  password: string;
  /** PostgreSQL database name */
  database: string;
  /** Whether to use SSL for the connection */
  ssl?: boolean;
  /** Table name for storing pathway state (default: "pathway_state") */
  tableName?: string;
  /** Time-to-live in milliseconds for processed events (default: 5 minutes) */
  ttlMs?: number;
}

/**
 * Implementation of PathwayState that uses PostgreSQL for storage
 * 
 * This class provides persistent storage of pathway state using a PostgreSQL database,
 * which allows for state to be shared across multiple instances of the application.
 * 
 * @implements {PathwayState}
 */
export class PostgresPathwayState implements PathwayState {
  /**
   * Default time-to-live for processed event records (5 minutes)
   * @private
   */
  private static readonly DEFAULT_TTL_MS = 5 * 60 * 1000; // 5 minutes
  
  /**
   * Default table name for storing pathway state
   * @private
   */
  private static readonly DEFAULT_TABLE_NAME = "pathway_state";

  /**
   * The PostgreSQL adapter instance
   * @private
   */
  private postgres: PostgresAdapter;
  
  /**
   * The table name for storing pathway state
   * @private
   */
  private tableName: string;
  
  /**
   * Time-to-live in milliseconds for processed events
   * @private
   */
  private ttlMs: number;
  
  /**
   * Whether the database has been initialized
   * @private
   */
  private initialized = false;

  /**
   * Creates a new PostgresPathwayState instance
   * 
   * @param {PostgresPathwayStateConfig} config The PostgreSQL configuration
   */
  constructor(private config: PostgresPathwayStateConfig) {
    this.tableName = config.tableName || PostgresPathwayState.DEFAULT_TABLE_NAME;
    this.ttlMs = config.ttlMs || PostgresPathwayState.DEFAULT_TTL_MS;
    this.postgres = null as unknown as PostgresAdapter;
  }

  /**
   * Initializes the PostgreSQL connection and creates the necessary table and index
   * 
   * @private
   * @returns {Promise<void>}
   */
  private async initialize(): Promise<void> {
    if (this.initialized) {
      return;
    }

    this.postgres = await createPostgresAdapter({
      host: this.config.host,
      port: this.config.port,
      user: this.config.user,
      password: this.config.password,
      database: this.config.database,
      ssl: this.config.ssl,
    });

    // Create table if it doesn't exist
    await this.postgres.execute(`
      CREATE TABLE IF NOT EXISTS ${this.tableName} (
        event_id TEXT PRIMARY KEY,
        processed BOOLEAN NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        expires_at TIMESTAMP WITH TIME ZONE NOT NULL
      )
    `);

    // Create index on expires_at to help with cleanup
    await this.postgres.execute(`
      CREATE INDEX IF NOT EXISTS ${this.tableName}_expires_at_idx ON ${this.tableName} (expires_at)
    `);

    this.initialized = true;
  }

  /**
   * Checks if an event has already been processed
   * 
   * @param {string} eventId - The ID of the event to check
   * @returns {Promise<boolean>} True if the event has been processed, false otherwise
   */
  async isProcessed(eventId: string): Promise<boolean> {
    await this.initialize();

    // Clean up expired entries
    await this.cleanupExpired();

    const result = await this.postgres.query<{ processed: boolean }[]>(`
      SELECT processed FROM ${this.tableName}
      WHERE event_id = $1 AND expires_at > NOW()
    `, [eventId]);

    return result.length > 0 && result[0].processed;
  }

  /**
   * Marks an event as processed
   * 
   * @param {string} eventId - The ID of the event to mark as processed
   * @returns {Promise<void>}
   */
  async setProcessed(eventId: string): Promise<void> {
    await this.initialize();

    // Insert or update the event state
    // Using ON CONFLICT to handle the case where the event is already in the table
    await this.postgres.execute(`
      INSERT INTO ${this.tableName} (event_id, processed, expires_at)
      VALUES ($1, TRUE, NOW() + interval '${Math.floor(this.ttlMs / 1000)} seconds')
      ON CONFLICT (event_id) 
      DO UPDATE SET 
        processed = TRUE, 
        expires_at = NOW() + interval '${Math.floor(this.ttlMs / 1000)} seconds'
    `, [eventId]);
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
    `);
  }

  /**
   * Closes the PostgreSQL connection
   * 
   * @returns {Promise<void>}
   */
  async close(): Promise<void> {
    if (this.postgres) {
      await this.postgres.disconnect();
    }
  }
}

/**
 * Creates a new PostgreSQL pathway state instance
 * 
 * @param {PostgresPathwayStateConfig} config The PostgreSQL configuration
 * @returns {PostgresPathwayState} A new PostgresPathwayState instance
 */
export function createPostgresPathwayState(config: PostgresPathwayStateConfig): PostgresPathwayState {
  const state = new PostgresPathwayState(config);
  return state;
} 