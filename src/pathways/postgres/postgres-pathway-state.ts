import type { PathwayState } from "../types.ts";
import type { PostgresAdapter } from "./postgres-adapter.ts";
import { createPostgresAdapter } from "./postgres-adapter.ts";

export interface PostgresPathwayStateConfig {
  host: string;
  port: number;
  user: string;
  password: string;
  database: string;
  ssl?: boolean;
  tableName?: string;
  ttlMs?: number;
}

export class PostgresPathwayState implements PathwayState {
  private static readonly DEFAULT_TTL_MS = 5 * 60 * 1000; // 5 minutes
  private static readonly DEFAULT_TABLE_NAME = "pathway_state";

  private postgres: PostgresAdapter;
  private tableName: string;
  private ttlMs: number;
  private initialized = false;

  constructor(private config: PostgresPathwayStateConfig) {
    this.tableName = config.tableName || PostgresPathwayState.DEFAULT_TABLE_NAME;
    this.ttlMs = config.ttlMs || PostgresPathwayState.DEFAULT_TTL_MS;
    this.postgres = null as unknown as PostgresAdapter;
  }

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

  private async cleanupExpired(): Promise<void> {
    // Delete expired entries
    await this.postgres.execute(`
      DELETE FROM ${this.tableName}
      WHERE expires_at < NOW()
    `);
  }

  async close(): Promise<void> {
    if (this.postgres) {
      await this.postgres.disconnect();
    }
  }
}

// Factory function for creating a PostgreSQL pathway state
export function createPostgresPathwayState(config: PostgresPathwayStateConfig): PostgresPathwayState {
  const state = new PostgresPathwayState(config);
  return state;
} 