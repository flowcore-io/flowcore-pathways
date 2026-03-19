import { PostgresJsAdapter } from "../postgres/index.ts"
import type { PostgresAdapter } from "../postgres/index.ts"
import type { PostgresPumpStateConfig, PumpState, PumpStateManager, PumpStateManagerFactory } from "./types.ts"

/**
 * PostgreSQL-backed pump state manager.
 * Stores per-flowType pump position (timeBucket + eventId) for resume support.
 */
class PostgresPumpStateManager implements PumpStateManager {
  private initialized = false

  constructor(
    private readonly adapter: PostgresAdapter,
    private readonly flowType: string,
    private readonly tableName: string,
  ) {}

  private async ensureInitialized(): Promise<void> {
    if (this.initialized) return
    await this.adapter.execute(`
      CREATE TABLE IF NOT EXISTS ${this.tableName} (
        flow_type TEXT PRIMARY KEY,
        time_bucket TEXT NOT NULL,
        event_id TEXT
      )
    `)
    this.initialized = true
  }

  async getState(): Promise<PumpState | null> {
    await this.ensureInitialized()
    const result = await this.adapter.query<Array<{ time_bucket: string; event_id: string | null }>>(
      `SELECT time_bucket, event_id FROM ${this.tableName} WHERE flow_type = $1`,
      [this.flowType],
    )

    if (!Array.isArray(result) || result.length === 0) return null

    return {
      timeBucket: result[0].time_bucket,
      eventId: result[0].event_id ?? undefined,
    }
  }

  async setState(state: PumpState): Promise<void> {
    await this.ensureInitialized()
    await this.adapter.execute(
      `INSERT INTO ${this.tableName} (flow_type, time_bucket, event_id)
       VALUES ($1, $2, $3)
       ON CONFLICT (flow_type) DO UPDATE
       SET time_bucket = $2, event_id = $3`,
      [this.flowType, state.timeBucket, state.eventId ?? null],
    )
  }
}

/**
 * Creates a factory function that produces PostgreSQL-backed pump state managers.
 * Each flowType gets its own state row in the shared table.
 *
 * The adapter is created once and shared across all state managers.
 */
export async function createPostgresPumpStateManagerFactory(
  config: PostgresPumpStateConfig,
): Promise<PumpStateManagerFactory> {
  const { tableName, ...pgConfig } = config
  const table = tableName ?? "pathway_pump_state"

  const adapter = new PostgresJsAdapter(pgConfig)
  await adapter.connect()

  return (flowType: string): PumpStateManager => {
    return new PostgresPumpStateManager(adapter, flowType, table)
  }
}
