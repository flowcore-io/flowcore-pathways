import { PostgresJsAdapter } from "../postgres/index.ts"
import type { PostgresAdapter } from "../postgres/index.ts"
import type { PostgresPumpStateConfig, PumpState, PumpStateManager, PumpStateManagerFactory } from "./types.ts"

const DEFAULT_PUMP_GROUP = "default"

/**
 * PostgreSQL-backed pump state manager.
 * Stores per-`(flowType, pumpGroup)` pump position (`timeBucket` + `eventId`) for resume support.
 */
class PostgresPumpStateManager implements PumpStateManager {
  private initialized = false

  constructor(
    private readonly adapter: PostgresAdapter,
    private readonly flowType: string,
    private readonly pumpGroup: string,
    private readonly tableName: string,
  ) {}

  /**
   * Idempotent schema bootstrap + migration:
   * 1. Create the table with the new composite-PK shape if missing.
   * 2. If a pre-existing single-column-PK table is found, add the `pump_group`
   *    column (defaulting existing rows to `"default"`) and swap the primary key
   *    to the composite `(flow_type, pump_group)`. Library uses CREATE TABLE
   *    IF NOT EXISTS so it WILL NOT fix existing tables on its own — this
   *    method runs the migration explicitly on first use.
   */
  private async ensureInitialized(): Promise<void> {
    if (this.initialized) return
    await this.adapter.execute(`
      CREATE TABLE IF NOT EXISTS ${this.tableName} (
        flow_type TEXT NOT NULL,
        pump_group TEXT NOT NULL DEFAULT 'default',
        time_bucket TEXT NOT NULL,
        event_id TEXT,
        PRIMARY KEY (flow_type, pump_group)
      )
    `)
    await this.adapter.execute(
      `ALTER TABLE ${this.tableName} ADD COLUMN IF NOT EXISTS pump_group TEXT NOT NULL DEFAULT 'default'`,
    )
    await this.adapter.execute(`
      DO $$
      DECLARE
        pk_cols int;
      BEGIN
        SELECT count(*) INTO pk_cols
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
        WHERE tc.table_name = '${this.tableName}'
          AND tc.constraint_type = 'PRIMARY KEY';
        IF pk_cols = 1 THEN
          EXECUTE 'ALTER TABLE ${this.tableName} DROP CONSTRAINT ${this.tableName}_pkey';
          EXECUTE 'ALTER TABLE ${this.tableName} ADD PRIMARY KEY (flow_type, pump_group)';
        END IF;
      END $$;
    `)
    this.initialized = true
  }

  async getState(): Promise<PumpState | null> {
    await this.ensureInitialized()
    const result = await this.adapter.query<Array<{ time_bucket: string; event_id: string | null }>>(
      `SELECT time_bucket, event_id FROM ${this.tableName} WHERE flow_type = $1 AND pump_group = $2`,
      [this.flowType, this.pumpGroup],
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
      `INSERT INTO ${this.tableName} (flow_type, pump_group, time_bucket, event_id)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (flow_type, pump_group) DO UPDATE
       SET time_bucket = $3, event_id = $4`,
      [this.flowType, this.pumpGroup, state.timeBucket, state.eventId ?? null],
    )
  }

  async clearState(): Promise<void> {
    await this.ensureInitialized()
    await this.adapter.execute(
      `DELETE FROM ${this.tableName} WHERE flow_type = $1 AND pump_group = $2`,
      [this.flowType, this.pumpGroup],
    )
  }
}

/**
 * Creates a factory function that produces PostgreSQL-backed pump state managers.
 * Each `(flowType, pumpGroup)` gets its own state row in the shared table.
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

  // The returned function MUST declare `pumpGroup` without a default value so
  // `Function.prototype.length === 2`. PathwayPump's arity check
  // (`stateManagerFactoryArity <= 1`) treats single-arg factories as legacy and
  // calls them with `flowType` only — collapsing every pumpGroup onto one
  // shared state manager. The `?? DEFAULT_PUMP_GROUP` fallback inside the body
  // preserves runtime safety for any external caller passing `undefined`.
  return (flowType: string, pumpGroup: string): PumpStateManager => {
    return new PostgresPumpStateManager(adapter, flowType, pumpGroup ?? DEFAULT_PUMP_GROUP, table)
  }
}
