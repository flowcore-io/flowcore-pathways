import type { PostgresConfig } from "../postgres/index.ts"

/**
 * Options for configuring the data pump
 */
export interface PathwayPumpOptions {
  stateManagerFactory: PumpStateManagerFactory
  notifier?: PumpNotifierConfig
  bufferSize?: number
  maxRedeliveryCount?: number
  /** If true, calls provision() before starting the pump */
  autoProvision?: boolean
  /** Optional pulse reporting to control plane */
  pulse?: {
    /** Control plane API URL for pulse endpoint */
    url: string
    /** Pulse interval in milliseconds (default: 30000) */
    intervalMs?: number
    /** Pathway ID for this pump */
    pathwayId?: string
  }
}

/**
 * Factory function that creates a state manager for a given flowType
 */
export type PumpStateManagerFactory = (flowType: string) => PumpStateManager

/**
 * State manager interface compatible with @flowcore/data-pump's FlowcoreDataPumpStateManager
 */
export interface PumpStateManager {
  getState(): Promise<PumpState | null> | PumpState | null
  setState(state: PumpState): Promise<void> | void
  clearState?(): Promise<void> | void
}

/**
 * Pump state tracking position in the event stream
 */
export interface PumpState {
  timeBucket: string
  eventId?: string
}

/**
 * Notifier configuration for the pump
 */
export type PumpNotifierConfig =
  | { type: "websocket" }
  | { type: "nats"; natsServers: string[] }
  | { type: "poller"; pollerIntervalMs: number }

/**
 * Config for the Postgres pump state manager factory
 */
export type PostgresPumpStateConfig = PostgresConfig & {
  tableName?: string
}
