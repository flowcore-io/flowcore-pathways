import type { PostgresConfig } from "../postgres/index.ts"

/**
 * Concurrency settings for event processing per pump.
 *
 * @property default    Default concurrency applied to every flow type. Default: 1.
 * @property byFlowType Per-flow-type overrides keyed by `flowType` name.
 */
export interface PumpConcurrencyConfig {
  /** Default concurrency applied to every flow type. Default: 1. */
  default?: number
  /** Per-flow-type overrides keyed by `flowType` name. */
  byFlowType?: Record<string, number>
}

/**
 * Options for configuring the data pump
 */
export interface PathwayPumpOptions {
  stateManagerFactory: PumpStateManagerFactory
  notifier?: PumpNotifierConfig
  bufferSize?: number
  maxRedeliveryCount?: number
  /** If true, applies the builder's environment-aware provisioning rules before startup */
  autoProvision?: boolean
  /**
   * Concurrency for event processing: pass a number for a shared default, or an object
   * for per-flow-type overrides. Missing flow types fall back to `default` (or 1).
   */
  concurrency?: number | PumpConcurrencyConfig
  /** Optional pulse reporting to control plane */
  pulse?: {
    /** Control plane API URL for pulse endpoint */
    url: string
    /** Pulse interval in milliseconds (default: 30000) */
    intervalMs?: number
    /** Pathway ID for this pump */
    pathwayId?: string
    /** Log level for successful pulses. Defaults to 'debug'. */
    successLogLevel?: "debug" | "info" | "warn" | "error"
    /** Log level for pulse failures. Defaults to 'warn'. */
    failureLogLevel?: "debug" | "info" | "warn" | "error"
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
