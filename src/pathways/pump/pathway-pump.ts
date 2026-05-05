import type { FlowcoreEvent } from "../../contracts/event.ts"
import type { Logger } from "../logger.ts"
import { NoopLogger } from "../logger.ts"
import type {
  PathwayPumpOptions,
  PumpConcurrencyConfig,
  PumpNotifierConfig,
  PumpState,
  PumpStateManager,
  PumpStateManagerFactory,
} from "./types.ts"

/**
 * Registered pathway info needed for pump grouping.
 * `pumpGroup` defaults to `"default"` when omitted.
 */
interface PathwayRegistration {
  flowType: string
  eventType: string
  pumpGroup?: string
}

/**
 * Filter for {@link PathwayPump.reset}. Each criterion narrows which pumps are reset.
 * - Omit both → reset every pump.
 * - `flowTypes` → reset every pump whose flow type matches.
 * - `pumpGroups` → reset every pump whose pump group matches.
 * - Both → reset pumps that match BOTH (intersection).
 */
export interface PumpResetFilter {
  flowTypes?: string[]
  pumpGroups?: string[]
}

// deno-lint-ignore no-explicit-any
type DataPumpInstance = any

// deno-lint-ignore no-explicit-any
type DataPumpConstructor = any

const RESTART_BASE_MS = 1_000
const RESTART_MAX_MS = 30_000

const DEFAULT_PUMP_GROUP = "default"

/**
 * Composite key uniquely identifying a pump within a builder: `${flowType}::${pumpGroup}`.
 * Used as the map key for `pumps`, `stateManagers`, `restartAttempts`, and `groupMeta`.
 */
function groupKey(flowType: string, pumpGroup: string): string {
  return `${flowType}::${pumpGroup}`
}

/**
 * Normalize the user-facing `concurrency` option into a `Required<PumpConcurrencyConfig>`.
 *
 * Accepts:
 *  - `undefined` → `{ default: 1, byFlowType: {}, byPumpGroup: {} }`
 *  - `number`    → `{ default: n, byFlowType: {}, byPumpGroup: {} }`
 *  - object      → shallow copy, `default` falls back to `1`, others to `{}`
 */
function normalizeConcurrency(
  concurrency: PathwayPumpOptions["concurrency"],
): Required<PumpConcurrencyConfig> {
  if (typeof concurrency === "number") {
    return { default: concurrency, byFlowType: {}, byPumpGroup: {} }
  }
  if (concurrency && typeof concurrency === "object") {
    return {
      default: concurrency.default ?? 1,
      byFlowType: { ...(concurrency.byFlowType ?? {}) },
      byPumpGroup: { ...(concurrency.byPumpGroup ?? {}) },
    }
  }
  return { default: 1, byFlowType: {}, byPumpGroup: {} }
}

interface GroupMeta {
  flowType: string
  pumpGroup: string
  eventTypes: string[]
}

/**
 * PathwayPump orchestrates data pump instances for auto-fetching events from Flowcore.
 *
 * Groups registered pathways by `(flowType, pumpGroup)` and creates one FlowcoreDataPump
 * per group. Within one `flowType`, multiple `pumpGroup`s give independent state cursors,
 * processor concurrency, and restart backoff. Events are routed to PathwaysBuilder.process()
 * for handling.
 *
 * Resilience: per-group restarts on error use exponential backoff and keep retrying
 * indefinitely (capped at {@link RESTART_MAX_MS}). A failure during a restart attempt
 * does NOT stop further attempts — the loop continues until the pump is explicitly stopped
 * or the restart eventually succeeds.
 */
export class PathwayPump {
  private readonly stateManagerFactory: PumpStateManagerFactory
  private readonly notifier: PumpNotifierConfig
  private readonly bufferSize: number
  private readonly maxRedeliveryCount: number
  private readonly concurrency: Required<PumpConcurrencyConfig>
  private readonly logger: Logger
  private readonly stateManagerFactoryArity: number
  private legacyFactoryWarningEmitted = false
  private pulseConfig?: {
    url: string
    intervalMs?: number
    pathwayId?: string
    successLogLevel?: "debug" | "info" | "warn" | "error"
    failureLogLevel?: "debug" | "info" | "warn" | "error"
  }

  private pumps: Map<string, DataPumpInstance> = new Map()
  private stateManagers: Map<string, PumpStateManager> = new Map()
  private running = false
  private restartAttempts: Map<string, number> = new Map()
  private restartTimers: Map<string, ReturnType<typeof setTimeout>> = new Map()
  private groupMeta: Map<string, GroupMeta> = new Map()
  private dataPumpConstructor: DataPumpConstructor = null

  // Required config from PathwaysBuilder
  private tenant = ""
  private dataCore = ""
  private apiKey = ""
  private baseUrl = ""

  // Event processor callback
  private processEvent: ((pathway: string, event: FlowcoreEvent) => Promise<void>) | null = null

  constructor(options: PathwayPumpOptions, logger?: Logger) {
    this.stateManagerFactory = options.stateManagerFactory
    this.stateManagerFactoryArity = options.stateManagerFactory.length
    this.notifier = options.notifier ?? { type: "websocket" }
    this.bufferSize = options.bufferSize ?? 1000
    this.maxRedeliveryCount = options.maxRedeliveryCount ?? 3
    this.concurrency = normalizeConcurrency(options.concurrency)
    this.logger = logger ?? new NoopLogger()
    this.pulseConfig = options.pulse
  }

  /**
   * Configure the pump with pathway builder context
   */
  configure(config: {
    tenant: string
    dataCore: string
    apiKey: string
    baseUrl: string
    processEvent: (pathway: string, event: FlowcoreEvent) => Promise<void>
  }): void {
    this.tenant = config.tenant
    this.dataCore = config.dataCore
    this.apiKey = config.apiKey
    this.baseUrl = config.baseUrl
    this.processEvent = config.processEvent
  }

  /**
   * Start pumps for the given pathway registrations.
   * Groups by `(flowType, pumpGroup)` and creates one pump per group.
   */
  async start(pathways: PathwayRegistration[]): Promise<void> {
    if (this.running) return
    if (!this.processEvent) {
      throw new Error("PathwayPump not configured — call configure() before start()")
    }

    this.running = true

    const groups = new Map<string, GroupMeta>()
    for (const pw of pathways) {
      const pumpGroup = pw.pumpGroup ?? DEFAULT_PUMP_GROUP
      const key = groupKey(pw.flowType, pumpGroup)
      const existing = groups.get(key)
      if (existing) {
        existing.eventTypes.push(pw.eventType)
      } else {
        groups.set(key, { flowType: pw.flowType, pumpGroup, eventTypes: [pw.eventType] })
      }
    }

    this.logger.info("Starting data pumps", {
      groups: [...groups.values()].map((g) => ({
        flowType: g.flowType,
        pumpGroup: g.pumpGroup,
        eventTypes: g.eventTypes.length,
      })),
      totalPathways: pathways.length,
    })

    // Dynamically import @flowcore/data-pump
    const { FlowcoreDataPump } = await import("@flowcore/data-pump")
    this.dataPumpConstructor = FlowcoreDataPump

    for (const meta of groups.values()) {
      this.groupMeta.set(groupKey(meta.flowType, meta.pumpGroup), meta)
      await this.startPumpForGroup(meta)
    }
  }

  /**
   * Resolve a state manager for a `(flowType, pumpGroup)` pair, falling back to
   * the legacy single-arg factory shape when the user-supplied factory has arity 1.
   */
  private resolveStateManager(flowType: string, pumpGroup: string): PumpStateManager {
    if (this.stateManagerFactoryArity <= 1) {
      if (!this.legacyFactoryWarningEmitted && pumpGroup !== DEFAULT_PUMP_GROUP) {
        this.logger.warn(
          "PumpStateManagerFactory has legacy single-arg signature; pump groups on the same flowType " +
            "will share state. Update the factory to accept (flowType, pumpGroup) for proper isolation.",
          { flowType, pumpGroup },
        )
        this.legacyFactoryWarningEmitted = true
      }
      // Cast through unknown to permit legacy single-arg invocation.
      return (this.stateManagerFactory as unknown as (flowType: string) => PumpStateManager)(flowType)
    }
    return this.stateManagerFactory(flowType, pumpGroup)
  }

  /**
   * Resolve effective concurrency for one pump.
   * Order: `byPumpGroup` → `byFlowType` → `default`.
   */
  private resolveConcurrency(flowType: string, pumpGroup: string): number {
    const composite = this.concurrency.byPumpGroup[`${flowType}::${pumpGroup}`]
    if (composite !== undefined) return composite
    const perFlow = this.concurrency.byFlowType[flowType]
    if (perFlow !== undefined) return perFlow
    return this.concurrency.default
  }

  /**
   * Start (or restart) a pump for a specific (flowType, pumpGroup) group.
   *
   * On error from the underlying pump, schedules an exponential-backoff restart
   * scoped to this group only. Restart attempts continue indefinitely (capped at
   * {@link RESTART_MAX_MS}); a synchronous failure during a restart attempt does
   * not stop the loop — it schedules another attempt.
   */
  private async startPumpForGroup(meta: GroupMeta): Promise<void> {
    const { flowType, pumpGroup, eventTypes } = meta
    const key = groupKey(flowType, pumpGroup)

    const stateManager = this.resolveStateManager(flowType, pumpGroup)
    this.stateManagers.set(key, stateManager)

    const notifierOptions = this.buildNotifierOptions(flowType, eventTypes)

    const pumpOptions: Record<string, unknown> = {
      auth: { apiKey: this.apiKey },
      dataSource: {
        tenant: this.tenant,
        dataCore: this.dataCore,
        flowType,
        eventTypes,
      },
      stateManager,
      processor: {
        concurrency: this.resolveConcurrency(flowType, pumpGroup),
        handler: async (events: FlowcoreEvent[]) => {
          for (const event of events) {
            const pathway = `${event.flowType}/${event.eventType}`
            await this.processEvent!(pathway, event)
          }
        },
      },
      bufferSize: this.bufferSize,
      maxRedeliveryCount: this.maxRedeliveryCount,
      notifier: notifierOptions,
      logger: {
        debug: (msg: string, m?: Record<string, unknown>) => this.logger.debug(msg, m),
        info: (msg: string, m?: Record<string, unknown>) => this.logger.info(msg, m),
        warn: (msg: string, m?: Record<string, unknown>) => this.logger.warn(msg, m),
        error: (msg: string | Error, m?: Record<string, unknown>) =>
          this.logger.error(msg instanceof Error ? msg.message : msg, m),
      },
    }

    if (this.pulseConfig) {
      // Send the resolved pathway UUID unmodified — the Data Pathways CP pulse
      // route validates `pathwayId: z.string().uuid()` and rejects anything else
      // with 400 BAD_REQUEST. Earlier versions of this file appended
      // `::${flowType}::${pumpGroup}` to distinguish per-group health, but the
      // CP was never updated to parse that suffix, so every 2.4.0 pulse was
      // 400'd in production. Per-group health visibility will return as a
      // proper additive `pumpGroup` field through SDK + CP — tracked separately.
      pumpOptions.pulse = {
        url: this.pulseConfig.url,
        intervalMs: this.pulseConfig.intervalMs,
        pathwayId: this.pulseConfig.pathwayId,
        successLogLevel: this.pulseConfig.successLogLevel,
        failureLogLevel: this.pulseConfig.failureLogLevel,
      }
    }

    // deno-lint-ignore no-explicit-any
    const pump = await this.dataPumpConstructor.create(pumpOptions as any)

    this.pumps.set(key, pump)

    await pump.start((error?: Error) => {
      if (error) {
        this.logger.error(`Data pump error`, error, { flowType, pumpGroup })
        if (!this.running) return
        this.scheduleRestart(meta)
      }
    })

    // Successful start: reset the per-group attempt counter.
    this.restartAttempts.set(key, 0)
    this.logger.info("Data pump started", { flowType, pumpGroup, eventTypes })
  }

  /**
   * Schedule a restart for one pump group with capped exponential backoff.
   * Multiple restart triggers for the same group within the backoff window are deduped.
   * A synchronous failure inside the scheduled restart re-arms another attempt — the
   * loop continues until the group is stopped or a restart succeeds.
   */
  private scheduleRestart(meta: GroupMeta): void {
    const key = groupKey(meta.flowType, meta.pumpGroup)
    if (this.restartTimers.has(key)) return

    const attempts = (this.restartAttempts.get(key) ?? 0) + 1
    this.restartAttempts.set(key, attempts)
    const delay = Math.min(RESTART_BASE_MS * Math.pow(2, attempts - 1), RESTART_MAX_MS)
    this.logger.warn(
      `Restarting pump in ${delay}ms (attempt ${attempts})`,
      { flowType: meta.flowType, pumpGroup: meta.pumpGroup, delay, attempts },
    )

    const timer = setTimeout(async () => {
      this.restartTimers.delete(key)
      if (!this.running) return

      // Stop and discard any prior pump instance for this group.
      const previous = this.pumps.get(key)
      if (previous) {
        try {
          await previous.stop()
        } catch (stopErr) {
          this.logger.warn(`Failed to stop pump before restart — continuing`, {
            flowType: meta.flowType,
            pumpGroup: meta.pumpGroup,
            error: stopErr instanceof Error ? stopErr.message : String(stopErr),
          })
        }
        this.pumps.delete(key)
      }

      try {
        await this.startPumpForGroup(meta)
      } catch (restartError) {
        this.logger.error(
          `Failed to restart pump`,
          restartError instanceof Error ? restartError : new Error(String(restartError)),
          { flowType: meta.flowType, pumpGroup: meta.pumpGroup, attempts },
        )
        if (this.running) {
          this.scheduleRestart(meta)
        }
      }
    }, delay)
    this.restartTimers.set(key, timer)
  }

  /**
   * Stop all running pumps
   */
  async stop(): Promise<void> {
    if (!this.running) return
    this.running = false

    this.logger.info("Stopping data pumps")

    for (const timer of this.restartTimers.values()) {
      clearTimeout(timer)
    }
    this.restartTimers.clear()

    for (const [key, pump] of this.pumps) {
      const meta = this.groupMeta.get(key)
      try {
        await pump.stop()
        this.logger.info("Data pump stopped", { flowType: meta?.flowType, pumpGroup: meta?.pumpGroup })
      } catch (err) {
        this.logger.error(
          `Error stopping pump`,
          err instanceof Error ? err : new Error(String(err)),
          { flowType: meta?.flowType, pumpGroup: meta?.pumpGroup },
        )
      }
    }

    this.pumps.clear()
    this.stateManagers.clear()
    this.restartAttempts.clear()
    this.groupMeta.clear()
  }

  /**
   * Reset pumps to a specific position, or clear state and bounce if no position given.
   * Uses @flowcore/data-pump's restart() to reposition the cursor without recreating instances.
   *
   * Filter accepts:
   *   - `string[]`                                 → legacy: filter by flow type names
   *   - `{ flowTypes?, pumpGroups? }`              → narrow to matching `(flowType, pumpGroup)` pumps
   *
   * Both are supported for back-compat; the array form is equivalent to `{ flowTypes }`.
   *
   * @param position - Target position { timeBucket, eventId? }. If omitted, clears persisted state
   *                   and restarts pumps (pump will start from live position).
   *                   To replay from the very beginning, pass the first time bucket explicitly.
   * @returns Array of `${flowType}::${pumpGroup}` keys for pumps that were reset.
   */
  async reset(position?: PumpState, filter?: string[] | PumpResetFilter): Promise<string[]> {
    if (!this.running) {
      throw new Error("PathwayPump is not running — cannot reset")
    }

    const normalized: PumpResetFilter | undefined = Array.isArray(filter) ? { flowTypes: filter } : filter

    this.logger.info("Resetting data pumps", { position, filter: normalized })

    const reset: string[] = []

    for (const [key, pump] of this.pumps) {
      const meta = this.groupMeta.get(key)
      if (!meta) continue
      if (normalized?.flowTypes && !normalized.flowTypes.includes(meta.flowType)) continue
      if (normalized?.pumpGroups && !normalized.pumpGroups.includes(meta.pumpGroup)) continue

      try {
        if (position) {
          await pump.restart({ timeBucket: position.timeBucket, eventId: position.eventId })
        } else {
          const stateManager = this.stateManagers.get(key)
          if (stateManager?.clearState) {
            await stateManager.clearState()
          }
          await pump.restart({ timeBucket: new Date().toISOString().replace(/[-:T]/g, "").slice(0, 14) })
        }
        reset.push(key)
        this.logger.info("Data pump reset", {
          flowType: meta.flowType,
          pumpGroup: meta.pumpGroup,
          position,
        })
      } catch (err) {
        this.logger.error(
          `Error resetting pump`,
          err instanceof Error ? err : new Error(String(err)),
          { flowType: meta.flowType, pumpGroup: meta.pumpGroup },
        )
        throw err
      }
    }

    return reset
  }

  async setPulseConfig(pulseConfig: NonNullable<PathwayPumpOptions["pulse"]>): Promise<void> {
    this.pulseConfig = pulseConfig

    if (!this.running) {
      return
    }

    const groups = [...this.groupMeta.values()]
    const existingPumps = [...this.pumps.entries()]

    for (const [key, pump] of existingPumps) {
      const meta = this.groupMeta.get(key)
      try {
        await pump.stop()
        this.logger.info("Data pump stopped for pulse reconfiguration", {
          flowType: meta?.flowType,
          pumpGroup: meta?.pumpGroup,
        })
      } catch (err) {
        this.logger.error(
          `Error stopping pump during pulse reconfiguration`,
          err instanceof Error ? err : new Error(String(err)),
          { flowType: meta?.flowType, pumpGroup: meta?.pumpGroup },
        )
        throw err
      }
    }

    for (const timer of this.restartTimers.values()) {
      clearTimeout(timer)
    }
    this.restartTimers.clear()
    this.pumps.clear()
    this.stateManagers.clear()
    this.restartAttempts.clear()

    for (const meta of groups) {
      await this.startPumpForGroup(meta)
    }
  }

  get isRunning(): boolean {
    return this.running
  }

  /**
   * Unique flow types currently driven by at least one pump (back-compat with pre-2.4 API).
   */
  get registeredFlowTypes(): string[] {
    const set = new Set<string>()
    for (const meta of this.groupMeta.values()) {
      set.add(meta.flowType)
    }
    return [...set]
  }

  /**
   * All `(flowType, pumpGroup)` pairs currently driven by a pump.
   */
  get registeredPumpGroups(): Array<{ flowType: string; pumpGroup: string }> {
    return [...this.groupMeta.values()].map((m) => ({ flowType: m.flowType, pumpGroup: m.pumpGroup }))
  }

  // deno-lint-ignore no-explicit-any
  private buildNotifierOptions(flowType: string, eventTypes: string[]): any {
    const base = {
      dataSource: {
        tenant: this.tenant,
        dataCore: this.dataCore,
        flowType,
        eventTypes,
      },
      auth: { apiKey: this.apiKey },
    }

    switch (this.notifier.type) {
      case "nats":
        return { ...base, natsServers: this.notifier.natsServers }
      case "poller":
        return { ...base, pollerIntervalMs: this.notifier.pollerIntervalMs }
      case "websocket":
      default:
        return base
    }
  }
}
