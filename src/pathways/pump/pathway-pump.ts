import type { FlowcoreEvent } from "../../contracts/event.ts"
import type { Logger } from "../logger.ts"
import { NoopLogger } from "../logger.ts"
import type {
  PathwayPumpOptions,
  PumpNotifierConfig,
  PumpState,
  PumpStateManager,
  PumpStateManagerFactory,
} from "./types.ts"

/**
 * Registered pathway info needed for pump grouping
 */
interface PathwayRegistration {
  flowType: string
  eventType: string
}

// deno-lint-ignore no-explicit-any
type DataPumpInstance = any

/**
 * PathwayPump orchestrates data pump instances for auto-fetching events from Flowcore.
 *
 * Groups registered pathways by flowType and creates one FlowcoreDataPump per flowType group.
 * Events are routed to PathwaysBuilder.process() for handling.
 */
export class PathwayPump {
  private readonly stateManagerFactory: PumpStateManagerFactory
  private readonly notifier: PumpNotifierConfig
  private readonly bufferSize: number
  private readonly maxRedeliveryCount: number
  private readonly logger: Logger
  private readonly pulseConfig?: { url: string; intervalMs?: number; pathwayId?: string }

  private pumps: Map<string, DataPumpInstance> = new Map()
  private stateManagers: Map<string, PumpStateManager> = new Map()
  private running = false

  // Required config from PathwaysBuilder
  private tenant = ""
  private dataCore = ""
  private apiKey = ""
  private baseUrl = ""

  // Event processor callback
  private processEvent: ((pathway: string, event: FlowcoreEvent) => Promise<void>) | null = null

  constructor(options: PathwayPumpOptions, logger?: Logger) {
    this.stateManagerFactory = options.stateManagerFactory
    this.notifier = options.notifier ?? { type: "websocket" }
    this.bufferSize = options.bufferSize ?? 1000
    this.maxRedeliveryCount = options.maxRedeliveryCount ?? 3
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
   * Groups by flowType and creates one pump per group.
   */
  async start(pathways: PathwayRegistration[]): Promise<void> {
    if (this.running) return
    if (!this.processEvent) {
      throw new Error("PathwayPump not configured — call configure() before start()")
    }

    this.running = true

    // Group pathways by flowType
    const flowTypeGroups = new Map<string, string[]>()
    for (const pw of pathways) {
      const eventTypes = flowTypeGroups.get(pw.flowType) ?? []
      eventTypes.push(pw.eventType)
      flowTypeGroups.set(pw.flowType, eventTypes)
    }

    this.logger.info("Starting data pumps", {
      flowTypes: [...flowTypeGroups.keys()],
      totalPathways: pathways.length,
    })

    // Dynamically import @flowcore/data-pump
    const { FlowcoreDataPump } = await import("@flowcore/data-pump")

    for (const [flowType, eventTypes] of flowTypeGroups) {
      const stateManager = this.stateManagerFactory(flowType)
      this.stateManagers.set(flowType, stateManager)

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
          concurrency: 1,
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
          debug: (msg: string, meta?: Record<string, unknown>) => this.logger.debug(msg, meta),
          info: (msg: string, meta?: Record<string, unknown>) => this.logger.info(msg, meta),
          warn: (msg: string, meta?: Record<string, unknown>) => this.logger.warn(msg, meta),
          error: (msg: string | Error, meta?: Record<string, unknown>) =>
            this.logger.error(
              msg instanceof Error ? msg.message : msg,
              meta,
            ),
        },
      }

      if (this.pulseConfig) {
        pumpOptions.pulse = {
          url: this.pulseConfig.url,
          intervalMs: this.pulseConfig.intervalMs,
          pathwayId: this.pulseConfig.pathwayId ?? "unknown",
        }
      }

      // deno-lint-ignore no-explicit-any
      const pump = await FlowcoreDataPump.create(pumpOptions as any)

      this.pumps.set(flowType, pump)

      await pump.start((error?: Error) => {
        if (error) {
          this.logger.error(`Data pump error for flowType ${flowType}`, error, { flowType })
        }
      })

      this.logger.info("Data pump started", { flowType, eventTypes })
    }
  }

  /**
   * Stop all running pumps
   */
  async stop(): Promise<void> {
    if (!this.running) return
    this.running = false

    this.logger.info("Stopping data pumps")

    for (const [flowType, pump] of this.pumps) {
      try {
        await pump.stop()
        this.logger.info("Data pump stopped", { flowType })
      } catch (err) {
        this.logger.error(
          `Error stopping pump for ${flowType}`,
          err instanceof Error ? err : new Error(String(err)),
        )
      }
    }

    this.pumps.clear()
    this.stateManagers.clear()
  }

  /**
   * Reset all pumps to a specific position, or clear state and bounce if no position given.
   * Uses @flowcore/data-pump's restart() to reposition the cursor without recreating instances.
   *
   * @param position - Target position { timeBucket, eventId? }. If omitted, clears persisted state
   *                   and restarts pumps (pump will start from live position).
   *                   To replay from the very beginning, pass the first time bucket explicitly.
   */
  async reset(position?: PumpState): Promise<void> {
    if (!this.running) {
      throw new Error("PathwayPump is not running — cannot reset")
    }

    this.logger.info("Resetting data pumps", { position })

    for (const [flowType, pump] of this.pumps) {
      try {
        if (position) {
          await pump.restart({ timeBucket: position.timeBucket, eventId: position.eventId })
        } else {
          // Clear persisted state then restart pump from live
          const stateManager = this.stateManagers.get(flowType)
          if (stateManager?.clearState) {
            await stateManager.clearState()
          }
          await pump.restart({ timeBucket: new Date().toISOString().replace(/[-:T]/g, "").slice(0, 14) })
        }
        this.logger.info("Data pump reset", { flowType, position })
      } catch (err) {
        this.logger.error(
          `Error resetting pump for ${flowType}`,
          err instanceof Error ? err : new Error(String(err)),
        )
        throw err
      }
    }
  }

  get isRunning(): boolean {
    return this.running
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
