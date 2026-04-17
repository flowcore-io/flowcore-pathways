import { type AnyZodObject, z } from "zod"
import type { WebhookBuilder as WebhookBuilderType, WebhookSendOptions } from "@flowcore/sdk-transformer-core"
import { fileTypeFromBuffer } from "file-type"
import { Subject } from "rxjs"
import { WebhookBuilder } from "../compatibility/flowcore-transformer-core.sdk.ts"
import type { FlowcoreEvent } from "../contracts/event.ts"
import { InternalPathwayState } from "./internal-pathway.state.ts"
import type { Logger } from "./logger.ts"
import { NoopLogger } from "./logger.ts"
import { CommandPoller } from "./command-poller.ts"
import type {
  EventMetadata,
  PathwayContract,
  PathwayKey,
  PathwayState,
  PathwayWriteOptions,
  SendFilehook,
  SendWebhook,
  SendWebhookBatch,
  WritablePathway,
} from "./types.ts"
import type { PathwayClusterOptions } from "./cluster/types.ts"
import { ClusterManager } from "./cluster/cluster-manager.ts"
import type { AutoProvisionConfig, PathwayPumpOptions, PumpState } from "./pump/types.ts"
import { PathwayPump } from "./pump/pathway-pump.ts"
import { PathwayProvisioner, type ProvisionerRegistration } from "./provisioner.ts"

export type { AutoProvisionConfig } from "./pump/types.ts"

/**
 * Defaults for each auto-provisioning stage — resources on, pathway registration off.
 *
 * These defaults deliberately skip the by-name pathway upsert so most deployments don't
 * accidentally hit the control plane at startup; opt in via `autoProvision.pathway: true`.
 */
const DEFAULT_AUTO_PROVISION: Required<AutoProvisionConfig> = {
  dataCore: true,
  flowType: true,
  eventType: true,
  pathway: false,
}

/**
 * Resolve a user-supplied `autoProvision` / `defaultAutoProvision` value into a fully
 * populated `Required<AutoProvisionConfig>`.
 *
 * Resolution rules (first match wins):
 *  1. `autoProvision` object   → merge with `DEFAULT_AUTO_PROVISION`
 *  2. `autoProvision` boolean  → `true` → defaults; `false` → all-false
 *  3. `defaultAutoProvision === false` → all-false
 *  4. otherwise → `DEFAULT_AUTO_PROVISION`
 */
function resolveAutoProvision(
  autoProvision: boolean | AutoProvisionConfig | undefined,
  defaultAutoProvision?: boolean,
): Required<AutoProvisionConfig> {
  if (typeof autoProvision === "object" && autoProvision !== null) {
    return { ...DEFAULT_AUTO_PROVISION, ...autoProvision }
  }
  if (autoProvision === true) {
    return { ...DEFAULT_AUTO_PROVISION }
  }
  if (autoProvision === false) {
    return { dataCore: false, flowType: false, eventType: false, pathway: false }
  }
  if (defaultAutoProvision === false) {
    return { dataCore: false, flowType: false, eventType: false, pathway: false }
  }
  return { ...DEFAULT_AUTO_PROVISION }
}
import {
  AUDIT_ENTITY_ID,
  AUDIT_ENTITY_TYPE,
  AUDIT_MODE,
  AUDIT_ON_BEHALF_OF_ID,
  AUDIT_ON_BEHALF_OF_TYPE,
  AUDIT_SESSION_ID,
  AUDIT_SYSTEM_MODE,
  AUDIT_USER_MODE,
} from "./constants.ts"
import { FileEventSchema, FileInputSchema } from "./types.ts"
import type { Buffer } from "node:buffer"
import process from "node:process"

/**
 * Default timeout for pathway processing in milliseconds (10 seconds)
 */
const DEFAULT_PATHWAY_TIMEOUT_MS = 10000

/**
 * Default maximum number of retry attempts for failed pathway processing
 */
const DEFAULT_MAX_RETRIES = 3

/**
 * Default delay between retry attempts in milliseconds
 */
const DEFAULT_RETRY_DELAY_MS = 500

/**
 * Default TTL for session-specific user resolvers in milliseconds (10seconds)
 */
const DEFAULT_SESSION_USER_RESOLVER_TTL_MS = 10 * 1000

function normalizeRuntimeEnv(runtimeEnv?: string): PathwayRuntimeEnv {
  switch (runtimeEnv) {
    case "development":
    case "production":
    case "test":
      return runtimeEnv
    default:
      return "development"
  }
}

/**
 * Defines the mode for auditing pathway operations
 * - "user": Normal user-initiated operations
 * - "system": System-initiated operations on behalf of a user
 */
export type AuditMode = "user" | "system"

/**
 * Handler function for auditing pathway events
 * @param path The pathway path being audited
 * @param event The event data being processed
 */
export type AuditHandler = (path: string, event: FlowcoreEvent) => void

/**
 * Represents an entity that can be used for audit purposes
 * @property entityId The unique identifier for the entity
 * @property entityType The type of entity (e.g., "user" or "key")
 */
export type UserResolverEntity = {
  entityId: string
  entityType: "user" | "key"
}

/**
 * Async function that resolves to the current user ID
 * Used for audit functionality to track which user initiated an action
 */
export type UserIdResolver = () => Promise<UserResolverEntity> | UserResolverEntity

/**
 * Extended webhook send options with additional audit-specific options
 */
export interface AuditWebhookSendOptions extends WebhookSendOptions {
  /**
   * Custom HTTP headers to include with the webhook request
   */
  headers?: Record<string, string>
}

/**
 * Represents the set log level for different internal log messages
 * @property debug Log level for debug messages
 * @property info Log level for info messages
 * @property warn Log level for warn messages
 * @property error Log level for error messages
 */
export type LogLevel = keyof Pick<Logger, "debug" | "info" | "warn" | "error">

/**
 * Configuration for log levels used by PathwaysBuilder for various operations.
 *
 * @property writeSuccess Log level used when a write operation is successful. Defaults to 'info'.
 * @property pulseSuccess Log level used when a pulse is successfully sent. Defaults to 'debug'.
 * @property pulseFailure Log level used when a pulse emission fails. Defaults to 'warn'.
 * @property provisionSuccess Log level used when virtual pathway provisioning succeeds. Defaults to 'info'.
 * @property provisionFailure Log level used when virtual pathway provisioning fails. Defaults to 'error'.
 */
export type LogLevelConfig = {
  writeSuccess?: LogLevel
  pulseSuccess?: LogLevel
  pulseFailure?: LogLevel
  provisionSuccess?: LogLevel
  provisionFailure?: LogLevel
}

/**
 * Internal log level configuration that ensures all properties are defined
 */
type InternalLogLevelConfig = Required<LogLevelConfig>

export type PathwayRuntimeEnv = "development" | "production" | "test"

export type PathwayMode = "virtual" | "managed"

export interface ManagedPathwayConfig {
  endpointUrl: string
  authHeaders?: Record<string, string>
  sizeClass?: "small" | "medium" | "high"
}

export interface PathwaysBuilderConfig {
  baseUrl: string
  tenant: string
  dataCore: string
  apiKey: string
  pathwayTimeoutMs?: number
  logger?: Logger
  enableSessionUserResolvers?: boolean
  overrideSessionUserResolvers?: SessionUserResolver
  logLevel?: LogLevelConfig
  dataCoreDescription?: string
  dataCoreAccessControl?: string
  dataCoreDeleteProtection?: boolean
  pathwayName?: string
  pathwayLabels?: Record<string, string>
  /** @deprecated No longer used — virtual pathway commands are now poll-based */
  advertisedUrl?: string
  /** @deprecated No longer used — virtual pathway commands are now poll-based */
  resetSecret?: string
  /** @deprecated No longer used — virtual pathway commands are now poll-based */
  resetPath?: string
  pulseUrl?: string
  pulseIntervalMs?: number
  commandPollingIntervalMs?: number
  runtimeEnv?: PathwayRuntimeEnv
  pathwayMode?: PathwayMode
  /**
   * Granular auto-provisioning toggles. Omitted fields fall back to resources-on,
   * pathway-off defaults — see `AutoProvisionConfig`.
   */
  autoProvision?: AutoProvisionConfig
  /**
   * @deprecated Prefer `autoProvision`. Mapping:
   *  - `true`  → `{ dataCore: true,  flowType: true,  eventType: true,  pathway: false }`
   *  - `false` → `{ dataCore: false, flowType: false, eventType: false, pathway: false }`
   */
  defaultAutoProvision?: boolean
  managedConfig?: ManagedPathwayConfig
}

/**
 * SessionUserResolver is a key-value store for storing and retrieving UserIdResolver functions
 * with a TTL (time to live).
 *
 * This allows for session-specific user resolvers to be stored and reused across different
 * sessions or operations.
 */

export interface SessionUserResolver {
  /**
   * Retrieves a UserIdResolver from the session user resolver store
   * @param key The key to retrieve the UserIdResolver for
   * @returns The UserIdResolver or undefined if it doesn't exist
   */
  get(key: string): Promise<UserIdResolver | undefined> | UserIdResolver | undefined

  /**
   * Stores a UserIdResolver in the session user resolver store
   * @param key The key to store the UserIdResolver under
   * @param value The UserIdResolver to store
   * @param ttlMs The time to live for the UserIdResolver in milliseconds
   */
  set(key: string, value: UserIdResolver, ttlMs: number): Promise<void> | void
}

/**
 * SessionUserResolver implementation that uses a Map to store UserIdResolver functions
 * with a TTL (time to live).
 */
export class SessionUser implements SessionUserResolver {
  /**
   * The underlying Map that stores UserIdResolver functions and their timeouts
   * Using unknown for timeout to support both Node.js and Deno timer types
   */
  private store: Map<string, { value: UserIdResolver; timeout: unknown }>

  /**
   * Creates a new SessionUser instance
   */
  constructor() {
    this.store = new Map()
  }

  /**
   * Retrieves a UserIdResolver from the session user resolver store
   * @param key The key to retrieve the UserIdResolver for
   * @returns The UserIdResolver or undefined if it doesn't exist
   */
  get(key: string): UserIdResolver | undefined {
    const entry = this.store.get(key)
    if (!entry) {
      return undefined
    }
    return entry.value as UserIdResolver
  }

  /**
   * Stores a UserIdResolver in the session user resolver store
   * @param key The key to store the UserIdResolver under
   * @param value The UserIdResolver to store
   * @param ttlMs The time to live for the UserIdResolver in milliseconds
   * @default 5 minutes
   */
  set(key: string, value: UserIdResolver, ttlMs = 1000 * 60 * 5): void {
    // Clear any existing timeout for this key
    const existingEntry = this.store.get(key)
    if (existingEntry) {
      clearTimeout(existingEntry.timeout as number)
    }

    // Set up new timeout
    const timeout = setTimeout(() => {
      this.store.delete(key)
    }, ttlMs)

    // Store the new value and its timeout
    this.store.set(key, { value, timeout })
  }
}

/**
 * Main builder class for creating and managing Flowcore pathways
 *
 * The PathwaysBuilder provides an interface for:
 * - Registering pathways with type-safe schemas
 * - Handling events sent to pathways
 * - Writing data to pathways
 * - Managing event processing and retries
 * - Observing event lifecycle (before/after/error)
 * - Audit logging of pathway operations
 *
 * @template TPathway Record type that maps pathway keys to their payload types
 * @template TWritablePaths Union type of pathway keys that can be written to
 */
export class PathwaysBuilder<
  // deno-lint-ignore ban-types
  TPathway extends Record<string, { input: unknown; output: unknown }> = {},
  TWritablePaths extends keyof TPathway = never,
> {
  private readonly pathways: TPathway = {} as TPathway
  private readonly handlers: Record<keyof TPathway, (event: FlowcoreEvent) => Promise<void> | void> = {} as Record<
    keyof TPathway,
    (event: FlowcoreEvent) => Promise<void> | void
  >
  private readonly beforeObservable: Record<keyof TPathway, Subject<FlowcoreEvent>> = {} as Record<
    keyof TPathway,
    Subject<FlowcoreEvent>
  >
  private readonly afterObservers: Record<keyof TPathway, Subject<FlowcoreEvent>> = {} as Record<
    keyof TPathway,
    Subject<FlowcoreEvent>
  >
  private readonly errorObservers: Record<keyof TPathway, Subject<{ event: FlowcoreEvent; error: Error }>> =
    {} as Record<
      keyof TPathway,
      Subject<{ event: FlowcoreEvent; error: Error }>
    >
  private readonly globalErrorSubject = new Subject<{ pathway: string; event: FlowcoreEvent; error: Error }>()
  private readonly writers: Record<TWritablePaths, (SendWebhook<TPathway[TWritablePaths]["output"]> | SendFilehook)> =
    {} as Record<
      TWritablePaths,
      (SendWebhook<TPathway[TWritablePaths]["output"]> | SendFilehook)
    >
  private readonly batchWriters: Record<TWritablePaths, SendWebhookBatch<TPathway[TWritablePaths]["output"]>> =
    {} as Record<
      TWritablePaths,
      SendWebhookBatch<TPathway[TWritablePaths]["output"]>
    >
  private readonly fileWriters: Record<TWritablePaths, SendFilehook> = {} as Record<
    TWritablePaths,
    SendFilehook
  >
  private readonly schemas: Record<keyof TPathway, AnyZodObject> = {} as Record<keyof TPathway, AnyZodObject>
  private readonly inputSchemas: Record<keyof TPathway, AnyZodObject> = {} as Record<keyof TPathway, AnyZodObject>
  private readonly writable: Record<keyof TPathway, boolean> = {} as Record<keyof TPathway, boolean>
  private readonly timeouts: Record<keyof TPathway, number> = {} as Record<keyof TPathway, number>
  private readonly maxRetries: Record<keyof TPathway, number> = {} as Record<keyof TPathway, number>
  private readonly retryDelays: Record<keyof TPathway, number> = {} as Record<keyof TPathway, number>
  private readonly filePathways: Set<keyof TPathway> = new Set()
  private readonly webhookBuilderFactory: () => WebhookBuilderType
  private pathwayState: PathwayState = new InternalPathwayState()
  private pathwayTimeoutMs: number = DEFAULT_PATHWAY_TIMEOUT_MS

  // Audit-related properties
  private auditHandler?: AuditHandler
  private userIdResolver?: UserIdResolver

  // Session-specific user resolvers
  private readonly sessionUserResolvers: SessionUserResolver | null = null

  // Logger instance (but not using it yet due to TypeScript errors)
  private readonly logger: Logger

  // Configuration values needed for cloning
  private readonly baseUrl: string
  private readonly tenant: string
  private readonly dataCore: string
  private readonly apiKey: string
  private readonly logLevel: InternalLogLevelConfig

  // Provisioning descriptions
  private readonly flowTypeDescriptions: Map<string, string> = new Map()
  private readonly eventTypeDescriptions: Map<string, string> = new Map()
  private readonly dataCoreDescription?: string
  private readonly dataCoreAccessControl: string
  private readonly dataCoreDeleteProtection: boolean

  // Virtual pathway auto-provisioning
  private readonly pathwayName?: string
  private readonly pathwayLabels: Record<string, string>
  private readonly pulseUrl: string
  private readonly pulseIntervalMs: number
  private readonly commandPollingIntervalMs: number
  private readonly runtimeEnv: PathwayRuntimeEnv
  private readonly pathwayMode: PathwayMode
  private readonly autoProvision: Required<AutoProvisionConfig>
  private readonly managedConfig?: ManagedPathwayConfig
  private pathwayId?: string

  // Cluster + pump + command poller
  private clusterManager: ClusterManager | null = null
  private pathwayPump: PathwayPump | null = null
  private commandPoller: CommandPoller | null = null
  private clusterBypassProcess = false
  private currentPumpProvisionsPathway = false
  private currentPumpUsesExplicitPulse = false
  private currentPumpUsesAutoPulse = false

  /**
   * Creates a new PathwaysBuilder instance
   * @param options Configuration options for the PathwaysBuilder
   * @param options.baseUrl The base URL for the Flowcore API
   * @param options.tenant The tenant name
   * @param options.dataCore The data core name
   * @param options.apiKey The API key for authentication
   * @param options.pathwayTimeoutMs Optional timeout for pathway processing in milliseconds
   * @param options.logger Optional logger instance
   * @param options.enableSessionUserResolvers Whether to enable session user resolvers
   * @param options.overrideSessionUserResolvers Optional SessionUserResolver instance to override the default
   * @param options.logLevel Optional configuration for log levels
   * @param options.logLevel.writeSuccess Log level for write success messages ('info' or 'debug'). Defaults to 'info'.
   */
  constructor({
    baseUrl,
    tenant,
    dataCore,
    apiKey,
    pathwayTimeoutMs,
    logger,
    enableSessionUserResolvers,
    overrideSessionUserResolvers,
    logLevel,
    dataCoreDescription,
    dataCoreAccessControl,
    dataCoreDeleteProtection,
    pathwayName,
    pathwayLabels,
    advertisedUrl: _advertisedUrl,
    resetSecret: _resetSecret,
    resetPath: _resetPath,
    pulseUrl,
    pulseIntervalMs,
    commandPollingIntervalMs,
    runtimeEnv,
    pathwayMode,
    autoProvision,
    defaultAutoProvision,
    managedConfig,
  }: PathwaysBuilderConfig) {
    // Initialize logger (use NoopLogger if none provided)
    this.logger = logger ?? new NoopLogger()

    // Initialize log levels with defaults
    this.logLevel = {
      writeSuccess: logLevel?.writeSuccess ?? "info",
      pulseSuccess: logLevel?.pulseSuccess ?? "debug",
      pulseFailure: logLevel?.pulseFailure ?? "warn",
      provisionSuccess: logLevel?.provisionSuccess ?? "info",
      provisionFailure: logLevel?.provisionFailure ?? "error",
    }

    // Store configuration values for cloning
    this.baseUrl = baseUrl
    this.tenant = tenant
    this.dataCore = dataCore
    this.apiKey = apiKey

    // Store provisioning config
    this.dataCoreDescription = dataCoreDescription
    this.dataCoreAccessControl = dataCoreAccessControl ?? "private"
    this.dataCoreDeleteProtection = dataCoreDeleteProtection ?? false

    // Store virtual pathway auto-provisioning config
    this.pathwayName = pathwayName
    this.pathwayLabels = pathwayLabels ?? {}
    this.pulseUrl = pulseUrl ?? "https://data-pathways.api.flowcore.io"
    this.pulseIntervalMs = pulseIntervalMs ?? 30_000
    this.commandPollingIntervalMs = commandPollingIntervalMs ?? 5_000
    this.runtimeEnv = normalizeRuntimeEnv(runtimeEnv ?? process.env.NODE_ENV)
    // Env-aware default: production → "managed" (control-plane delivery, serverless-safe),
    // development/test → "virtual" (single-instance local pump).
    this.pathwayMode = pathwayMode ?? (this.runtimeEnv === "production" ? "managed" : "virtual")
    this.autoProvision = resolveAutoProvision(autoProvision, defaultAutoProvision)
    this.managedConfig = managedConfig

    if (enableSessionUserResolvers) {
      this.sessionUserResolvers = overrideSessionUserResolvers ?? new SessionUser()
    }

    this.logger.debug("Initializing PathwaysBuilder", {
      baseUrl,
      tenant,
      dataCore,
      pathwayTimeoutMs,
      runtimeEnv: this.runtimeEnv,
      pathwayMode: this.pathwayMode,
      autoProvision: this.autoProvision,
    })

    this.webhookBuilderFactory = new WebhookBuilder({
      baseUrl,
      tenant,
      dataCore,
      apiKey,
    })
      .withRetry({
        maxAttempts: 5,
        attemptDelayMs: 250,
      })
      .factory()

    if (pathwayTimeoutMs) {
      this.pathwayTimeoutMs = pathwayTimeoutMs
    }
  }

  /**
   * Configures the PathwaysBuilder to use a custom pathway state implementation
   * @param state The PathwayState implementation to use
   * @returns The PathwaysBuilder instance with custom state configured
   */
  withPathwayState(state: PathwayState): PathwaysBuilder<TPathway, TWritablePaths> {
    this.logger.debug("Setting custom pathway state")
    this.pathwayState = state
    return this as PathwaysBuilder<TPathway, TWritablePaths>
  }

  /**
   * Configures the PathwaysBuilder to use audit functionality
   * @param handler The handler function that receives pathway and event information
   * @returns The PathwaysBuilder instance with audit configured
   */
  withAudit(handler: AuditHandler): PathwaysBuilder<TPathway, TWritablePaths> {
    this.logger.debug("Configuring audit functionality")
    this.auditHandler = handler
    return this as PathwaysBuilder<TPathway, TWritablePaths>
  }

  /**
   * Configures the PathwaysBuilder to use a custom user ID resolver
   * @param resolver The resolver function that resolves to the current user ID
   * @returns The PathwaysBuilder instance with custom user ID resolver configured
   */
  withUserResolver(resolver: UserIdResolver): PathwaysBuilder<TPathway, TWritablePaths> {
    this.logger.debug("Configuring user resolver")
    this.userIdResolver = resolver
    return this as PathwaysBuilder<TPathway, TWritablePaths>
  }

  /**
   * Registers a user resolver for a specific session
   *
   * Session-specific user resolvers allow you to associate different user IDs with different
   * sessions, which is useful in multi-user applications or when tracking user actions across
   * different sessions.
   *
   * The resolver is stored in a key-value store with a TTL (time to live), and will be used
   * to resolve the user ID when operations are performed with the given session ID. If the resolver
   * expires, it will need to be registered again.
   *
   * This feature works in conjunction with the SessionPathwayBuilder to provide a complete
   * session management solution.
   *
   * @param sessionId The session ID to associate with this resolver
   * @param resolver The resolver function that resolves to the user ID for this session
   * @returns The PathwaysBuilder instance for chaining
   *
   * @throws Error if session user resolvers are not configured (sessionUserResolvers not provided in constructor)
   *
   * @example
   * ```typescript
   * // Register a resolver for a specific session
   * pathwaysBuilder.withSessionUserResolver("session-123", async () => {
   *   return "user-456"; // Return the user ID for this session
   * });
   *
   * // Use with SessionPathwayBuilder
   * const session = new SessionPathwayBuilder(pathwaysBuilder, "session-123");
   * await session.write("user/action", actionData);
   * // The user ID will be automatically included in the metadata
   * ```
   */
  withSessionUserResolver(
    sessionId: string,
    resolver: UserIdResolver,
  ): PathwaysBuilder<TPathway, TWritablePaths> {
    if (!this.sessionUserResolvers) {
      throw new Error("Session user resolvers not configured")
    }

    this.logger.debug("Configuring session-specific user resolver", { sessionId })
    this.sessionUserResolvers.set(sessionId, resolver, DEFAULT_SESSION_USER_RESOLVER_TTL_MS)
    return this as PathwaysBuilder<TPathway, TWritablePaths>
  }

  /**
   * Gets a user resolver for a specific session ID
   * @param sessionId The session ID to get the resolver for
   * @returns The resolver function for the session, or undefined if none exists
   */
  getSessionUserResolver(sessionId: string): UserIdResolver | undefined {
    if (!this.sessionUserResolvers) {
      return undefined
    }
    const resolver = this.sessionUserResolvers.get(sessionId)
    return resolver as UserIdResolver | undefined
  }

  /**
   * Process a pathway event with error handling and retries
   * @param pathway The pathway to process
   * @param data The event data to process
   * @returns Promise that resolves when processing is complete
   */
  public async process(pathway: keyof TPathway, data: FlowcoreEvent) {
    const pathwayStr = String(pathway)

    this.logger.debug(`Processing pathway event`, {
      pathway: pathwayStr,
      eventId: data.eventId,
    })

    if (!this.pathways[pathway]) {
      const error = `Pathway ${pathwayStr} not found`
      this.logger.error(error)
      throw new Error(error)
    }

    // Validate event payload against schema if available
    if (this.schemas[pathway]) {
      const parsedPayload = this.schemas[pathway].safeParse(data.payload)
      try {
        if (!parsedPayload.success) {
          const validationMessage = this.validationErrorToString(parsedPayload.error)
          const error = `Event payload does not match schema for pathway ${pathwayStr}. ${validationMessage}`
          this.logger.error(error, {
            pathway: pathwayStr,
            schema: this.schemas[pathway].toString(),
            validationErrors: parsedPayload.error.errors, // Keep all errors in the logs for debugging
          })
          throw new Error(error)
        }
      } catch (err) {
        const error = `Error validating event payload against schema for pathway ${pathwayStr}: ${
          err instanceof Error ? err.message : String(err)
        }`
        this.logger.error(error)
        throw new Error(error)
      }
      data.payload = parsedPayload.data
    }

    // Call audit handler if configured
    if (this.auditHandler) {
      this.logger.debug(`Calling audit handler for pathway`, {
        pathway: pathwayStr,
        eventId: data.eventId,
      })
      this.auditHandler(pathwayStr, data)
    }

    // Route through cluster if active and not bypassed (bypass = worker processing locally)
    if (this.clusterManager && !this.clusterBypassProcess) {
      await this.clusterManager.processEvent(pathwayStr, data)
      return
    }

    if (this.handlers[pathway]) {
      let retryCount = 0
      const maxRetries = this.maxRetries[pathway] ?? DEFAULT_MAX_RETRIES
      const retryDelayMs = this.retryDelays[pathway] ?? DEFAULT_RETRY_DELAY_MS

      this.logger.debug(`Emitting 'before' event`, {
        pathway: pathwayStr,
        eventId: data.eventId,
      })
      this.beforeObservable[pathway].next(data)

      while (true) {
        try {
          this.logger.debug(`Executing handler for pathway`, {
            pathway: pathwayStr,
            eventId: data.eventId,
            attempt: retryCount + 1,
          })

          // Execute the handler
          const handle = this.handlers[pathway](data)
          await handle

          // If successful, emit success event and mark as processed
          this.logger.debug(`Handler executed successfully, emitting 'after' event`, {
            pathway: pathwayStr,
            eventId: data.eventId,
          })

          this.afterObservers[pathway].next(data)
          await this.pathwayState.setProcessed(data.eventId)

          this.logger.info(`Successfully processed pathway event`, {
            pathway: pathwayStr,
            eventId: data.eventId,
          })

          return
        } catch (error) {
          // Create error object if needed
          const errorObj = error instanceof Error ? error : new Error(String(error))

          this.logger.error(`Error processing pathway event`, errorObj, {
            pathway: pathwayStr,
            eventId: data.eventId,
            retryCount,
            maxRetries,
          })

          // Emit error event with both error and event data
          this.errorObservers[pathway].next({ event: data, error: errorObj })

          // Also emit to global error subject
          this.globalErrorSubject.next({
            pathway: pathwayStr,
            event: data,
            error: errorObj,
          })

          // Check if we should retry
          if (retryCount < maxRetries) {
            retryCount++
            const nextDelay = retryDelayMs * retryCount

            this.logger.debug(`Retrying pathway event processing`, {
              pathway: pathwayStr,
              eventId: data.eventId,
              attempt: retryCount,
              maxRetries,
              nextDelay,
            })

            // Wait for delay before retrying
            await new Promise((resolve) => setTimeout(resolve, nextDelay))
            continue
          }

          // If we've exhausted retries, mark as processed to avoid hanging
          this.logger.warn(`Max retries exceeded for pathway event, marking as processed`, {
            pathway: pathwayStr,
            eventId: data.eventId,
            retryCount,
            maxRetries,
          })

          await this.pathwayState.setProcessed(data.eventId)
          throw error
        }
      }
    } else {
      // No handler, just emit events and mark as processed
      this.logger.debug(`No handler for pathway, emitting events and marking as processed`, {
        pathway: pathwayStr,
        eventId: data.eventId,
      })

      this.beforeObservable[pathway].next(data)
      this.afterObservers[pathway].next(data)
      await this.pathwayState.setProcessed(data.eventId)
    }
  }

  /**
   * Registers a new pathway with the given contract
   * @template F The flow type string
   * @template E The event type string
   * @template S The schema type extending ZodTypeAny
   * @template W Boolean indicating if the pathway is writable (defaults to true)
   * @param contract The pathway contract describing the pathway
   * @returns The PathwaysBuilder instance with the new pathway registered
   */
  register<
    F extends string,
    E extends string,
    S extends AnyZodObject = AnyZodObject,
    W extends boolean = true,
    FP extends boolean = false,
  >(
    contract: PathwayContract<F, E, S> & {
      writable?: W
      maxRetries?: number
      retryDelayMs?: number
      isFilePathway?: FP
    },
  ): PathwaysBuilder<
    & TPathway
    & Record<PathwayKey<F, E>, {
      output: FP extends true ? z.infer<typeof FileEventSchema> & z.infer<S> : z.infer<S>
      input: FP extends true ? z.input<typeof FileInputSchema> & z.input<S> : z.input<S>
    }>,
    TWritablePaths | WritablePathway<PathwayKey<F, E>, W>
  > {
    const path = `${contract.flowType}/${contract.eventType}` as PathwayKey<F, E>
    const writable = contract.writable ?? true

    this.logger.debug(`Registering pathway`, {
      pathway: path,
      flowType: contract.flowType,
      eventType: contract.eventType,
      writable,
      isFilePathway: contract.isFilePathway,
      timeoutMs: contract.timeoutMs,
      maxRetries: contract.maxRetries,
      retryDelayMs: contract.retryDelayMs,
    }) // deno-lint-ignore no-explicit-any
    ;(this.pathways as any)[path] = true
    this.beforeObservable[path] = new Subject<FlowcoreEvent>()
    this.afterObservers[path] = new Subject<FlowcoreEvent>()
    this.errorObservers[path] = new Subject<{ event: FlowcoreEvent; error: Error }>()

    if (writable) {
      if (contract.isFilePathway) {
        this.filePathways.add(path)
        this.fileWriters[path as TWritablePaths] = this.webhookBuilderFactory()
          .buildFileWebhook(contract.flowType, contract.eventType).send as SendFilehook
      } else {
        this.writers[path as TWritablePaths] = this.webhookBuilderFactory()
          .buildWebhook<TPathway[keyof TPathway]["output"]>(contract.flowType, contract.eventType).send as SendWebhook<
            TPathway[keyof TPathway]["output"]
          >
        this.batchWriters[path as TWritablePaths] = this.webhookBuilderFactory()
          .buildWebhook<TPathway[keyof TPathway]["output"]>(contract.flowType, contract.eventType)
          .sendBatch as SendWebhookBatch<
            TPathway[keyof TPathway]["output"]
          >
      }
    }

    if (contract.timeoutMs) {
      this.timeouts[path] = contract.timeoutMs
    }

    if (contract.maxRetries !== undefined) {
      this.maxRetries[path] = contract.maxRetries
    }

    if (contract.retryDelayMs !== undefined) {
      this.retryDelays[path] = contract.retryDelayMs
    }

    if (contract.isFilePathway) {
      this.schemas[path] = (contract.schema ?? z.object({})).merge(FileEventSchema)
      this.inputSchemas[path] = (contract.schema ?? z.object({})).merge(FileInputSchema)
    } else {
      this.schemas[path] = contract.schema ?? z.object({})
      this.inputSchemas[path] = contract.schema ?? z.object({})
    }
    this.writable[path] = writable

    // Store provisioning descriptions
    if (contract.description !== undefined) {
      this.eventTypeDescriptions.set(path, contract.description)
    }
    if (contract.flowTypeDescription !== undefined) {
      this.flowTypeDescriptions.set(contract.flowType, contract.flowTypeDescription)
    }

    this.logger.info(`Pathway registered successfully`, {
      pathway: path,
      flowType: contract.flowType,
      eventType: contract.eventType,
      writable,
      isFilePathway: contract.isFilePathway,
    })

    return this as PathwaysBuilder<
      & TPathway
      & Record<PathwayKey<F, E>, {
        output: FP extends true ? z.infer<typeof FileEventSchema> & z.infer<S> : z.infer<S>
        input: FP extends true ? z.input<typeof FileInputSchema> & z.input<S> : z.input<S>
      }>,
      TWritablePaths | WritablePathway<PathwayKey<F, E>, W>
    >
  }

  /**
   * Gets a pathway instance by its path
   *
   * @template TPath The specific pathway key to retrieve
   * @param path The pathway key to get
   * @returns The pathway instance
   */
  get<TPath extends keyof TPathway>(path: TPath): TPathway[TPath]["output"] {
    this.logger.debug(`Getting pathway`, { pathway: String(path) })
    return this.pathways[path]
  }

  /**
   * Sets a handler function for a pathway
   *
   * This handler will be called whenever an event is received for the specified pathway.
   * Only one handler can be registered per pathway in a given PathwaysBuilder instance.
   *
   * @template TPath The specific pathway key to handle
   * @param path The pathway key to handle
   * @param handler The function that will process events for this pathway
   * @throws Error if the pathway doesn't exist or already has a handler
   */
  handle<TPath extends keyof TPathway>(
    path: TPath,
    handler: (event: FlowcoreEvent<TPathway[TPath]["output"]>) => Promise<void> | void,
  ): PathwaysBuilder<TPathway, TWritablePaths> {
    const pathStr = String(path)
    this.logger.debug(`Setting handler for pathway`, { pathway: pathStr })

    const pathway = this.pathways[path]
    if (!pathway) {
      const error = `Pathway ${pathStr} not found`
      this.logger.error(error)
      throw new Error(error)
    }

    if (this.handlers[path]) {
      const error = `Someone is already handling pathway ${pathStr} in this instance`
      this.logger.error(error)
      throw new Error(error)
    }

    this.handlers[path] = handler as (event: FlowcoreEvent) => Promise<void> | void
    this.logger.info(`Handler set for pathway`, { pathway: pathStr })
    return this
  }

  /**
   * Subscribe to pathway events (before or after processing)
   * @param path The pathway to subscribe to
   * @param handler The handler function for the events
   * @param type The event type to subscribe to (before, after, or all)
   */
  subscribe<TPath extends keyof TPathway>(
    path: TPath,
    handler: (event: FlowcoreEvent<TPathway[TPath]["output"]>) => void,
    type: "before" | "after" | "all" = "before",
  ): PathwaysBuilder<TPathway, TWritablePaths> {
    const pathStr = String(path)

    const pathway = this.pathways[path]
    if (!pathway) {
      const error = `Pathway ${pathStr} not found`
      this.logger.error(error)
      throw new Error(error)
    }

    const typedHandler = handler as (event: FlowcoreEvent) => void

    if (type === "before" || type === "all") {
      this.beforeObservable[path].subscribe(typedHandler)
      this.logger.debug(`Subscribed to 'before' events for pathway`, { pathway: pathStr })
    }

    if (type === "after" || type === "all") {
      this.afterObservers[path].subscribe(typedHandler)
      this.logger.debug(`Subscribed to 'after' events for pathway`, { pathway: pathStr })
    }

    this.logger.info(`Subscription to pathway events set up`, {
      pathway: pathStr,
      type,
    })

    return this
  }

  /**
   * Subscribe to errors for a specific pathway
   * @param path The pathway to subscribe to errors for
   * @param handler The handler function that receives the error and event
   */
  onError<TPath extends keyof TPathway>(
    path: TPath,
    handler: (error: Error, event: FlowcoreEvent<TPathway[TPath]["output"]>) => void,
  ): PathwaysBuilder<TPathway, TWritablePaths> {
    const pathStr = String(path)
    this.logger.debug(`Setting error handler for pathway`, { pathway: pathStr })

    const pathway = this.pathways[path]
    if (!pathway) {
      const error = `Pathway ${pathStr} not found`
      this.logger.error(error)
      throw new Error(error)
    }

    // Type cast to maintain internal consistency while preserving external type safety
    const typedHandler = (payload: { event: FlowcoreEvent; error: Error }) =>
      handler(payload.error, payload.event as FlowcoreEvent<TPathway[TPath]["output"]>)

    this.errorObservers[path].subscribe(typedHandler)
    this.logger.info(`Error handler set for pathway`, { pathway: pathStr })

    return this
  }

  /**
   * Subscribe to errors for all pathways
   * @param handler The handler function that receives the error, event, and pathway name
   */
  onAnyError(
    handler: (error: Error, event: FlowcoreEvent, pathway: string) => void,
  ): PathwaysBuilder<TPathway, TWritablePaths> {
    this.logger.debug(`Subscribing to all pathway errors`)
    this.globalErrorSubject.subscribe(({ pathway, event, error }) => handler(error, event, pathway))
    this.logger.debug(`Subscribed to all pathway errors`)

    return this
  }

  /**
   * Writes data to a pathway with optional audit metadata
   * @param path The pathway to write to
   * @param data The data to write
   * @param metadata Optional metadata to include with the event
   * @param options Optional write options
   * @returns A promise that resolves to the event ID(s)
   */
  async write<TPath extends TWritablePaths, B extends boolean = false>(
    path: TPath,
    input: {
      batch?: B
      data: B extends true ? TPathway[TPath]["input"][] : TPathway[TPath]["input"]
      metadata?: EventMetadata
      options?: PathwayWriteOptions
    },
  ): Promise<string | string[]> {
    const pathStr = String(path)
    const { data: inputData, metadata, options, batch } = input

    if (batch && this.filePathways.has(path)) {
      const error = `Batch is not possible for file pathways. Pathway ${pathStr} is a file pathway`
      this.logger.error(error)
      throw new Error(error)
    }

    this.logger.debug(`Writing to pathway`, {
      pathway: pathStr,
      metadata,
      options: {
        fireAndForget: options?.fireAndForget,
        sessionId: options?.sessionId,
      },
    })

    if (!this.pathways[path]) {
      const error = `Pathway ${pathStr} not found`
      this.logger.error(error)
      throw new Error(error)
    }

    if (!this.writable[path]) {
      const error = `Pathway ${pathStr} is not writable`
      this.logger.error(error)
      throw new Error(error)
    }

    const schema = batch ? z.array(this.inputSchemas[path]) : this.inputSchemas[path]
    const parsedData = schema.safeParse(inputData)
    if (!parsedData.success) {
      const validationMessage = this.validationErrorToString(parsedData.error)
      const errorMessage = `Invalid data for pathway ${pathStr}. ${validationMessage}`
      this.logger.error(errorMessage, {
        pathway: pathStr,
        schema: schema.toString(),
        validationErrors: parsedData.error.errors, // Keep all errors in the logs for debugging
      })
      throw new Error(errorMessage)
    }
    const data = parsedData.data

    // Create a copy of the metadata to avoid modifying the original
    const finalMetadata: EventMetadata = metadata ? { ...metadata } : {}

    // Check for session-specific user resolver
    let userId: UserResolverEntity | undefined
    if (options?.sessionId) {
      const sessionUserResolver = this.getSessionUserResolver(options.sessionId)
      if (sessionUserResolver) {
        try {
          userId = await sessionUserResolver()

          this.logger.debug(`Using session-specific user resolver`, {
            pathway: pathStr,
            sessionId: options.sessionId,
            userId,
          })
        } catch (error) {
          this.logger.error(
            `Error resolving session user ID`,
            error instanceof Error ? error : new Error(String(error)),
            {
              pathway: pathStr,
              sessionId: options.sessionId,
            },
          )
        }
      }
    }

    // Process audit metadata if audit is configured
    if (this.userIdResolver) {
      // Only use global resolver if we don't already have a user ID from a session resolver
      if (!userId) {
        this.logger.debug(`Resolving user ID for audit metadata`, { pathway: pathStr })
        userId = await this.userIdResolver()
      }
    }

    // Determine the audit mode: default is "user" unless explicitly specified as "system"
    const auditMode = options?.auditMode ?? "user"

    this.logger.debug(`Adding audit metadata`, {
      pathway: pathStr,
      auditMode,
      userId,
    })

    if (userId) {
      // Add appropriate audit metadata based on mode
      if (auditMode === AUDIT_SYSTEM_MODE) {
        finalMetadata[AUDIT_MODE] = AUDIT_SYSTEM_MODE
        finalMetadata[AUDIT_ENTITY_TYPE] = "system"
        finalMetadata[AUDIT_ENTITY_ID] = "system"
        finalMetadata[AUDIT_ON_BEHALF_OF_TYPE] = userId.entityType
        finalMetadata[AUDIT_ON_BEHALF_OF_ID] = userId.entityId
      } else {
        finalMetadata[AUDIT_MODE] = AUDIT_USER_MODE
        finalMetadata[AUDIT_ENTITY_TYPE] = userId.entityType
        finalMetadata[AUDIT_ENTITY_ID] = userId.entityId
      }
    }

    if (options?.sessionId) {
      finalMetadata[AUDIT_SESSION_ID] = options.sessionId
    }

    let eventIds: string | string[] = []
    this.logger.debug(`Writing webhook data to pathway`, { pathway: pathStr, batch })
    if (batch) {
      eventIds = await (this.batchWriters[path] as SendWebhookBatch<TPathway[TPath]["output"]>)(
        data as unknown as TPathway[TPath]["output"][],
        finalMetadata,
        options,
      ).catch((error) => {
        this.logger.error(`Error writing batch to pathway`, {
          pathway: pathStr,
          error,
        })
        throw error
      })
    } else if (this.filePathways.has(path)) {
      const { fileId, fileName, fileContent, ...additionalProperties } = data as z.infer<typeof FileInputSchema>
      const fileType = await fileTypeFromBuffer(fileContent as Buffer)
      process.env.DEBUG?.includes("pathways") && console.log("additionalProperties", additionalProperties)
      eventIds = await (this.fileWriters[path] as SendFilehook)(
        {
          fileId,
          fileName,
          fileType: fileType?.mime ?? "application/octet-stream",
          fileContent: new Blob([new Uint8Array(fileContent as Buffer)]),
          additionalProperties,
        },
        finalMetadata,
        options,
      ).catch((error) => {
        this.logger.error(`Error writing file to pathway`, {
          pathway: pathStr,
          fileId,
          fileName,
          error,
        })
        throw error
      })
    } else {
      eventIds = await (this.writers[path] as SendWebhook<TPathway[TPath]["output"]>)(data, finalMetadata, options)
        .catch((error) => {
          this.logger.error(`Error writing webhook to pathway`, {
            pathway: pathStr,
            error,
          })
          throw error
        })
    }

    this.logger[this.logLevel.writeSuccess](`Successfully wrote to pathway`, {
      pathway: pathStr,
      eventIds: Array.isArray(eventIds) ? eventIds : [eventIds],
      fireAndForget: options?.fireAndForget,
    })

    if (!options?.fireAndForget) {
      this.logger.debug(`Waiting for pathway to be processed`, {
        pathway: pathStr,
        eventIds: Array.isArray(eventIds) ? eventIds : [eventIds],
      })

      await Promise.all(
        Array.isArray(eventIds)
          ? eventIds.map((id) => this.waitForPathwayToBeProcessed(id))
          : [this.waitForPathwayToBeProcessed(eventIds)],
      )
    }

    return eventIds
  }

  /**
   * Waits for a specific event to be processed
   *
   * This method polls the pathway state to check if an event has been processed,
   * with a configurable timeout. It will throw an error if the timeout is exceeded.
   *
   * @private
   * @param eventId The ID of the event to wait for
   * @returns Promise that resolves when the event is processed
   * @throws Error if the timeout is exceeded
   */
  private async waitForPathwayToBeProcessed(eventId: string): Promise<void> {
    const startTime = Date.now()
    const timeoutMs = this.timeouts[eventId] ?? this.pathwayTimeoutMs

    this.logger.debug(`Waiting for event to be processed`, {
      eventId,
      timeoutMs,
    })

    let attempts = 0

    while (!(await this.pathwayState.isProcessed(eventId))) {
      attempts++
      const elapsedTime = Date.now() - startTime

      if (elapsedTime > timeoutMs) {
        const errorMessage = `Pathway processing timed out after ${timeoutMs}ms for event ${eventId}`
        this.logger.error(errorMessage, new Error(errorMessage), {
          eventId,
          timeoutMs,
          elapsedTime,
          attempts,
        })
        throw new Error(errorMessage)
      }

      if (attempts % 10 === 0) { // Log every 10 attempts (1 second)
        this.logger.debug(`Still waiting for event to be processed`, {
          eventId,
          elapsedTime,
          attempts,
          timeoutMs,
        })
      }

      await new Promise((resolve) => setTimeout(resolve, 100))
    }

    this.logger.debug(`Event has been processed`, {
      eventId,
      elapsedTime: Date.now() - startTime,
      attempts,
    })
  }

  /**
   * Start cluster mode for distributed event processing.
   * This instance joins the cluster and either becomes a leader (distributes events)
   * or a worker (receives and processes events).
   */
  async startCluster(options: PathwayClusterOptions): Promise<ClusterManager> {
    if (this.clusterManager) {
      throw new Error("Cluster already started")
    }

    this.clusterManager = new ClusterManager(options, this.logger)

    // Set handler: when cluster needs to process locally, call process() with bypass
    this.clusterManager.setEventHandler(async (pathway: string, event: FlowcoreEvent) => {
      this.clusterBypassProcess = true
      try {
        await this.process(pathway as keyof TPathway, event)
      } finally {
        this.clusterBypassProcess = false
      }
    })

    // Listen for leadership changes to auto-start/stop the pump
    this.clusterManager.onLeadershipChange((isLeader: boolean) => {
      this.handleLeadershipChange(isLeader).catch((err) => {
        this.logger.error(
          isLeader
            ? "Failed to bootstrap leader runtime after becoming leader"
            : "Failed to stop leader runtime after losing leadership",
          err instanceof Error ? err : new Error(String(err)),
        )
      })
    })

    // Wire reset handler: leader receives reset requests and delegates to pump
    this.clusterManager.onReset(async (position?: PumpState) => {
      if (!this.pathwayPump) {
        throw new Error("Pump not running on this leader")
      }
      await this.pathwayPump.reset(position)
    })

    await this.clusterManager.start()

    this.logger.info("Cluster started", {
      advertisedAddress: options.advertisedAddress,
      port: options.port,
    })

    return this.clusterManager
  }

  /**
   * Stop cluster mode
   */
  async stopCluster(): Promise<void> {
    if (!this.clusterManager) return
    await this.clusterManager.stop()
    this.clusterManager = null
    this.logger.info("Cluster stopped")
  }

  /**
   * Whether cluster mode is currently active
   */
  get isClusterActive(): boolean {
    return this.clusterManager !== null && this.clusterManager.isRunning
  }

  private buildAutoPulseConfig(): NonNullable<PathwayPumpOptions["pulse"]> | null {
    if (this.pathwayMode !== "virtual" || !this.pathwayId) {
      return null
    }

    return {
      url: this.pulseUrl,
      intervalMs: this.pulseIntervalMs,
      pathwayId: this.pathwayId,
      successLogLevel: this.logLevel.pulseSuccess,
      failureLogLevel: this.logLevel.pulseFailure,
    }
  }

  private async applyAutoPulseConfig(): Promise<void> {
    if (!this.pathwayPump || this.currentPumpUsesExplicitPulse || this.currentPumpUsesAutoPulse) {
      return
    }

    const pulse = this.buildAutoPulseConfig()
    if (!pulse) {
      return
    }

    await this.pathwayPump.setPulseConfig(pulse)
    this.currentPumpUsesAutoPulse = true
    this.logger.info("Auto-configured pulse", { pathwayId: this.pathwayId, url: this.pulseUrl })
  }

  private startCommandPollerIfNeeded(): void {
    if (
      this.pathwayMode !== "virtual" || !this.pathwayId || !this.pathwayPump?.isRunning ||
      this.commandPoller
    ) {
      return
    }

    this.commandPoller = new CommandPoller({
      cpBaseUrl: this.pulseUrl,
      pathwayId: this.pathwayId,
      apiKey: this.apiKey,
      intervalMs: this.commandPollingIntervalMs,
      logger: this.logger,
      onCommand: async (cmd) => {
        if (cmd.type === "datapumpRestart") {
          const position = cmd.position
            ? {
              timeBucket: (cmd.position as Record<string, string>).timeBucket ?? "",
              eventId: (cmd.position as Record<string, string>).eventId,
            }
            : undefined
          const flowTypes = cmd.sourceFlowTypes ?? undefined
          await this.pathwayPump!.reset(position, flowTypes)
        } else {
          this.logger.warn("Unknown command type received", { type: cmd.type, commandId: cmd.id })
        }
      },
      logLevel: {
        pollSuccess: this.logLevel.pulseSuccess,
        pollFailure: this.logLevel.pulseFailure,
      },
    })
    this.commandPoller.start()
    this.logger.info("Command poller started", {
      pathwayId: this.pathwayId,
      intervalMs: this.commandPollingIntervalMs,
    })
  }

  private stopCommandPoller(): void {
    if (!this.commandPoller) {
      return
    }

    this.commandPoller.stop()
    this.commandPoller = null
  }

  private async startCurrentPump(): Promise<void> {
    if (!this.pathwayPump || this.pathwayPump.isRunning) {
      return
    }

    const registrations = this.buildRegistrations()
    await this.pathwayPump.start(registrations)

    this.logger.info("Pump started", {
      pathways: registrations.length,
    })
  }

  private async stopLeaderRuntime(): Promise<void> {
    this.stopCommandPoller()

    if (!this.pathwayPump?.isRunning) {
      return
    }

    await this.pathwayPump.stop()
    this.logger.info("Pump stopped")
  }

  private async bootstrapLeaderPump(): Promise<void> {
    if (!this.pathwayPump) {
      return
    }

    await this.applyAutoPulseConfig()
    await this.startCurrentPump()

    if (this.runtimeEnv !== "production" || this.pathwayMode !== "virtual") {
      this.startCommandPollerIfNeeded()
      return
    }

    if (this.currentPumpProvisionsPathway && !this.pathwayId) {
      try {
        await this.registerPathwayInstance(this.buildRegistrations())
        await this.applyAutoPulseConfig()
      } catch (err) {
        await this.stopLeaderRuntime()
        throw err
      }
    }

    this.startCommandPollerIfNeeded()
  }

  private async handleLeadershipChange(isLeader: boolean): Promise<void> {
    if (isLeader) {
      if (!this.pathwayPump) {
        return
      }

      this.logger.info("Became leader, bootstrapping pump")
      await this.bootstrapLeaderPump()
      return
    }

    this.logger.info("Lost leadership, stopping leader runtime")
    await this.stopLeaderRuntime()
  }

  private buildRegistrations(): ProvisionerRegistration[] {
    return Object.keys(this.pathways).map((key) => {
      const [flowType, eventType] = key.split("/")
      return {
        flowType,
        eventType,
        flowTypeDescription: this.flowTypeDescriptions.get(flowType),
        eventTypeDescription: this.eventTypeDescriptions.get(key),
      }
    })
  }

  private async provisionSharedResources(
    skipFlags: { skipDataCore?: boolean; skipFlowTypes?: boolean; skipEventTypes?: boolean } = {},
  ): Promise<ProvisionerRegistration[]> {
    const registrations = this.buildRegistrations()

    const provisioner = new PathwayProvisioner({
      tenant: this.tenant,
      dataCore: this.dataCore,
      apiKey: this.apiKey,
      dataCoreDescription: this.dataCoreDescription,
      dataCoreAccessControl: this.dataCoreAccessControl,
      dataCoreDeleteProtection: this.dataCoreDeleteProtection,
      registrations,
      logger: this.logger,
      skipDataCore: skipFlags.skipDataCore,
      skipFlowTypes: skipFlags.skipFlowTypes,
      skipEventTypes: skipFlags.skipEventTypes,
    })

    await provisioner.provision()
    return registrations
  }

  private getPathwayProvisionAuthHeader(): string {
    const apiKey = this.apiKey.startsWith("fc_") ? `${this.apiKey.split("_")[1]}:${this.apiKey}` : this.apiKey

    return `ApiKey ${apiKey}`
  }

  private ensurePathwayName(reason: string): string {
    if (!this.pathwayName) {
      throw new Error(reason)
    }
    return this.pathwayName
  }

  private buildManagedPathwayConfig(registrations: ProvisionerRegistration[]): {
    sizeClass: "small" | "medium" | "high"
    config: {
      sources: Array<{
        flowType: string
        eventTypes: string[]
        endpoints: Array<{
          url: string
          authHeaders: Record<string, string>
        }>
      }>
    }
  } {
    if (!this.managedConfig?.endpointUrl) {
      throw new Error(
        "managedConfig.endpointUrl is required when provisioning a managed pathway",
      )
    }

    const grouped = new Map<string, Set<string>>()
    for (const registration of registrations) {
      const eventTypes = grouped.get(registration.flowType) ?? new Set<string>()
      eventTypes.add(registration.eventType)
      grouped.set(registration.flowType, eventTypes)
    }

    return {
      sizeClass: this.managedConfig.sizeClass ?? "small",
      config: {
        sources: [...grouped.entries()].map(([flowType, eventTypes]) => ({
          flowType,
          eventTypes: [...eventTypes],
          endpoints: [{
            url: this.managedConfig!.endpointUrl,
            authHeaders: this.managedConfig!.authHeaders ?? {},
          }],
        })),
      },
    }
  }

  private async upsertPathwayByName(
    type: PathwayMode,
    body: Record<string, unknown>,
    logMeta: Record<string, unknown>,
  ): Promise<void> {
    const pathwayName = this.ensurePathwayName(
      `pathwayName is required when provisioning a ${type} pathway`,
    )
    const url = `${this.pulseUrl}/api/v1/pathways/by-name/${encodeURIComponent(pathwayName)}`

    let response: Response
    try {
      response = await fetch(url, {
        method: "PUT",
        headers: {
          "Content-Type": "application/json",
          Authorization: this.getPathwayProvisionAuthHeader(),
        },
        body: JSON.stringify(body),
      })
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err)
      this.logger[this.logLevel.provisionFailure](`${type} pathway registration failed`, {
        pathwayName,
        url,
        error: msg,
        phase: "network",
      })
      throw new Error(`Failed to register ${type} pathway "${pathwayName}": ${msg}`)
    }

    if (!response.ok) {
      const text = await response.text().catch(() => "")
      this.logger[this.logLevel.provisionFailure](`${type} pathway registration failed`, {
        pathwayName,
        url,
        status: response.status,
        body: text,
        phase: "response",
      })
      throw new Error(
        `Failed to register ${type} pathway "${pathwayName}": ${response.status} ${text}`,
      )
    }

    const result = await response.json() as { pathwayId: string; status: string }
    this.pathwayId = result.pathwayId
    this.logger[this.logLevel.provisionSuccess](`${type} pathway registered`, {
      pathwayName,
      pathwayId: this.pathwayId,
      status: result.status,
      ...logMeta,
    })
  }

  private async registerPathwayInstance(registrations: ProvisionerRegistration[]): Promise<void> {
    if (this.pathwayMode === "managed") {
      const { sizeClass, config } = this.buildManagedPathwayConfig(registrations)
      await this.upsertPathwayByName("managed", {
        tenant: this.tenant,
        dataCore: this.dataCore,
        labels: this.pathwayLabels,
        sizeClass,
        enabled: true,
        type: "managed",
        config,
      }, {
        sizeClass,
        sourceCount: config.sources.length,
      })
      return
    }

    if (!this.pathwayName) {
      return
    }

    const flowTypes = [...new Set(registrations.map((registration) => registration.flowType))]
    await this.upsertPathwayByName("virtual", {
      tenant: this.tenant,
      dataCore: this.dataCore,
      labels: this.pathwayLabels,
      type: "virtual",
      virtualConfig: {
        flowTypes,
      },
    }, {
      flowTypes,
    })
  }

  /**
   * Provision Flowcore infrastructure (data core, flow types, event types).
   * Creates missing resources when descriptions are provided, updates descriptions
   * when they differ. Fails if a resource is missing and no description is provided.
   * Additive only — never deletes.
   */
  async provision(): Promise<void> {
    const registrations = await this.provisionSharedResources()
    await this.registerPathwayInstance(registrations)
  }

  /**
   * Start the data pump to auto-fetch events from Flowcore.
   * If cluster is active, pump only runs on the leader instance.
   */
  async startPump(options: PathwayPumpOptions): Promise<PathwayPump> {
    if (this.pathwayPump) {
      throw new Error("Pump already started")
    }

    // Resolve effective auto-provision config: per-call override wins over builder-level setting.
    const ap = options.autoProvision != null ? resolveAutoProvision(options.autoProvision) : this.autoProvision
    // Track pathway-registration intent separately so bootstrapLeaderPump can pick it up on leadership gain.
    this.currentPumpProvisionsPathway = ap.pathway
    this.currentPumpUsesExplicitPulse = Boolean(options.pulse)
    this.currentPumpUsesAutoPulse = false

    if (this.runtimeEnv === "test") {
      this.logger.info("Skipping remote auto-provisioning in test runtime")
    } else {
      if (this.runtimeEnv === "production" && this.pathwayMode === "virtual" && !this.isClusterActive) {
        throw new Error("Cluster mode must be started before production virtual pump startup")
      }

      const shouldProvisionResources = ap.dataCore || ap.flowType || ap.eventType
      let registrations: ProvisionerRegistration[] | null = null
      if (shouldProvisionResources) {
        this.logger.info("Auto-provisioning Flowcore resources", {
          runtimeEnv: this.runtimeEnv,
          pathwayMode: this.pathwayMode,
          autoProvision: ap,
        })
        registrations = await this.provisionSharedResources({
          skipDataCore: !ap.dataCore,
          skipFlowTypes: !ap.flowType,
          skipEventTypes: !ap.eventType,
        })
      }

      if (ap.pathway) {
        // In production+virtual, pathway registration must be deferred until the leader is ready
        // (bootstrapLeaderPump performs it after pump start). Everywhere else it happens upfront.
        const deferToLeaderBootstrap = this.runtimeEnv === "production" && this.pathwayMode === "virtual"
        if (!deferToLeaderBootstrap) {
          this.logger.info("Registering pathway instance", {
            runtimeEnv: this.runtimeEnv,
            pathwayMode: this.pathwayMode,
          })
          await this.registerPathwayInstance(registrations ?? this.buildRegistrations())
        }
      }
    }

    if (options.pulse) {
      options = {
        ...options,
        pulse: {
          ...options.pulse,
          successLogLevel: options.pulse.successLogLevel ?? this.logLevel.pulseSuccess,
          failureLogLevel: options.pulse.failureLogLevel ?? this.logLevel.pulseFailure,
        },
      }
    } else {
      const autoPulse = this.buildAutoPulseConfig()
      if (autoPulse) {
        this.currentPumpUsesAutoPulse = true
        this.logger.info("Auto-configured pulse", { pathwayId: this.pathwayId, url: this.pulseUrl })
        options = {
          ...options,
          pulse: autoPulse,
        }
      }
    }

    // If cluster active and not leader, don't start pump
    if (this.clusterManager && !this.clusterManager.isLeader) {
      this.logger.info("Not starting pump — this instance is not the cluster leader")
      // Still create the pump but don't start it — it can be started if we become leader
      this.pathwayPump = new PathwayPump(options, this.logger)
      this.pathwayPump.configure({
        tenant: this.tenant,
        dataCore: this.dataCore,
        apiKey: this.apiKey,
        baseUrl: this.baseUrl,
        processEvent: async (pathway: string, event: FlowcoreEvent) => {
          await this.process(pathway as keyof TPathway, event)
        },
      })
      return this.pathwayPump
    }

    this.pathwayPump = new PathwayPump(options, this.logger)
    this.pathwayPump.configure({
      tenant: this.tenant,
      dataCore: this.dataCore,
      apiKey: this.apiKey,
      baseUrl: this.baseUrl,
      processEvent: async (pathway: string, event: FlowcoreEvent) => {
        await this.process(pathway as keyof TPathway, event)
      },
    })

    if (this.runtimeEnv === "production" && this.pathwayMode === "managed") {
      this.logger.info("Not starting local pump — production managed pathways rely on control-plane delivery")
      return this.pathwayPump
    }

    try {
      await this.bootstrapLeaderPump()
    } catch (err) {
      await this.stopPump()
      throw err
    }

    return this.pathwayPump
  }

  /**
   * Stop the data pump
   */
  async stopPump(): Promise<void> {
    this.stopCommandPoller()
    if (!this.pathwayPump) {
      this.currentPumpProvisionsPathway = false
      this.currentPumpUsesExplicitPulse = false
      this.currentPumpUsesAutoPulse = false
      return
    }
    await this.pathwayPump.stop()
    this.pathwayPump = null
    this.currentPumpProvisionsPathway = false
    this.currentPumpUsesExplicitPulse = false
    this.currentPumpUsesAutoPulse = false
    this.logger.info("Pump stopped")
  }

  /**
   * Reset the data pump to a specific position or clear state and restart.
   * In cluster mode, the request is routed to the leader automatically.
   *
   * @param position - Target position { timeBucket, eventId? }. If omitted, clears persisted state
   *                   and restarts from the live position. To replay from the very beginning,
   *                   pass the first time bucket explicitly.
   */
  async resetPump(position?: PumpState, flowTypes?: string[]): Promise<string[]> {
    if (!this.pathwayPump) {
      throw new Error("Pump not started — call startPump() first")
    }

    if (this.clusterManager) {
      if (flowTypes?.length) {
        this.logger.warn("flowTypes filter is not supported in cluster mode reset — resetting all flow types")
      }
      await this.clusterManager.requestReset(position)
      return [...this.pathwayPump.registeredFlowTypes]
    }

    return await this.pathwayPump.reset(position, flowTypes)
  }

  /**
   * Converts a Zod validation error to a human-readable string
   * @param error The Zod validation error to convert
   * @returns A formatted error message string
   */
  private validationErrorToString<Input, Output>(error: z.SafeParseReturnType<Input, Output>["error"]): string {
    const primaryError = error?.errors[0]

    if (!primaryError) {
      return "Unknown validation error"
    }

    const path = primaryError.path.join(".")
    const pathOutput = path ? `${path}: ` : ""

    return `${pathOutput}${primaryError.message}`
  }
}
