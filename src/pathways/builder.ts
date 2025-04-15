import { type Static, type TSchema, Type } from "@sinclair/typebox"
import { Value } from "@sinclair/typebox/value"
import type {
  WebhookBuilder as WebhookBuilderType,
  WebhookFileData,
  WebhookSendOptions,
} from "npm:@flowcore/sdk-transformer-core@^2.3.6"
import { Subject } from "rxjs"
import { WebhookBuilder } from "../compatibility/flowcore-transformer-core.sdk.ts"
import type { FlowcoreEvent } from "../contracts/event.ts"
import { InternalPathwayState } from "./internal-pathway.state.ts"
import type { Logger } from "./logger.ts"
import { NoopLogger } from "./logger.ts"
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
 * Async function that resolves to the current user ID
 * Used for audit functionality to track which user initiated an action
 */
export type UserIdResolver = () => Promise<string>

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
  TPathway extends Record<string, unknown> = {},
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
  private readonly writers: Record<TWritablePaths, (SendWebhook<TPathway[TWritablePaths]> | SendFilehook)> =
    {} as Record<
      TWritablePaths,
      (SendWebhook<TPathway[TWritablePaths]> | SendFilehook)
    >
  private readonly batchWriters: Record<TWritablePaths, SendWebhookBatch<TPathway[TWritablePaths]>> = {} as Record<
    TWritablePaths,
    SendWebhookBatch<TPathway[TWritablePaths]>
  >
  private readonly schemas: Record<keyof TPathway, TSchema> = {} as Record<keyof TPathway, TSchema>
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
  }: {
    baseUrl: string
    tenant: string
    dataCore: string
    apiKey: string
    pathwayTimeoutMs?: number
    logger?: Logger
    enableSessionUserResolvers?: boolean
    overrideSessionUserResolvers?: SessionUserResolver
  }) {
    // Initialize logger (use NoopLogger if none provided)
    this.logger = logger ?? new NoopLogger()

    // Store configuration values for cloning
    this.baseUrl = baseUrl
    this.tenant = tenant
    this.dataCore = dataCore
    this.apiKey = apiKey

    if (enableSessionUserResolvers) {
      this.sessionUserResolvers = overrideSessionUserResolvers ?? new SessionUser()
    }

    this.logger.debug("Initializing PathwaysBuilder", {
      baseUrl,
      tenant,
      dataCore,
      pathwayTimeoutMs,
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
  withSessionUserResolver(sessionId: string, resolver: UserIdResolver): PathwaysBuilder<TPathway, TWritablePaths> {
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
      try {
        const isValid = Value.Check(this.schemas[pathway], data.payload)
        if (!isValid) {
          const error = `Event payload does not match schema for pathway ${pathwayStr}`
          this.logger.error(error)
          throw new Error(error)
        }
      } catch (err) {
        const error = `Error validating event payload against schema for pathway ${pathwayStr}: ${
          err instanceof Error ? err.message : String(err)
        }`
        this.logger.error(error)
        throw new Error(error)
      }
    }

    // Call audit handler if configured
    if (this.auditHandler) {
      this.logger.debug(`Calling audit handler for pathway`, {
        pathway: pathwayStr,
        eventId: data.eventId,
      })
      this.auditHandler(pathwayStr, data)
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
   * @template S The schema type extending TSchema
   * @template W Boolean indicating if the pathway is writable (defaults to true)
   * @param contract The pathway contract describing the pathway
   * @returns The PathwaysBuilder instance with the new pathway registered
   */
  register<
    F extends string,
    E extends string,
    S extends TSchema,
    W extends boolean = true,
  >(
    contract: PathwayContract<F, E, S> & { writable?: W; maxRetries?: number; retryDelayMs?: number },
  ): PathwaysBuilder<
    TPathway & Record<PathwayKey<F, E>, Static<S>>,
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
        this.writers[path as TWritablePaths] = this.webhookBuilderFactory()
          .buildFileWebhook(contract.flowType, contract.eventType).send as SendFilehook
      } else {
        this.writers[path as TWritablePaths] = this.webhookBuilderFactory()
          .buildWebhook<TPathway[keyof TPathway]>(contract.flowType, contract.eventType).send as SendWebhook<
            TPathway[keyof TPathway]
          >
        this.batchWriters[path as TWritablePaths] = this.webhookBuilderFactory()
          .buildWebhook<TPathway[keyof TPathway]>(contract.flowType, contract.eventType).sendBatch as SendWebhookBatch<
            TPathway[keyof TPathway]
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

    this.schemas[path] = contract.schema
    this.writable[path] = writable

    this.logger.info(`Pathway registered successfully`, {
      pathway: path,
      flowType: contract.flowType,
      eventType: contract.eventType,
      writable,
    })

    return this as PathwaysBuilder<
      TPathway & Record<PathwayKey<F, E>, Static<S>>,
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
  get<TPath extends keyof TPathway>(path: TPath): TPathway[TPath] {
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
    handler: (event: Omit<FlowcoreEvent, "payload"> & { payload: TPathway[TPath] }) => Promise<void> | void,
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
    handler: (event: Omit<FlowcoreEvent, "payload"> & { payload: TPathway[TPath] }) => void,
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
    handler: (error: Error, event: Omit<FlowcoreEvent, "payload"> & { payload: TPathway[TPath] }) => void,
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
      handler(payload.error, payload.event as Omit<FlowcoreEvent, "payload"> & { payload: TPathway[TPath] })

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
  async write<TPath extends TWritablePaths>(
    path: TPath,
    data: TPathway[TPath],
    metadata?: EventMetadata,
    options?: PathwayWriteOptions,
  ): Promise<string | string[]> {
    const pathStr = String(path)

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

    const schema = this.schemas[path]
    if (!Value.Check(schema, data)) {
      const errorMessage = `Invalid data for pathway ${pathStr}`
      this.logger.error(errorMessage, new Error(errorMessage), {
        pathway: pathStr,
        schema: schema.toString(),
      })
      throw new Error(errorMessage)
    }

    // Create a copy of the metadata to avoid modifying the original
    const finalMetadata: EventMetadata = metadata ? { ...metadata } : {}

    // Check for session-specific user resolver
    let userId: string | undefined
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
      if (auditMode === "system") {
        finalMetadata["audit/user-id"] = "system"
        finalMetadata["audit/on-behalf-of"] = userId
        finalMetadata["audit/mode"] = "system"
      } else {
        finalMetadata["audit/user-id"] = userId
        finalMetadata["audit/mode"] = "user" // Always set mode for user
      }
    }
    let eventIds: string | string[] = []
    if (this.filePathways.has(path)) {
      this.logger.debug(`Writing file data to pathway`, { pathway: pathStr })
      const fileData = data as unknown as WebhookFileData
      eventIds = await (this.writers[path] as SendFilehook)(fileData, finalMetadata, options)
    } else {
      this.logger.debug(`Writing webhook data to pathway`, { pathway: pathStr })
      eventIds = await (this.writers[path] as SendWebhook<TPathway[TPath]>)(data, finalMetadata, options)
    }

    this.logger.info(`Successfully wrote to pathway`, {
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

  async writeBatch<TPath extends TWritablePaths>(
    path: TPath,
    data: TPathway[TPath][],
    metadata?: EventMetadata,
    options?: PathwayWriteOptions,
  ): Promise<string | string[]> {
    const pathStr = String(path)

    this.logger.debug(`Writing batch to pathway`, {
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

    const schema = this.schemas[path]
    if (!Value.Check(Type.Array(schema), data)) {
      const errorMessage = `Invalid batch data for pathway ${pathStr}`
      this.logger.error(errorMessage, new Error(errorMessage), {
        pathway: pathStr,
        schema: schema.toString(),
      })
      throw new Error(errorMessage)
    }

    // Create a copy of the metadata to avoid modifying the original
    const finalMetadata: EventMetadata = metadata ? { ...metadata } : {}

    // Check for session-specific user resolver
    let userId: string | undefined
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
      if (auditMode === "system") {
        finalMetadata["audit/user-id"] = "system"
        finalMetadata["audit/on-behalf-of"] = userId
        finalMetadata["audit/mode"] = "system"
      } else {
        finalMetadata["audit/user-id"] = userId
        finalMetadata["audit/mode"] = "user" // Always set mode for user
      }
    }
    let eventIds: string | string[] = []
    this.logger.debug(`Writing batch webhook data to pathway`, { pathway: pathStr })
    eventIds = await (this.batchWriters[path] as SendWebhookBatch<TPathway[TPath]>)(data, finalMetadata, options)

    this.logger.info(`Successfully wrote to pathway`, {
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
}
