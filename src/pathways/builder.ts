import type { Static, TSchema } from "@sinclair/typebox"
import { Value } from "@sinclair/typebox/value"
import type { WebhookBuilder as WebhookBuilderType, WebhookFileData, WebhookSendOptions } from "npm:@flowcore/sdk-transformer-core"
import { Subject } from "rxjs"
import { WebhookBuilder } from "../compatibility/flowcore-transformer-core.sdk.ts"
import type { FlowcoreEvent } from "../contracts/event.ts"
import { InternalPathwayState } from "./internal-pathway.state.ts"
import type { Logger } from "./logger.ts"
import { NoopLogger } from "./logger.ts"
import type { EventMetadata, PathwayContract, PathwayKey, PathwayState, PathwayWriteOptions, SendFilehook, SendWebhook, WritablePathway } from "./types.ts"

const DEFAULT_PATHWAY_TIMEOUT_MS = 10000
const DEFAULT_MAX_RETRIES = 3
const DEFAULT_RETRY_DELAY_MS = 500

// Define audit-related types
export type AuditMode = "user" | "system"
export type AuditHandler = (path: string, event: FlowcoreEvent) => void
export type UserIdResolver = () => Promise<string>

// Extend WebhookSendOptions to add audit-specific options
export interface AuditWebhookSendOptions extends WebhookSendOptions {
  headers?: Record<string, string>
}

export class PathwaysBuilder<
  // deno-lint-ignore ban-types
  TPathway extends Record<string, unknown> = {},
  TWritablePaths extends keyof TPathway = never
> {
  private readonly pathways: TPathway = {} as TPathway
  private readonly handlers: Record<keyof TPathway, (event: FlowcoreEvent) => (Promise<void> | void)> = {} as Record<
    keyof TPathway,
    (event: FlowcoreEvent) => (Promise<void> | void)
  >
  private readonly beforeObservable: Record<keyof TPathway, Subject<FlowcoreEvent>> = {} as Record<
    keyof TPathway,
    Subject<FlowcoreEvent>
  >
  private readonly afterObservers: Record<keyof TPathway, Subject<FlowcoreEvent>> = {} as Record<
    keyof TPathway,
    Subject<FlowcoreEvent>
  >
  private readonly errorObservers: Record<keyof TPathway, Subject<{ event: FlowcoreEvent, error: Error }>> = {} as Record<
    keyof TPathway,
    Subject<{ event: FlowcoreEvent, error: Error }>
  >
  private readonly globalErrorSubject = new Subject<{ pathway: string, event: FlowcoreEvent, error: Error }>()
  private readonly writers: Record<TWritablePaths, (SendWebhook<TPathway[TWritablePaths]> | SendFilehook)> = {} as Record<
    TWritablePaths,
    (SendWebhook<TPathway[TWritablePaths]> | SendFilehook)
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
  
  // Logger instance (but not using it yet due to TypeScript errors)
  private readonly logger: Logger

  constructor({
    baseUrl,
    tenant,
    dataCore,
    apiKey,
    pathwayTimeoutMs,
    logger,
  }: {
    baseUrl: string
    tenant: string
    dataCore: string
    apiKey: string
    pathwayTimeoutMs?: number
    logger?: Logger
  }) {
    // Initialize logger (use NoopLogger if none provided)
    this.logger = logger ?? new NoopLogger();
    
    this.logger.debug('Initializing PathwaysBuilder', {
      baseUrl,
      tenant,
      dataCore,
      pathwayTimeoutMs
    });
    
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

  withPathwayState(state: PathwayState): PathwaysBuilder<TPathway, TWritablePaths> {
    this.logger.debug('Setting custom pathway state');
    this.pathwayState = state
    return this as PathwaysBuilder<TPathway, TWritablePaths>
  }

  /**
   * Configures the PathwaysBuilder to use audit functionality
   * @param handler The handler function that receives pathway and event information
   * @param userIdResolver An async function that resolves to the current user ID
   * @returns The PathwaysBuilder instance with audit configured
   */
  withAudit(handler: AuditHandler, userIdResolver: UserIdResolver): PathwaysBuilder<TPathway, TWritablePaths> {
    this.logger.debug('Configuring audit functionality');
    this.auditHandler = handler
    this.userIdResolver = userIdResolver
    return this as PathwaysBuilder<TPathway, TWritablePaths>
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
      eventId: data.eventId
    });
    
    if (!this.pathways[pathway]) {
      const error = `Pathway ${pathwayStr} not found`;
      this.logger.error(error);
      throw new Error(error)
    }

    // Call audit handler if configured
    if (this.auditHandler) {
      this.logger.debug(`Calling audit handler for pathway`, {
        pathway: pathwayStr,
        eventId: data.eventId
      });
      this.auditHandler(pathwayStr, data)
    }

    if (this.handlers[pathway]) {
      let retryCount = 0;
      const maxRetries = this.maxRetries[pathway] ?? DEFAULT_MAX_RETRIES;
      const retryDelayMs = this.retryDelays[pathway] ?? DEFAULT_RETRY_DELAY_MS;
      
      this.logger.debug(`Emitting 'before' event`, {
        pathway: pathwayStr,
        eventId: data.eventId
      });
      this.beforeObservable[pathway].next(data)
      
      while (true) {
        try {
          this.logger.debug(`Executing handler for pathway`, {
            pathway: pathwayStr,
            eventId: data.eventId,
            attempt: retryCount + 1
          });
          
          // Execute the handler
          const handle = this.handlers[pathway](data)
          await handle
          
          // If successful, emit success event and mark as processed
          this.logger.debug(`Handler executed successfully, emitting 'after' event`, {
            pathway: pathwayStr,
            eventId: data.eventId
          });
          
          this.afterObservers[pathway].next(data)
          await this.pathwayState.setProcessed(data.eventId)
          
          this.logger.info(`Successfully processed pathway event`, {
            pathway: pathwayStr,
            eventId: data.eventId
          });
          
          return
        } catch (error) {
          // Create error object if needed
          const errorObj = error instanceof Error ? error : new Error(String(error))
          
          this.logger.error(`Error processing pathway event`, {
            pathway: pathwayStr,
            eventId: data.eventId,
            error: errorObj.message,
            retryCount,
            maxRetries
          });
          
          // Emit error event with both error and event data
          this.errorObservers[pathway].next({ event: data, error: errorObj })
          
          // Also emit to global error subject
          this.globalErrorSubject.next({ 
            pathway: pathwayStr, 
            event: data, 
            error: errorObj 
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
              nextDelay
            });
            
            // Wait for delay before retrying
            await new Promise(resolve => setTimeout(resolve, nextDelay))
            continue
          }
          
          // If we've exhausted retries, mark as processed to avoid hanging
          this.logger.warn(`Max retries exceeded for pathway event, marking as processed`, {
            pathway: pathwayStr,
            eventId: data.eventId,
            retryCount,
            maxRetries
          });
          
          await this.pathwayState.setProcessed(data.eventId)
          throw error
        }
      }
    } else {
      // No handler, just emit events and mark as processed
      this.logger.debug(`No handler for pathway, emitting events and marking as processed`, {
        pathway: pathwayStr,
        eventId: data.eventId
      });
      
      this.beforeObservable[pathway].next(data)
      this.afterObservers[pathway].next(data)
      await this.pathwayState.setProcessed(data.eventId)
    }
  }

  register<
    F extends string,
    E extends string,
    S extends TSchema,
    W extends boolean = true
  >(
    contract: PathwayContract<F, E, S> & { writable?: W; maxRetries?: number; retryDelayMs?: number }
  ): PathwaysBuilder<
    TPathway & Record<PathwayKey<F, E>, Static<S>>,
    TWritablePaths | WritablePathway<PathwayKey<F, E>, W>
  > {
    const path = `${contract.flowType}/${contract.eventType}` as PathwayKey<F, E>
    const writable = contract.writable ?? true;
    
    this.logger.debug(`Registering pathway`, {
      pathway: path,
      flowType: contract.flowType,
      eventType: contract.eventType,
      writable,
      isFilePathway: contract.isFilePathway,
      timeoutMs: contract.timeoutMs,
      maxRetries: contract.maxRetries,
      retryDelayMs: contract.retryDelayMs
    });
    
    // deno-lint-ignore no-explicit-any
    (this.pathways as any)[path] = true
    this.beforeObservable[path] = new Subject<FlowcoreEvent>()
    this.afterObservers[path] = new Subject<FlowcoreEvent>()
    this.errorObservers[path] = new Subject<{ event: FlowcoreEvent, error: Error }>()
    
    if (writable) {
      if (contract.isFilePathway) {
        this.filePathways.add(path)
        this.writers[path as TWritablePaths] = this.webhookBuilderFactory()
          .buildFileWebhook(contract.flowType, contract.eventType).send as SendFilehook
      } else {
        this.writers[path as TWritablePaths] = this.webhookBuilderFactory()
          .buildWebhook<TPathway[keyof TPathway]>(contract.flowType, contract.eventType).send as SendWebhook<TPathway[keyof TPathway]>
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
      writable
    });
    
    return this as PathwaysBuilder<
      TPathway & Record<PathwayKey<F, E>, Static<S>>,
      TWritablePaths | WritablePathway<PathwayKey<F, E>, W>
    >
  }

  get<TPath extends keyof TPathway>(path: TPath): TPathway[TPath] {
    this.logger.debug(`Getting pathway`, { pathway: String(path) });
    return this.pathways[path]
  }

  handle<TPath extends keyof TPathway>(path: TPath, handler: (event: FlowcoreEvent) => (Promise<void> | void )): void {
    const pathStr = String(path)
    this.logger.debug(`Setting handler for pathway`, { pathway: pathStr });
    
    const pathway = this.pathways[path]
    if (!pathway) {
      const error = `Pathway ${pathStr} not found`;
      this.logger.error(error);
      throw new Error(error)
    }

    if (this.handlers[path]) {
      const error = `Someone is already handling pathway ${pathStr} in this instance`;
      this.logger.error(error);
      throw new Error(error)
    }

    this.handlers[path] = handler
    this.logger.info(`Handler set for pathway`, { pathway: pathStr });
  }

  /**
   * Subscribe to pathway events (before or after processing)
   * @param path The pathway to subscribe to
   * @param handler The handler function for the events
   * @param type The event type to subscribe to (before, after, or all)
   */
  subscribe<TPath extends keyof TPathway>(
    path: TPath,
    handler: (event: FlowcoreEvent) => void,
    type: "before" | "after" | "all" = "before",
  ): void {
    const pathStr = String(path)
    this.logger.debug(`Subscribing to pathway events`, { 
      pathway: pathStr, 
      type 
    });
    
    if (!this.pathways[path]) {
      const error = `Pathway ${pathStr} not found`;
      this.logger.error(error);
      throw new Error(error)
    }

    if (type === "before") {
      this.beforeObservable[path].subscribe(handler)
    } else if (type === "after") {
      this.afterObservers[path].subscribe(handler)
    } else if (type === "all") {
      // Subscribe to both before and after events
      this.beforeObservable[path].subscribe(handler)
      this.afterObservers[path].subscribe(handler)
    }
    
    this.logger.debug(`Subscribed to pathway events`, { 
      pathway: pathStr, 
      type 
    });
  }

  /**
   * Subscribe to errors for a specific pathway
   * @param path The pathway to subscribe to errors for
   * @param handler The handler function that receives the error and event
   */
  onError<TPath extends keyof TPathway>(
    path: TPath,
    handler: (error: Error, event: FlowcoreEvent) => void,
  ): void {
    const pathStr = String(path)
    this.logger.debug(`Subscribing to pathway errors`, { pathway: pathStr });
    
    if (!this.pathways[path]) {
      const error = `Pathway ${pathStr} not found`;
      this.logger.error(error);
      throw new Error(error)
    }
    
    this.errorObservers[path].subscribe(({ event, error }) => handler(error, event))
    this.logger.debug(`Subscribed to pathway errors`, { pathway: pathStr });
  }

  /**
   * Subscribe to errors for all pathways
   * @param handler The handler function that receives the error, event, and pathway name
   */
  onAnyError(
    handler: (error: Error, event: FlowcoreEvent, pathway: string) => void,
  ): void {
    this.logger.debug(`Subscribing to all pathway errors`);
    this.globalErrorSubject.subscribe(({ pathway, event, error }) => handler(error, event, pathway))
    this.logger.debug(`Subscribed to all pathway errors`);
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
    options?: PathwayWriteOptions
  ): Promise<string | string[]> {
    const pathStr = String(path)
    
    this.logger.debug(`Writing to pathway`, { 
      pathway: pathStr,
      metadata,
      options: {
        fireAndForget: options?.fireAndForget
      }
    });
    
    if (!this.pathways[path]) {
      const error = `Pathway ${pathStr} not found`;
      this.logger.error(error);
      throw new Error(error)
    }

    if (!this.writable[path]) {
      const error = `Pathway ${pathStr} is not writable`;
      this.logger.error(error);
      throw new Error(error)
    }

    const schema = this.schemas[path]
    if (!Value.Check(schema, data)) {
      const error = `Invalid data for pathway ${pathStr}`;
      this.logger.error(error, {
        pathway: pathStr,
        schema: schema.toString()
      });
      throw new Error(error)
    }

    // Create a copy of the metadata to avoid modifying the original
    const finalMetadata: EventMetadata = metadata ? { ...metadata } : {};
    
    // Process audit metadata if audit is configured
    if (this.userIdResolver) {
      this.logger.debug(`Resolving user ID for audit metadata`, { pathway: pathStr });
      const userId = await this.userIdResolver()
      
      // Determine the audit mode: default is "user" unless explicitly specified as "system"
      const auditMode = (finalMetadata?.["audit/mode"] as AuditMode) || "user"
      
      this.logger.debug(`Adding audit metadata`, { 
        pathway: pathStr,
        auditMode,
        userId
      });
      
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
      this.logger.debug(`Writing file data to pathway`, { pathway: pathStr });
      const fileData = data as unknown as WebhookFileData
      eventIds = await (this.writers[path] as SendFilehook)(fileData, finalMetadata, options)
    } else {
      this.logger.debug(`Writing webhook data to pathway`, { pathway: pathStr });
      eventIds = await (this.writers[path] as SendWebhook<TPathway[TPath]>)(data, finalMetadata, options)
    }

    this.logger.info(`Successfully wrote to pathway`, { 
      pathway: pathStr,
      eventIds: Array.isArray(eventIds) ? eventIds : [eventIds],
      fireAndForget: options?.fireAndForget
    });

    if (!options?.fireAndForget) {
      this.logger.debug(`Waiting for pathway to be processed`, { 
        pathway: pathStr,
        eventIds: Array.isArray(eventIds) ? eventIds : [eventIds]
      });
      
      await Promise.all(Array.isArray(eventIds) 
        ? eventIds.map(id => this.waitForPathwayToBeProcessed(id)) 
        : [this.waitForPathwayToBeProcessed(eventIds)]
      )
    }

    return eventIds
  }

  private async waitForPathwayToBeProcessed(eventId: string): Promise<void> {
    const startTime = Date.now()
    const timeoutMs = this.timeouts[eventId] ?? this.pathwayTimeoutMs
    
    this.logger.debug(`Waiting for event to be processed`, { 
      eventId,
      timeoutMs
    });
    
    let attempts = 0
    
    while (!(await this.pathwayState.isProcessed(eventId))) {
      attempts++
      const elapsedTime = Date.now() - startTime
      
      if (elapsedTime > timeoutMs) {
        const error = `Pathway processing timed out after ${timeoutMs}ms for event ${eventId}`;
        this.logger.error(error, { 
          eventId,
          timeoutMs,
          elapsedTime,
          attempts
        });
        throw new Error(error)
      }
      
      if (attempts % 10 === 0) { // Log every 10 attempts (1 second)
        this.logger.debug(`Still waiting for event to be processed`, { 
          eventId,
          elapsedTime,
          attempts,
          timeoutMs
        });
      }
      
      await new Promise((resolve) => setTimeout(resolve, 100))
    }
    
    this.logger.debug(`Event has been processed`, { 
      eventId,
      elapsedTime: Date.now() - startTime,
      attempts
    });
  }
}