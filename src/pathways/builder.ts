import type { WebhookBuilder as WebhookBuilderType, WebhookFileData, WebhookSendOptions } from "@flowcore/sdk-transformer-core"
import type { Static, TSchema } from "@sinclair/typebox"
import { Value } from "@sinclair/typebox/value"
import { Subject } from "rxjs"
import { WebhookBuilder } from "../compatibility/flowcore-transformer-core.sdk.ts"
import type { FlowcoreEvent } from "../contracts/event.ts"
import { InternalPathwayState } from "./internal-pathway.state.ts"
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

  constructor({
    baseUrl,
    tenant,
    dataCore,
    apiKey,
    pathwayTimeoutMs,
  }: {
    baseUrl: string
    tenant: string
    dataCore: string
    apiKey: string
    pathwayTimeoutMs?: number
  }) {
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
    if (!this.pathways[pathway]) {
      throw new Error(`Pathway ${String(pathway)} not found`)
    }

    // Call audit handler if configured
    if (this.auditHandler) {
      this.auditHandler(String(pathway), data)
    }

    if (this.handlers[pathway]) {
      let retryCount = 0;
      const maxRetries = this.maxRetries[pathway] ?? DEFAULT_MAX_RETRIES;
      const retryDelayMs = this.retryDelays[pathway] ?? DEFAULT_RETRY_DELAY_MS;
      
      this.beforeObservable[pathway].next(data)
      
      while (true) {
        try {
          // Execute the handler
          const handle = this.handlers[pathway](data)
          await handle
          
          // If successful, emit success event and mark as processed
          this.afterObservers[pathway].next(data)
          await this.pathwayState.setProcessed(data.eventId)
          return
        } catch (error) {
          // Create error object if needed
          const errorObj = error instanceof Error ? error : new Error(String(error))
          
          // Emit error event with both error and event data
          this.errorObservers[pathway].next({ event: data, error: errorObj })
          
          // Also emit to global error subject
          this.globalErrorSubject.next({ 
            pathway: String(pathway), 
            event: data, 
            error: errorObj 
          })
          
          // Check if we should retry
          if (retryCount < maxRetries) {
            retryCount++
            // Wait for delay before retrying
            await new Promise(resolve => setTimeout(resolve, retryDelayMs * retryCount))
            continue
          }
          
          // If we've exhausted retries, mark as processed to avoid hanging
          await this.pathwayState.setProcessed(data.eventId)
          throw error
        }
      }
    } else {
      // No handler, just emit events and mark as processed
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
    return this as PathwaysBuilder<
      TPathway & Record<PathwayKey<F, E>, Static<S>>,
      TWritablePaths | WritablePathway<PathwayKey<F, E>, W>
    >
  }

  get<TPath extends keyof TPathway>(path: TPath): TPathway[TPath] {
    return this.pathways[path]
  }

  handle<TPath extends keyof TPathway>(path: TPath, handler: (event: FlowcoreEvent) => (Promise<void> | void )): void {
    const pathway = this.pathways[path]
    if (!pathway) {
      throw new Error(`Pathway ${String(path)} not found`)
    }

    if (this.handlers[path]) {
      throw new Error(`Someone is already handling pathway ${String(path)} in this instance`)
    }

    this.handlers[path] = handler
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
    if (!this.pathways[path]) {
      throw new Error(`Pathway ${String(path)} not found`)
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
    if (!this.pathways[path]) {
      throw new Error(`Pathway ${String(path)} not found`)
    }
    
    this.errorObservers[path].subscribe(({ event, error }) => handler(error, event))
  }

  /**
   * Subscribe to errors for all pathways
   * @param handler The handler function that receives the error, event, and pathway name
   */
  onAnyError(
    handler: (error: Error, event: FlowcoreEvent, pathway: string) => void,
  ): void {
    this.globalErrorSubject.subscribe(({ pathway, event, error }) => handler(error, event, pathway))
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
    if (!this.pathways[path]) {
      throw new Error(`Pathway ${String(path)} not found`)
    }

    if (!this.writable[path]) {
      throw new Error(`Pathway ${String(path)} is not writable`)
    }

    const schema = this.schemas[path]
    if (!Value.Check(schema, data)) {
      throw new Error(`Invalid data for pathway ${String(path)}`)
    }

    // Create a copy of the metadata to avoid modifying the original
    const finalMetadata: EventMetadata = metadata ? { ...metadata } : {};
    
    // Process audit metadata if audit is configured
    if (this.userIdResolver) {
      const userId = await this.userIdResolver()
      
      // Determine the audit mode: default is "user" unless explicitly specified as "system"
      const auditMode = (finalMetadata?.["audit/mode"] as AuditMode) || "user"
      
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
      const fileData = data as unknown as WebhookFileData
      eventIds = await (this.writers[path] as SendFilehook)(fileData, finalMetadata, options)
    } else {
      eventIds = await (this.writers[path] as SendWebhook<TPathway[TPath]>)(data, finalMetadata, options)
    }

    if (!options?.fireAndForget) {
      await Promise.all(Array.isArray(eventIds) ? eventIds.map(this.waitForPathwayToBeProcessed) : [this.waitForPathwayToBeProcessed(eventIds)])
    }

    return eventIds
  }

  private async waitForPathwayToBeProcessed(eventId: string): Promise<void> {
    const startTime = Date.now()
    const timeoutMs = this.timeouts[eventId] ?? this.pathwayTimeoutMs

    while (!(await this.pathwayState.isProcessed(eventId))) {
      if (Date.now() - startTime > timeoutMs) {
        throw new Error(`Pathway processing timed out after ${timeoutMs}ms for event ${eventId}`)
      }
      await new Promise((resolve) => setTimeout(resolve, 100))
    }
  }
}