import type { WebhookBuilder as WebhookBuilderType } from "@flowcore/sdk-transformer-core"
import type { Static, TSchema } from "@sinclair/typebox"
import { Value } from "@sinclair/typebox/value"
import { Subject } from "rxjs"
import { WebhookBuilder } from "../compatibility/flowcore-transformer-core.sdk.ts"
import type { FlowcoreEvent } from "../contracts/event.ts"
import { InternalPathwayState } from "./internal-pathway.state.ts"
import type { EventMetadata, PathwayContract, PathwayKey, PathwayState, PathwayWriteOptions, SendWebhook, WritablePathway } from "./types.ts"

const DEFAULT_PATHWAY_TIMEOUT_MS = 10000

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
  private readonly writers: Record<TWritablePaths, SendWebhook<TPathway[TWritablePaths]>> = {} as Record<
    TWritablePaths,
    SendWebhook<TPathway[TWritablePaths]>
  >
  private readonly schemas: Record<keyof TPathway, TSchema> = {} as Record<keyof TPathway, TSchema>
  private readonly writable: Record<keyof TPathway, boolean> = {} as Record<keyof TPathway, boolean>
  private readonly timeouts: Record<keyof TPathway, number> = {} as Record<keyof TPathway, number>
  private readonly webhookBuilderFactory: () => WebhookBuilderType
  private pathwayState: PathwayState = new InternalPathwayState()
  private pathwayTimeoutMs: number = DEFAULT_PATHWAY_TIMEOUT_MS

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

  public async processPathway(pathway: keyof TPathway, data: FlowcoreEvent) {
    if (!this.pathways[pathway]) {
      throw new Error(`Pathway ${String(pathway)} not found`)
    }

    if (this.handlers[pathway]) {
      const handle = this.handlers[pathway](data)

      this.beforeObservable[pathway].next(data)

      await handle

      this.afterObservers[pathway].next(data)

      await this.pathwayState.setProcessed(data.eventId)
    } else {
      this.beforeObservable[pathway].next(data)
    }
  }

  registerPathway<
    F extends string,
    E extends string,
    S extends TSchema,
    W extends boolean = true
  >(
    contract: PathwayContract<F, E, S> & { writable?: W }
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
    if (writable) {
      this.writers[path as TWritablePaths] = this.webhookBuilderFactory()
        .buildWebhook<TPathway[keyof TPathway]>(contract.flowType, contract.eventType).send as SendWebhook<TPathway[keyof TPathway]>
    }

    if (contract.timeoutMs) {
      this.timeouts[path] = contract.timeoutMs
    }

    this.schemas[path] = contract.schema
    this.writable[path] = writable
    return this as PathwaysBuilder<
      TPathway & Record<PathwayKey<F, E>, Static<S>>,
      TWritablePaths | WritablePathway<PathwayKey<F, E>, W>
    >
  }

  getPathway<TPath extends keyof TPathway>(path: TPath): TPathway[TPath] {
    return this.pathways[path]
  }

  handlePathway<TPath extends keyof TPathway>(path: TPath, handler: (event: FlowcoreEvent) => (Promise<void> | void )): void {
    const pathway = this.pathways[path]
    if (!pathway) {
      throw new Error(`Pathway ${String(path)} not found`)
    }

    if (this.handlers[path]) {
      throw new Error(`Someone is already handling pathway ${String(path)} in this instance`)
    }

    this.handlers[path] = handler
  }

  subscribeToPathway<TPath extends keyof TPathway>(
    path: TPath,
    handler: (event: FlowcoreEvent) => void,
    type: "before" | "after" | "all" = "before",
  ): void {
    if (type === "before") {
      this.beforeObservable[path].subscribe(handler)
    } else if (type === "after") {
      this.afterObservers[path].subscribe(handler)
    } else {
      this.beforeObservable[path].subscribe(handler)
      this.afterObservers[path].subscribe(handler)
    }
  }

  async writeToPathway<TPath extends TWritablePaths>(
    path: TPath,
    data: TPathway[TPath],
    metadata?: EventMetadata,
    options?: PathwayWriteOptions
  ): Promise<void> {
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

    if (options?.fireAndForget) {
      this.writers[path](data, metadata, options).catch((error) => {
        console.error(`Error writing to pathway ${String(path)}`, error)
      })
      return
    }

    const result = await this.writers[path](data, metadata, options)

    await this.waitForPathwayToBeProcessed(result)
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