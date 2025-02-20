import type { WebhookBuilder as WebhookBuilderType, WebhookSendOptions } from "@flowcore/sdk-transformer-core"
import type { Static, TSchema } from "@sinclair/typebox"
import { Value } from "@sinclair/typebox/value"
import { Subject } from "rxjs"
import { WebhookBuilder } from "../compatibility/flowcore-transformer-core.sdk.ts"
import type { FlowcoreEvent } from "../contracts/event.ts"
import type { EventMetadata, PathwayContract, PathwayKey, SendWebhook, WritablePathway } from "./types.ts"

// deno-lint-ignore ban-types
export class PathwaysBuilder<
  TPathway extends Record<string, unknown> = {},
  TWritablePaths extends keyof TPathway = never
> {
  private readonly pathways: TPathway = {} as TPathway
  private readonly handlers: Record<keyof TPathway, (event: FlowcoreEvent) => Promise<void>> = {} as Record<
    keyof TPathway,
    (event: FlowcoreEvent) => Promise<void>
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
  private readonly webhookBuilderFactory: () => WebhookBuilderType

  constructor({
    baseUrl,
    tenant,
    dataCore,
    apiKey,
  }: {
    baseUrl: string
    tenant: string
    dataCore: string
    apiKey: string
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

  handlePathway<TPath extends keyof TPathway>(path: TPath, handler: (event: FlowcoreEvent) => Promise<void>): void {
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
    options?: WebhookSendOptions
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

    await this.writers[path](data, metadata, options)
  }
}