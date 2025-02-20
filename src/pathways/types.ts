import type { WebhookSendOptions } from "@flowcore/sdk-transformer-core"
import type { TSchema } from "@sinclair/typebox"
import { FlowcoreEvent } from "../contracts/index.ts"

/**
 * Helper type to create a custom type error for non-writable pathways
 */
type NonWritablePathwayError<T extends string> = T & {
  readonly __nonWritablePathwayError: "This pathway is not writable. To make it writable, remove 'writable: false' from the pathway contract."
}

/**
 * Contract for defining a pathway
 * @template F - The flow type
 * @template E - The event type
 * @template T - The schema type
 */
export interface PathwayContract<F extends string, E extends string, T extends TSchema> {
  flowType: F
  eventType: E
  schema: T
  /**
   * Whether the pathway is writable. Use `false as const` to make the pathway non-writable at compile time.
   * @example
   * ```ts
   * // Non-writable pathway (will not be available in writeToPathway)
   * registerPathway({
   *   flowType: "test",
   *   eventType: "event",
   *   schema: Type.Object({}),
   *   writable: false as const
   * })
   * 
   * // Writable pathway
   * registerPathway({
   *   flowType: "test",
   *   eventType: "writable",
   *   schema: Type.Object({})
   * })
   * ```
   * @default true
   */
  writable?: boolean
  timeoutMs?: number
}

export type PathwayKey<F extends string, E extends string> = `${F}/${E}`

export interface EventMetadata extends Record<string, unknown> {}

export type SendWebhook<EventPayload> = (payload: EventPayload, metadata?: EventMetadata, options?: WebhookSendOptions) => Promise<string>

/**
 * Helper type to create a better error message for non-writable pathways
 */
export type WritablePathway<T extends string, IsWritable extends boolean> = IsWritable extends false ? NonWritablePathwayError<T> : T

export type PathwayState = {
  isProcessed: (eventId: string) => (boolean | Promise<boolean>)
  setProcessed: (eventId: string) => (void | Promise<void>)
}

export type PathwayWriteOptions = WebhookSendOptions & {
  fireAndForget?: boolean
}