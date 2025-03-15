import type { TSchema } from "@sinclair/typebox"
import type { WebhookFileData, WebhookSendOptions } from "npm:@flowcore/sdk-transformer-core@^2.3.6"

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
   * // Non-writable pathway (will not be available in write)
   * register({
   *   flowType: "test",
   *   eventType: "event",
   *   schema: Type.Object({}),
   *   writable: false as const
   * })
   * 
   * // Writable pathway
   * register({
   *   flowType: "test",
   *   eventType: "writable",
   *   schema: Type.Object({})
   * })
   * ```
   * @default true
   */
  writable?: boolean
  /**
   * The maximum number of times to retry processing an event if it fails
   * @default 0
   */
  maxRetries?: number
  /**
   * The delay in milliseconds between retry attempts
   * Used as the base for exponential backoff if retries > 1
   * @default 1000
   */
  retryDelayMs?: number
  /**
   * HTTP status codes that should trigger a retry
   * @default [500, 502, 503, 504]
   */
  retryStatusCodes?: number[]
  /**
   * Custom timeout for this pathway in milliseconds
   */
  timeoutMs?: number
  /**
   * Whether this pathway is for file processing
   */
  isFilePathway?: boolean
}

export type PathwayKey<F extends string, E extends string> = `${F}/${E}`

export interface EventMetadata extends Record<string, unknown> {}

export type SendWebhook<EventPayload> = (payload: EventPayload, metadata?: EventMetadata, options?: WebhookSendOptions) => Promise<string>
export type SendFilehook = (payload: WebhookFileData, metadata?: EventMetadata, options?: WebhookSendOptions) => Promise<string[]>

/**
 * Helper type to create a better error message for non-writable pathways
 */
export type WritablePathway<T extends string, IsWritable extends boolean> = IsWritable extends false ? NonWritablePathwayError<T> : T

export type PathwayState = {
  isProcessed: (eventId: string) => (boolean | Promise<boolean>)
  setProcessed: (eventId: string) => (void | Promise<void>)
}

/**
 * Options for pathway writes, extending WebhookSendOptions
 */
export type PathwayWriteOptions = WebhookSendOptions & {
  fireAndForget?: boolean
  headers?: Record<string, string>
}