import { type AnyZodObject, z } from "zod"
import type { WebhookFileData, WebhookSendOptions } from "npm:@flowcore/sdk-transformer-core@^2.3.6"
import { Buffer } from "node:buffer"

/**
 * Helper type to create a custom type error for non-writable pathways
 * @template T The string type to create an error for
 */
type NonWritablePathwayError<T extends string> = T & {
  readonly __nonWritablePathwayError:
    "This pathway is not writable. To make it writable, remove 'writable: false' from the pathway contract."
}

/**
 * Contract for defining a pathway
 * @template F The flow type
 * @template E The event type
 * @template T The schema type
 */
export interface PathwayContract<F extends string, E extends string, T extends AnyZodObject> {
  /**
   * The flow type for this pathway
   */
  flowType: F

  /**
   * The event type for this pathway
   */
  eventType: E

  /**
   * The schema that defines the structure of events for this pathway
   */
  schema?: T

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

/**
 * Creates a string key from flow type and event type in the format `flowType/eventType`
 * @template F The flow type
 * @template E The event type
 */
export type PathwayKey<F extends string, E extends string> = `${F}/${E}`

/**
 * Interface for event metadata, extending Record<string, unknown>
 */
export interface EventMetadata extends Record<string, unknown> {}

/**
 * Function type for sending an event to a webhook
 * @template EventPayload The type of the event payload
 */
export type SendWebhook<EventPayload> = (
  payload: EventPayload,
  metadata?: EventMetadata,
  options?: WebhookSendOptions,
) => Promise<string>

/**
 * Function type for sending batch events to a webhook
 * @template EventPayload The type of the event payload
 */
export type SendWebhookBatch<EventPayload> = (
  payload: EventPayload[],
  metadata?: EventMetadata,
  options?: WebhookSendOptions,
) => Promise<string[]>

/**
 * Function type for sending a file to a webhook
 */
export type SendFilehook = (
  payload: WebhookFileData,
  metadata?: EventMetadata,
  options?: WebhookSendOptions,
) => Promise<string[]>

/**
 * Helper type to create a better error message for non-writable pathways
 * @template T The string type for the pathway
 * @template IsWritable Boolean indicating if the pathway is writable
 */
export type WritablePathway<T extends string, IsWritable extends boolean> = IsWritable extends false
  ? NonWritablePathwayError<T>
  : T

/**
 * Interface for managing pathway processing state
 */
export type PathwayState = {
  /**
   * Checks if an event has been processed
   * @param eventId The ID of the event to check
   * @returns Boolean indicating if the event has been processed
   */
  isProcessed: (eventId: string) => boolean | Promise<boolean>

  /**
   * Marks an event as processed
   * @param eventId The ID of the event to mark as processed
   */
  setProcessed: (eventId: string) => void | Promise<void>
}

/**
 * Options for pathway writes, extending WebhookSendOptions
 */
export type PathwayWriteOptions = WebhookSendOptions & {
  /**
   * If true, doesn't wait for confirmation of event delivery
   */
  fireAndForget?: boolean

  /**
   * Additional HTTP headers to include with the request
   */
  headers?: Record<string, string>

  /**
   * Audit Mode
   * @default "user"
   */
  auditMode?: "user" | "system"

  /**
   * Session ID for this write operation
   * Used to associate the operation with a specific session
   */
  sessionId?: string
}

export const FileInputSchema: z.ZodObject<{
  fileId: z.ZodString
  fileName: z.ZodString
  fileContent: z.ZodType<Buffer<ArrayBufferLike>, z.ZodTypeDef, Buffer<ArrayBufferLike>>
}> = z.object({
  fileId: z.string(),
  fileName: z.string(),
  fileContent: z.instanceof(Buffer),
})

export const FileEventSchema: z.ZodObject<{
  fileName: z.ZodString
  fileType: z.ZodString
  fileSize: z.ZodNumber
  data: z.ZodString
  part: z.ZodNumber
  totalParts: z.ZodNumber
  checksum: z.ZodString
  hashType: z.ZodString
  fileId: z.ZodString
}> = z.object({
  fileName: z.string(),
  fileType: z.string(),
  fileSize: z.number(),
  data: z.string(),
  part: z.number(),
  totalParts: z.number(),
  checksum: z.string(),
  hashType: z.string(),
  fileId: z.string(),
})
