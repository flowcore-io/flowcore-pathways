import { type AnyZodObject, z } from "zod"
import type { WebhookFileData, WebhookSendOptions } from "@flowcore/sdk-transformer-core"
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

  /**
   * Enables whole-payload encryption for this pathway.
   *
   * When a symmetric key is configured on the builder, writes send a Flowcore-compatible
   * encrypted envelope and processing decrypts marked events before validation.
   */
  encrypted?: boolean

  /**
   * Description for the event type. When provided, the provisioner will create/update
   * this event type on the platform. When undefined, the event type must pre-exist.
   */
  description?: string

  /**
   * Description for the flow type. When provided, the provisioner will create/update
   * this flow type on the platform. When undefined, the flow type must pre-exist.
   */
  flowTypeDescription?: string

  /**
   * Optional pump group. Same `(flowType, pumpGroup)` lands on the same data pump;
   * different `pumpGroup` values within one `flowType` run on independent pumps.
   * Omit (or pass `"default"`) to keep the legacy single-pump-per-flowType behavior.
   *
   * Use to isolate hot event types from cold ones on the same `flowType` so they
   * have independent state cursors, processor concurrency, and restart backoff.
   *
   * NOTE: the WebSocket notifier subscribes at `flowType` scope, so two pump groups
   * on the same `flowType` receive identical notifications and each pulls. Isolation
   * is at processor + state level, not bandwidth. If you need bandwidth isolation,
   * use distinct `flowType`s instead.
   */
  pumpGroup?: string
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
  fileContent: z.ZodType<Buffer>
}> = z.object({
  fileId: z.string(),
  fileName: z.string(),
  fileContent: z.instanceof(Buffer),
})

export type FileInput = z.infer<typeof FileInputSchema>

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

export type FileEvent = z.infer<typeof FileEventSchema>

/**
 * Input for {@link PathwayChunkStore.storePart}
 */
export interface StorePathwayChunkPartInput {
  /** Full UUID shared by every part of one logical event */
  chunkId: string
  /** 1-based part number */
  part: number
  /** Total number of parts in the chunk */
  totalParts: number
  /** SHA-256 hex digest of the full plaintext JSON */
  digest: string
  /** Plaintext slice carried by this part (already decrypted for encrypted pathways) */
  data: string
  /** Flowcore event id of this part event */
  eventId: string
}

/**
 * Result of {@link PathwayChunkStore.storePart}
 *
 * - `stored`: the part was recorded and the chunk is still incomplete
 * - `duplicate`: an identical part was already recorded (exact replay); the chunk may or may not be complete
 * - `complete`: this call recorded the final missing part. Exactly one call per chunk returns this.
 */
export interface PathwayChunkStoreResult {
  status: "stored" | "duplicate" | "complete"
  /** Ordered plaintext slices, present only when `status` is `complete` */
  parts?: string[]
  /** Ordered part event ids, present only when `status` is `complete` */
  partEventIds?: string[]
}

/**
 * Durable store for the parts of an oversized event while they are being collected.
 *
 * Implementations must be safe for concurrent callers on different instances:
 * the same `(chunkId, part)` with identical data is an exact replay and must be
 * accepted; with different data it is a conflict and must throw. Exactly one
 * caller may observe `complete` for a given chunk.
 */
export type PathwayChunkStore = {
  /**
   * Records one part and reports whether the chunk is now complete
   * @param input The part to record
   */
  storePart: (input: StorePathwayChunkPartInput) => Promise<PathwayChunkStoreResult>

  /**
   * Removes every part of a chunk after the logical event has been handled
   * @param chunkId The chunk to remove
   */
  deleteChunk: (chunkId: string) => Promise<void>
}

/**
 * Durable home for the delivery pause of a pathway.
 *
 * A pause issued by the control plane must outlive the process. Without this store a
 * redeploy, a pod restart or a cluster leader change brings the pathway back delivering,
 * silently, while the operator's dashboard still reads "paused".
 *
 * Implementations store an opaque set of pump keys, each `${flowType}::${pumpGroup}`.
 * An empty set means delivery is active everywhere.
 *
 * Keyed by `pathwayKey`, so several deployables can share one database. Use the same
 * value the builder derives from `statePrefix` + pathway name.
 */
export interface PathwayDeliveryStore {
  /** Reads the paused pump keys. Returns an empty array when nothing is paused. */
  getPausedPumps(pathwayKey: string): Promise<string[]>
  /** Replaces the paused pump keys. An empty array clears the pause. */
  setPausedPumps(pathwayKey: string, pumpKeys: string[]): Promise<void>
}

/**
 * In-memory {@link PathwayDeliveryStore}. This is the DEFAULT, and it does NOT survive a
 * process restart. Configure a durable implementation with `withPathwayDeliveryStore()`
 * before relying on a pause in production.
 */
export class InMemoryPathwayDeliveryStore implements PathwayDeliveryStore {
  private readonly paused = new Map<string, string[]>()

  getPausedPumps(pathwayKey: string): Promise<string[]> {
    return Promise.resolve([...(this.paused.get(pathwayKey) ?? [])])
  }

  setPausedPumps(pathwayKey: string, pumpKeys: string[]): Promise<void> {
    if (pumpKeys.length) {
      this.paused.set(pathwayKey, [...pumpKeys])
    } else {
      this.paused.delete(pathwayKey)
    }
    return Promise.resolve()
  }
}
