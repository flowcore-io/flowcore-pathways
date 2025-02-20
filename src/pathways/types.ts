import type { TSchema } from "@sinclair/typebox"
import { WebhookSendOptions } from "@flowcore/sdk-transformer-core"

export interface PathwayContract<F extends string, E extends string, T extends TSchema> {
  flowType: F
  eventType: E
  schema: T
  writable?: boolean
}

export type PathwayKey<F extends string, E extends string> = `${F}/${E}`

export interface EventMetadata extends Record<string, unknown> {}

export type SendWebhook<EventPayload> = (payload: EventPayload, metadata?: EventMetadata, options?: WebhookSendOptions) => Promise<string>