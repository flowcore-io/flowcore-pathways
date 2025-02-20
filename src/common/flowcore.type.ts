import type { FlowcoreEvent } from "../contracts/event.ts"

export type FlowcoreLegacyEvent = FlowcoreEvent & {
  aggregator?: string
}
