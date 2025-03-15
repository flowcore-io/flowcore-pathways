import type { FlowcoreEvent } from "../contracts/event.ts"

/**
 * Extends the FlowcoreEvent with legacy fields
 */
export type FlowcoreLegacyEvent = FlowcoreEvent & {
  /**
   * Optional aggregator information for legacy support
   */
  aggregator?: string
}
