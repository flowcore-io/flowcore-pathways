import type { FlowcoreEvent } from "../contracts/event.ts"

/**
 * Extends the FlowcoreEvent with legacy fields
 * 
 * @typedef {Object} FlowcoreLegacyEvent
 * @property {string} [aggregator] - Optional aggregator information for legacy support
 */
export type FlowcoreLegacyEvent = FlowcoreEvent & {
  aggregator?: string
}
