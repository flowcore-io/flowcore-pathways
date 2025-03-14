import type { FlowcoreLegacyEvent } from "../common/flowcore.type.ts"
import type { FlowcoreEvent } from "../contracts/event.ts"
import type { PathwaysBuilder } from "../pathways/index.ts"

export class PathwayRouter {
  // deno-lint-ignore no-explicit-any
  constructor(private readonly pathways: PathwaysBuilder<Record<string, any>>) {}

  async processEvent(event: FlowcoreLegacyEvent) {
    const compatibleEvent: FlowcoreEvent = {
      ...event,
      ...(event.aggregator ? { flowType: event.aggregator } : {}),
    }
    const pathwayKey = `${compatibleEvent.flowType}/${compatibleEvent.eventType}`
    const pathway = this.pathways.get(pathwayKey)
    if (!pathway) {
      console.error(`Pathway ${pathwayKey} not found`)
      throw new Error(`Pathway ${pathwayKey} not found`)
    }
    await this.pathways.process(pathwayKey, compatibleEvent)
  }
}

//TODO: handle errors in the pathway
//TODO: handle authentication with secret key