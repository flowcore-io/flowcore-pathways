import type { FlowcoreLegacyEvent } from "../common/flowcore.type.ts"
import type { FlowcoreEvent } from "../contracts/event.ts"
import type { PathwaysBuilder } from "../pathways/index.ts"

export class PathwayRouter {
  constructor(
    // deno-lint-ignore no-explicit-any
    private readonly pathways: PathwaysBuilder<Record<string, any>>,
    private readonly secretKey: string
  ) {
    if (!secretKey || secretKey.trim() === "") {
      throw new Error("Secret key is required for PathwayRouter")
    }
  }

  async processEvent(event: FlowcoreLegacyEvent, providedSecret: string) {
    // Validate secret key
    if (!providedSecret || providedSecret !== this.secretKey) {
      throw new Error("Invalid secret key")
    }

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