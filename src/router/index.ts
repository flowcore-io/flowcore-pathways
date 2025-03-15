import type { FlowcoreLegacyEvent } from "../common/flowcore.type.ts"
import type { FlowcoreEvent } from "../contracts/event.ts"
import type { PathwaysBuilder } from "../pathways/index.ts"
import type { Logger } from "../pathways/logger.ts"
import { NoopLogger } from "../pathways/logger.ts"

/**
 * Router class that handles directing events to the appropriate pathway handlers
 * 
 * @class PathwayRouter
 */
export class PathwayRouter {
  private readonly logger: Logger;

  /**
   * Creates a new instance of PathwayRouter
   * 
   * @param {PathwaysBuilder<Record<string, any>>} pathways - The pathways builder instance that contains all registered pathways
   * @param {string} secretKey - Secret key used for authentication when processing events
   * @param {Logger} [logger] - Optional logger instance (defaults to NoopLogger if not provided)
   * @throws {Error} Will throw an error if secretKey is empty or not provided
   */
  constructor(
    // deno-lint-ignore no-explicit-any
    private readonly pathways: PathwaysBuilder<Record<string, any>>,
    private readonly secretKey: string,
    logger?: Logger
  ) {
    this.logger = logger ?? new NoopLogger();
    
    if (!secretKey || secretKey.trim() === "") {
      this.logger.error("Secret key is required for PathwayRouter");
      throw new Error("Secret key is required for PathwayRouter")
    }
    
    this.logger.debug("PathwayRouter initialized");
  }

  /**
   * Processes an incoming event by routing it to the appropriate pathway
   * 
   * @param {FlowcoreLegacyEvent} event - The event to process
   * @param {string} providedSecret - The secret key provided for authentication
   * @returns {Promise<{ success: boolean; message: string }>} Result of the event processing
   * @throws {Error} Will throw an error if authentication fails, pathway is not found, or processing fails
   */
  async processEvent(event: FlowcoreLegacyEvent, providedSecret: string): Promise<{ success: boolean; message: string }> {
    // Validate secret key
    if (!providedSecret || providedSecret !== this.secretKey) {
      this.logger.error("Invalid secret key provided");
      throw new Error("Invalid secret key")
    }

    const compatibleEvent: FlowcoreEvent = {
      ...event,
      ...(event.aggregator ? { flowType: event.aggregator } : {}),
    }

    const pathwayKey = `${compatibleEvent.flowType}/${compatibleEvent.eventType}`
    this.logger.debug(`Processing event for pathway: ${pathwayKey}`, { 
      eventId: compatibleEvent.eventId 
    });
    
    const pathway = this.pathways.get(pathwayKey)
    if (!pathway) {
      const error = `Pathway ${pathwayKey} not found`;
      this.logger.error(error);
      throw new Error(error)
    }
    
    try {
      this.logger.debug(`Delegating event processing to pathway handler`, { 
        pathwayKey, 
        eventId: compatibleEvent.eventId 
      });
      
      await this.pathways.process(pathwayKey, compatibleEvent)
      
      this.logger.debug(`Event successfully processed through pathway`, { 
        pathwayKey, 
        eventId: compatibleEvent.eventId 
      });
      
      return { success: true, message: `Event processed through pathway ${pathwayKey}` }
    } catch (error) {
      const errorMessage = error instanceof Error 
        ? error.message 
        : String(error)
      
      this.logger.error(`Error processing pathway ${pathwayKey}`, {
        error: errorMessage,
        eventId: compatibleEvent.eventId
      });
      
      // Rethrow the error with additional context
      throw new Error(`Failed to process event in pathway ${pathwayKey}: ${errorMessage}`)
    }
  }
}