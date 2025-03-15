/**
 * Router module for Flowcore Pathways
 * 
 * This module provides routing functionality to direct incoming events
 * to the appropriate pathway handlers based on flow type and event type.
 * 
 * @module
 */
import type { FlowcoreLegacyEvent } from "../common/flowcore.type.ts"
import type { FlowcoreEvent } from "../contracts/event.ts"
import type { PathwaysBuilder } from "../pathways/index.ts"
import type { Logger } from "../pathways/logger.ts"
import { NoopLogger } from "../pathways/logger.ts"

/**
 * Router class that handles directing events to the appropriate pathway handlers
 */
export class PathwayRouter {
  private readonly logger: Logger;

  /**
   * Creates a new instance of PathwayRouter
   * 
   * @param pathways The pathways builder instance that contains all registered pathways
   * @param secretKey Secret key used for authentication when processing events
   * @param logger Optional logger instance (defaults to NoopLogger if not provided)
   * @throws Error if secretKey is empty or not provided
   */
  constructor(
    // deno-lint-ignore no-explicit-any
    private readonly pathways: PathwaysBuilder<Record<string, any>>,
    private readonly secretKey: string,
    logger?: Logger
  ) {
    this.logger = logger ?? new NoopLogger();
    
    if (!secretKey || secretKey.trim() === "") {
      const errorMsg = "Secret key is required for PathwayRouter";
      this.logger.error(errorMsg, new Error(errorMsg));
      throw new Error(errorMsg)
    }
    
    this.logger.debug("PathwayRouter initialized");
  }

  /**
   * Process an incoming event by routing it to the appropriate pathway handler
   * 
   * @param event The event to process
   * @param providedSecret The secret key provided for authentication
   * @returns Result of the event processing with success status and message
   * @throws Error if authentication fails, pathway is not found, or processing fails
   */
  async processEvent(event: FlowcoreLegacyEvent, providedSecret: string): Promise<{ success: boolean; message: string }> {
    // Validate secret key
    if (!providedSecret || providedSecret !== this.secretKey) {
      const errorMsg = "Invalid secret key";
      this.logger.error(errorMsg, new Error(errorMsg));
      throw new Error(errorMsg)
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
      const errorMsg = `Pathway ${pathwayKey} not found`;
      this.logger.error(errorMsg, new Error(errorMsg));
      throw new Error(errorMsg)
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
      const errorObj = error instanceof Error ? error : new Error(String(error))
      
      this.logger.error(`Error processing pathway ${pathwayKey}`, errorObj, {
        eventId: compatibleEvent.eventId
      });
      
      // Rethrow the error with additional context
      throw new Error(`Failed to process event in pathway ${pathwayKey}: ${errorObj.message}`)
    }
  }
}