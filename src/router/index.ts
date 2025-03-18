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
 * 
 * The PathwayRouter serves as a bridge between incoming webhook events and the PathwaysBuilder,
 * ensuring events are routed to the correct pathway handlers based on flow type and event type.
 * 
 * Key features:
 * - Secure authentication using a secret key
 * - Automatic mapping of events to the correct pathway handlers
 * - Compatibility with both legacy and modern Flowcore event formats
 * - Detailed error handling and logging
 * 
 * Use cases:
 * - Building webhook endpoints that receive Flowcore events
 * - Creating API routes that process events from external systems
 * - Implementing event-driven microservices that consume Flowcore events
 * 
 * @example
 * ```typescript
 * // Create a router with authentication
 * const SECRET_KEY = "your-webhook-secret";
 * const router = new PathwayRouter(pathwaysBuilder, SECRET_KEY);
 * 
 * // In your HTTP handler:
 * async function handleWebhook(req: Request) {
 *   const event = await req.json();
 *   const secret = req.headers.get("X-Webhook-Secret");
 *   
 *   try {
 *     const result = await router.processEvent(event, secret);
 *     return new Response(JSON.stringify(result), { 
 *       status: 200,
 *       headers: { "Content-Type": "application/json" }
 *     });
 *   } catch (error) {
 *     console.error("Error processing event:", error);
 *     return new Response(JSON.stringify({ 
 *       error: error.message 
 *     }), { 
 *       status: 401,
 *       headers: { "Content-Type": "application/json" }
 *     });
 *   }
 * }
 * ```
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
   * This method handles the complete lifecycle of an incoming event:
   * 1. Validates the authentication using the provided secret key
   * 2. Maps the event to the correct pathway based on flowType and eventType
   * 3. Delegates processing to the PathwaysBuilder
   * 4. Provides detailed error handling and feedback
   * 
   * The method supports both modern Flowcore events and legacy events that used
   * the "aggregator" field instead of "flowType". It automatically converts legacy
   * events to the modern format before processing.
   * 
   * @param event The event to process, containing flowType, eventType, and payload
   * @param providedSecret The secret key provided for authentication
   * @returns Result of the event processing with success status and message
   * 
   * @throws Error if authentication fails (401 unauthorized)
   * @throws Error if the pathway is not found (404 not found)
   * @throws Error if processing fails (includes the original error message)
   * 
   * @example
   * ```typescript
   * // Basic usage
   * try {
   *   const result = await router.processEvent(incomingEvent, secretFromHeader);
   *   console.log("Success:", result.message);
   * } catch (error) {
   *   console.error("Failed to process event:", error.message);
   * }
   * 
   * // With error handling for different error types
   * try {
   *   const result = await router.processEvent(event, secret);
   *   return { status: 200, body: result };
   * } catch (error) {
   *   if (error.message.includes("Invalid secret key")) {
   *     return { status: 401, body: { error: "Unauthorized" } };
   *   } else if (error.message.includes("not found")) {
   *     return { status: 404, body: { error: "Pathway not found" } };
   *   } else {
   *     return { status: 500, body: { error: "Processing failed" } };
   *   }
   * }
   * ```
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