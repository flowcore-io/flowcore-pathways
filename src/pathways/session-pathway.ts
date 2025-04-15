import type { PathwaysBuilder, UserIdResolver } from "./builder.ts"
import type { EventMetadata, PathwayWriteOptions } from "./types.ts"

/**
 * Generates a UUID v4 in a cross-platform compatible way (Deno, Bun, Node.js)
 * @returns A random UUID v4 string
 */
function generateUUID(): string {
  // Check for Deno or browser crypto.randomUUID support
  if (typeof crypto !== "undefined" && crypto.randomUUID) {
    return crypto.randomUUID()
  }

  // Fallback to manual UUID generation (compatible with all platforms)
  // Implementation based on RFC4122 version 4
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (c) => {
    const r = Math.random() * 16 | 0
    const v = c === "x" ? r : (r & 0x3 | 0x8)
    return v.toString(16)
  })
}

/**
 * SessionPathwayBuilder wraps a PathwaysBuilder instance and automatically
 * associates a session ID with all pathway writes.
 *
 * This provides a convenient way to track operations within a user session
 * by automatically including the session ID in metadata.
 *
 * Key features:
 * - Automatic session ID generation if none is provided
 * - Cross-platform UUID generation (works in Deno, Bun, and Node.js)
 * - Simple API for accessing the current session ID
 * - Convenient integration with session-specific user resolvers
 * - Automatic inclusion of session ID in all write operations
 * - Support for overriding the session ID on specific writes
 *
 * Use cases:
 * - Tracking user actions across multiple pathway writes
 * - Connecting related events in a single user session
 * - Supporting multi-user environments where different users' operations need to be tracked separately
 * - Building user activity logs with session grouping
 *
 * @example
 * ```typescript
 * // Create a session pathway with auto-generated ID
 * const session = new SessionPathwayBuilder(pathwaysBuilder);
 *
 * // Get the auto-generated session ID
 * const sessionId = session.getSessionId();
 *
 * // Register a user resolver for this session
 * session.withUserResolver(async () => getCurrentUserId());
 *
 * // Write events with session context
 * await session.write("order/placed", orderData);
 * await session.write("user/action", actionData);
 *
 * // All events will be associated with the same session ID
 * ```
 */
export class SessionPathwayBuilder<
  // deno-lint-ignore ban-types
  TPathway extends Record<string, unknown> = {},
  TWritablePaths extends keyof TPathway = never,
> {
  private readonly pathwaysBuilder: PathwaysBuilder<TPathway, TWritablePaths>
  private readonly sessionId: string

  /**
   * Creates a new SessionPathwayBuilder
   *
   * @param pathwaysBuilder The configured PathwaysBuilder instance to wrap
   * @param sessionId Optional session ID to associate with all operations. If not provided, one will be generated automatically.
   */
  constructor(
    pathwaysBuilder: PathwaysBuilder<TPathway, TWritablePaths>,
    sessionId?: string,
  ) {
    this.pathwaysBuilder = pathwaysBuilder
    this.sessionId = sessionId ?? generateUUID()
  }

  /**
   * Gets the current session ID
   *
   * @returns The session ID associated with this instance
   */
  getSessionId(): string {
    return this.sessionId
  }

  /**
   * Registers a user resolver for this session
   *
   * This is a convenience method that calls `pathwaysBuilder.withSessionUserResolver`
   * with the current session ID, allowing you to set up a resolver specific to this session
   * without having to manually pass the session ID.
   *
   * The resolver will be called whenever events are written through this session,
   * and the resolved user ID will be included in the event metadata.
   *
   * @param resolver The function that resolves to the user ID for this session
   * @returns The SessionPathwayBuilder instance for chaining
   *
   * @throws Error if the underlying PathwaysBuilder does not have session user resolvers configured
   *
   * @example
   * ```typescript
   * const session = new SessionPathwayBuilder(pathwaysBuilder);
   *
   * // Register a user resolver for this session
   * session.withUserResolver(async () => {
   *   // Get the user ID associated with this session
   *   return getUserIdFromSession();
   * });
   *
   * // When writing events, the user ID will be automatically included
   * await session.write("user/action", actionData);
   * ```
   */
  withUserResolver(resolver: UserIdResolver): this {
    this.pathwaysBuilder.withSessionUserResolver(this.sessionId, resolver)
    return this
  }

  /**
   * Writes data to a pathway, proxying to the underlying PathwaysBuilder
   *
   * @param path The pathway to write to
   * @param data The data to write
   * @param metadata Optional metadata to include with the event
   * @param options Optional write options
   * @returns A promise that resolves to the event ID(s)
   */
  async write<TPath extends TWritablePaths>(
    path: TPath,
    data: TPathway[TPath],
    metadata?: EventMetadata,
    options?: PathwayWriteOptions,
  ): Promise<string | string[]> {
    // Create new options object with session ID
    const finalOptions: PathwayWriteOptions = options ? { ...options } : {}

    // Always include the session ID in the options
    finalOptions.sessionId = options?.sessionId ?? this.sessionId

    // The PathwaysBuilder will handle session-specific user resolvers
    return await this.pathwaysBuilder.write(path, data, metadata, finalOptions)
  }
}
