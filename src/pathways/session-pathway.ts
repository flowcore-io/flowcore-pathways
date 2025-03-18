import type { PathwaysBuilder } from "./builder.ts";
import type { EventMetadata, PathwayWriteOptions } from "./types.ts";

/**
 * Generates a UUID v4 in a cross-platform compatible way (Deno, Bun, Node.js)
 * @returns A random UUID v4 string
 */
function generateUUID(): string {
  // Check for Deno or browser crypto.randomUUID support
  if (typeof crypto !== 'undefined' && crypto.randomUUID) {
    return crypto.randomUUID();
  }
  
  // Fallback to manual UUID generation (compatible with all platforms)
  // Implementation based on RFC4122 version 4
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = Math.random() * 16 | 0;
    const v = c === 'x' ? r : (r & 0x3 | 0x8);
    return v.toString(16);
  });
}

/**
 * SessionPathwayBuilder wraps a PathwaysBuilder instance and automatically
 * associates a session ID with all pathway writes.
 * 
 * This provides a convenient way to track operations within a user session
 * by automatically including the session ID in metadata.
 */
export class SessionPathwayBuilder<
  // deno-lint-ignore ban-types
  TPathway extends Record<string, unknown> = {},
  TWritablePaths extends keyof TPathway = never
> {
  private readonly pathwaysBuilder: PathwaysBuilder<TPathway, TWritablePaths>;
  private readonly sessionId: string;

  /**
   * Creates a new SessionPathwayBuilder
   * 
   * @param pathwaysBuilder The configured PathwaysBuilder instance to wrap
   * @param sessionId Optional session ID to associate with all operations. If not provided, one will be generated automatically.
   */
  constructor(
    pathwaysBuilder: PathwaysBuilder<TPathway, TWritablePaths>,
    sessionId?: string
  ) {
    this.pathwaysBuilder = pathwaysBuilder;
    this.sessionId = sessionId ?? generateUUID();
  }

  /**
   * Gets the current session ID
   * 
   * @returns The session ID associated with this instance
   */
  getSessionId(): string {
    return this.sessionId;
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
    options?: PathwayWriteOptions
  ): Promise<string | string[]> {
    // Create new options object with session ID
    const finalOptions: PathwayWriteOptions = options ? { ...options } : {};
    
    // Always include the session ID in the options
    finalOptions.sessionId = options?.sessionId ?? this.sessionId;
    
    // The PathwaysBuilder will handle session-specific user resolvers
    return await this.pathwaysBuilder.write(path, data, metadata, finalOptions);
  }
} 