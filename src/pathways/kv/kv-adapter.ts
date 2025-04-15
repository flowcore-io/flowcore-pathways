/**
 * Interface for key-value storage adapters
 *
 * Provides a common interface for different KV storage implementations
 * that can be used for storing pathway state and other application data.
 *
 * This interface abstracts away the details of specific storage backends,
 * allowing the application to work with different storage providers
 * without changing the core logic.
 *
 * The Flowcore Pathways library includes several implementations of this interface:
 * - BunKvAdapter: Uses Bun's built-in KV store
 * - NodeKvAdapter: Uses node-cache for in-memory storage
 * - DenoKvAdapter: Uses Deno's KV store (when available)
 *
 * Custom implementations can be created for other storage backends
 * by implementing this interface.
 *
 * @example
 * ```typescript
 * // Create a KV adapter
 * const store = await createKvAdapter();
 *
 * // Store a value with a TTL
 * await store.set("session:123", { userId: "user-456" }, 3600000); // 1 hour TTL
 *
 * // Retrieve the value
 * const session = await store.get<{ userId: string }>("session:123");
 * if (session) {
 *   console.log("User ID:", session.userId);
 * }
 * ```
 */
export interface KvAdapter {
  /**
   * Retrieves a value from storage by key
   *
   * @template T The expected type of the stored value
   * @param key The key to retrieve
   * @returns The stored value or null if not found
   */
  get<T>(key: string): Promise<T | null> | T | null

  /**
   * Stores a value in storage with the specified key and TTL
   *
   * @param key The key to store the value under
   * @param value The value to store
   * @param ttlMs Time-to-live in milliseconds
   * @returns Promise or void when the operation completes
   */
  set(key: string, value: unknown, ttlMs: number): Promise<void> | void
}

/**
 * Creates an appropriate KV adapter based on the runtime environment
 *
 * This function automatically detects the current runtime environment and creates
 * the most suitable KV adapter implementation:
 *
 * - In Bun: Returns a BunKvAdapter using Bun's built-in KV store
 * - In Deno with KV access: Returns a DenoKvAdapter
 * - Otherwise: Returns a NodeKvAdapter using an in-memory cache
 *
 * Using this factory function rather than directly instantiating a specific adapter
 * implementation makes your code more portable across different JavaScript runtimes.
 *
 * The adapter is lazily initialized, so any necessary setup only happens when
 * you first interact with the adapter.
 *
 * @returns A KV adapter instance for the current runtime
 *
 * @example
 * ```typescript
 * // Create a runtime-specific KV adapter
 * const kv = await createKvAdapter();
 *
 * // Use with PathwaysBuilder for session user resolvers
 * const pathways = new PathwaysBuilder({
 *   baseUrl: "https://api.flowcore.io",
 *   tenant: "my-tenant",
 *   dataCore: "my-data-core",
 *   apiKey: "my-api-key",
 *   sessionUserResolvers: kv
 * });
 *
 * // Use as a general-purpose key-value store
 * await kv.set("cache:user:123", userData, 60 * 60 * 1000); // 1 hour TTL
 * const cachedUser = await kv.get("cache:user:123");
 * ```
 */
export async function createKvAdapter(): Promise<KvAdapter> {
  try {
    // Try to import Bun adapter
    const { BunKvAdapter } = await import("./bun-kv-adapter.ts")
    return new BunKvAdapter()
  } catch {
    // Default to node-cache if Bun is not available
    const { NodeKvAdapter } = await import("./node-kv-adapter.ts")
    return new NodeKvAdapter()
  }
}
