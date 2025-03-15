/**
 * Interface for key-value storage adapters
 * 
 * Provides a common interface for different KV storage implementations
 * that can be used for storing pathway state.
 */
export interface KvAdapter {
  /**
   * Retrieves a value from storage by key
   * 
   * @template T The expected type of the stored value
   * @param key The key to retrieve
   * @returns The stored value or null if not found
   */
  get<T>(key: string): (Promise<T | null> | T | null);
  
  /**
   * Stores a value in storage with the specified key and TTL
   * 
   * @param key The key to store the value under
   * @param value The value to store
   * @param ttlMs Time-to-live in milliseconds
   * @returns Promise or void when the operation completes
   */
  set(key: string, value: unknown, ttlMs: number): (Promise<void> | void);
}

/**
 * Creates an appropriate KV adapter based on the runtime environment
 * 
 * Attempts to use Bun KV adapter if running in Bun, falls back to Node adapter otherwise
 * 
 * @returns A KV adapter instance for the current runtime
 */
export async function createKvAdapter(): Promise<KvAdapter> {
  try {
    // Try to import Bun adapter
    const { BunKvAdapter } = await import("./bun-kv-adapter.ts");
    return new BunKvAdapter();
  } catch {
    // Default to node-cache if Bun is not available
    const { NodeKvAdapter } = await import("./node-kv-adapter.ts");
    return new NodeKvAdapter();
  }
} 