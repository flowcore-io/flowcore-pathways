import NodeCache from "npm:node-cache@5.1.2"
import type { KvAdapter } from "./kv-adapter.ts"

/**
 * KV adapter implementation for Node.js runtime
 *
 * Uses node-cache for in-memory key-value storage
 */
export class NodeKvAdapter implements KvAdapter {
  /**
   * The underlying Node.js cache instance
   * @private
   */
  private readonly kv = new NodeCache()

  /**
   * Retrieves a value from the Node.js cache
   *
   * @template T The expected type of the stored value
   * @param key The key to retrieve
   * @returns The stored value or null if not found
   */
  async get<T>(key: string): Promise<T | null> {
    const result = await this.kv.get<T>(key)
    return result ?? null
  }

  /**
   * Stores a value in the Node.js cache with the specified TTL
   *
   * @param key The key to store the value under
   * @param value The value to store
   * @param ttlMs Time-to-live in milliseconds
   * @returns Promise that resolves when the operation completes
   */
  async set(key: string, value: unknown, ttlMs: number): Promise<void> {
    await this.kv.set(key, value, ttlMs / 1000)
  }
}
