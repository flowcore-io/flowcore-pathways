import NodeCache from "npm:node-cache";
import type { KvAdapter } from "./kv-adapter.ts";

/**
 * KV adapter implementation for Node.js runtime
 * 
 * Uses node-cache for in-memory key-value storage
 * 
 * @implements {KvAdapter}
 */
export class NodeKvAdapter implements KvAdapter {
  /**
   * The underlying Node.js cache instance
   * @private
   */
  private readonly kv = new NodeCache();

  /**
   * Retrieves a value from the Node.js cache
   * 
   * @template T The expected type of the stored value
   * @param {string} key The key to retrieve
   * @returns {Promise<T | null>} The stored value or null if not found
   */
  async get<T>(key: string): Promise<T | null> {
    const result = await this.kv.get<T>(key);
    return result ?? null;
  }

  /**
   * Stores a value in the Node.js cache with the specified TTL
   * 
   * @param {string} key The key to store the value under
   * @param {unknown} value The value to store
   * @param {number} ttlMs Time-to-live in milliseconds
   * @returns {Promise<void>}
   */
  async set(key: string, value: unknown, ttlMs: number): Promise<void> {
    await this.kv.set(key, value, (ttlMs / 1000));
  }
} 