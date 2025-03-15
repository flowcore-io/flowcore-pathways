import { BunSqliteKeyValue } from "npm:bun-sqlite-key-value@1.13.1";
import type { KvAdapter } from "./kv-adapter.ts";

/**
 * KV adapter implementation for Bun runtime
 * 
 * Uses Bun's SQLite-based key-value store for storage
 */
export class BunKvAdapter implements KvAdapter {
  /**
   * The underlying Bun SQLite key-value store
   * @private
   */
  private store: BunSqliteKeyValue;

  /**
   * Creates a new in-memory Bun KV adapter
   */
  constructor() {
    this.store = new BunSqliteKeyValue(":memory:");
  }

  /**
   * Retrieves a value from the Bun KV store
   * 
   * @template T The expected type of the stored value
   * @param key The key to retrieve
   * @returns The stored value or null if not found
   */
  get<T>(key: string): T | null {
    const value = this.store.get(key);
    return value as T | null;
  }

  /**
   * Stores a value in the Bun KV store with the specified TTL
   * 
   * @param key The key to store the value under
   * @param value The value to store
   * @param ttlMs Time-to-live in milliseconds
   */
  set(key: string, value: unknown, ttlMs: number): void {
    this.store.set(key, value, ttlMs);
  }
} 