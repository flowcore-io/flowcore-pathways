import { BunSqliteKeyValue } from "bun-sqlite-key-value"
import type { KvAdapter } from "./kv-adapter.ts"

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
  private store: BunSqliteKeyValue
  private static readonly PURGE_INTERVAL_MS = 60_000
  private lastPurgeAttempt = Date.now()

  private purgeExpired(): void {
    const now = Date.now()
    if (now >= this.lastPurgeAttempt && now - this.lastPurgeAttempt < BunKvAdapter.PURGE_INTERVAL_MS) return
    // Throttle attempts, including failures. Maintenance must not turn an already
    // delivered event's completion write into a handler retry and duplicate delivery.
    this.lastPurgeAttempt = now
    try {
      this.store.deleteExpired()
    } catch {
      // Best effort only; actual KV read/write errors still propagate below.
    }
  }

  /**
   * Creates a new in-memory Bun KV adapter
   */
  constructor() {
    this.store = new BunSqliteKeyValue(":memory:")
  }

  /**
   * Retrieves a value from the Bun KV store
   *
   * @template T The expected type of the stored value
   * @param key The key to retrieve
   * @returns The stored value or null if not found
   */
  get<T>(key: string): T | null {
    this.purgeExpired()
    const value = this.store.get(key)
    return value as T | null
  }

  /**
   * Stores a value in the Bun KV store with the specified TTL
   *
   * @param key The key to store the value under
   * @param value The value to store
   * @param ttlMs Time-to-live in milliseconds
   */
  set(key: string, value: unknown, ttlMs: number): void {
    this.purgeExpired()
    this.store.set(key, value, ttlMs)
  }
}
