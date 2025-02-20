import { BunSqliteKeyValue } from "npm:bun-sqlite-key-value";
import type { KvAdapter } from "./kv-adapter.ts";

export class BunKvAdapter implements KvAdapter {
  private store: BunSqliteKeyValue;

  constructor() {
    this.store = new BunSqliteKeyValue(":memory:");
  }

  get<T>(key: string): T | null {
    const value = this.store.get(key);
    return value as T | null;
  }

  set(key: string, value: unknown, ttlMs: number): void {
    this.store.set(key, value, ttlMs);
  }
} 