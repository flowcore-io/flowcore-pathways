import NodeCache from "npm:node-cache";
import type { KvAdapter } from "./kv-adapter.ts";

export class NodeKvAdapter implements KvAdapter {
  private readonly kv = new NodeCache();

  async get<T>(key: string): Promise<T | null> {
    const result = await this.kv.get<T>(key);
    return result ?? null;
  }

  async set(key: string, value: unknown, ttlMs: number): Promise<void> {
    await this.kv.set(key, value, (ttlMs / 1000));
  }
} 