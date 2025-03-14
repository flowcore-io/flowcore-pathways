export interface KvAdapter {
  get<T>(key: string): (Promise<T | null> | T | null);
  set(key: string, value: unknown, ttlMs: number): (Promise<void> | void);
}

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