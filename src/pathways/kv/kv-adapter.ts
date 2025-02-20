export interface KvAdapter {
  get<T>(key: string): (Promise<T | null> | T | null);
  set(key: string, value: unknown, ttlMs: number): (Promise<void> | void);
}

export async function createKvAdapter(): Promise<KvAdapter> {
  // Check for Bun
  if (typeof Bun !== "undefined") {
    const { BunKvAdapter } = await import("./bun-kv-adapter.ts");
    return new BunKvAdapter();
  }
  
  // Default to node-cache
  const { NodeKvAdapter } = await import("./node-kv-adapter.ts");
  return new NodeKvAdapter();
} 