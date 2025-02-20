import { type KvAdapter, createKvAdapter } from "./kv/kv-adapter.ts";
import type { PathwayState } from "./types.ts";

export class InternalPathwayState implements PathwayState {
  private static readonly DEFAULT_TTL_MS = 5 * 60 * 1000; // 5 minutes
  private kv: KvAdapter | null = null;

  private async getKv(): Promise<KvAdapter> {
    if (!this.kv) {
      this.kv = await createKvAdapter();
    }
    return this.kv;
  }

  async isProcessed(eventId: string): Promise<boolean> {
    const kv = await this.getKv();
    const result = await kv.get<boolean>(eventId);
    return result === true;
  }

  async setProcessed(eventId: string): Promise<void> {
    const kv = await this.getKv();
    await kv.set(eventId, true, InternalPathwayState.DEFAULT_TTL_MS);
  }
}