import { type KvAdapter, createKvAdapter } from "./kv/kv-adapter.ts";
import type { PathwayState } from "./types.ts";

/**
 * Internal implementation of PathwayState interface that uses KV storage
 * for tracking processed events to prevent duplicate processing
 */
export class InternalPathwayState implements PathwayState {
  /**
   * Default time-to-live for processed event records (5 minutes)
   * @private
   */
  private static readonly DEFAULT_TTL_MS = 5 * 60 * 1000; // 5 minutes
  
  /**
   * The KV adapter instance for storage
   * @private
   */
  private kv: KvAdapter | null = null;

  /**
   * Gets or initializes the KV adapter
   * 
   * @private
   * @returns The KV adapter instance
   */
  private async getKv(): Promise<KvAdapter> {
    if (!this.kv) {
      this.kv = await createKvAdapter();
    }
    return this.kv;
  }

  /**
   * Checks if an event has already been processed
   * 
   * @param eventId The ID of the event to check
   * @returns True if the event has been processed, false otherwise
   */
  async isProcessed(eventId: string): Promise<boolean> {
    const kv = await this.getKv();
    const result = await kv.get<boolean>(eventId);
    return result === true;
  }

  /**
   * Marks an event as processed
   * 
   * @param eventId The ID of the event to mark as processed
   * @returns Promise that resolves when the operation completes
   */
  async setProcessed(eventId: string): Promise<void> {
    const kv = await this.getKv();
    await kv.set(eventId, true, InternalPathwayState.DEFAULT_TTL_MS);
  }
}