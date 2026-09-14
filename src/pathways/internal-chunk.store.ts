import { sha256Hex } from "./chunking.ts"
import type { PathwayChunkStore, PathwayChunkStoreResult, StorePathwayChunkPartInput } from "./types.ts"

interface StoredPart {
  data: string
  dataHash: string
  eventId: string
}

interface StoredChunk {
  totalParts: number
  digest: string
  parts: Map<number, StoredPart>
  assembled: boolean
  expiresAt: number
}

/**
 * In-memory implementation of {@link PathwayChunkStore}.
 *
 * Single-process only: parts held here are invisible to other instances and are
 * lost on restart. Use {@link PostgresPathwayChunkStore} in production.
 */
export class InternalPathwayChunkStore implements PathwayChunkStore {
  /** Default time-to-live for incomplete chunks (1 hour) */
  private static readonly DEFAULT_TTL_MS = 60 * 60 * 1000

  private readonly chunks = new Map<string, StoredChunk>()
  private readonly ttlMs: number

  constructor(options?: { ttlMs?: number }) {
    this.ttlMs = options?.ttlMs ?? InternalPathwayChunkStore.DEFAULT_TTL_MS
  }

  storePart(input: StorePathwayChunkPartInput): Promise<PathwayChunkStoreResult> {
    this.cleanupExpired()

    let chunk = this.chunks.get(input.chunkId)
    if (!chunk) {
      chunk = {
        totalParts: input.totalParts,
        digest: input.digest,
        parts: new Map(),
        assembled: false,
        expiresAt: Date.now() + this.ttlMs,
      }
      this.chunks.set(input.chunkId, chunk)
    }

    if (chunk.totalParts !== input.totalParts || chunk.digest !== input.digest) {
      return Promise.reject(
        new Error(`Inconsistent pathway chunk part ${input.part} for chunk ${input.chunkId} (digest or part count)`),
      )
    }

    const dataHash = sha256Hex(input.data)
    const existing = chunk.parts.get(input.part)
    if (existing) {
      if (existing.dataHash !== dataHash) {
        return Promise.reject(
          new Error(`Conflicting pathway chunk part ${input.part} for chunk ${input.chunkId}`),
        )
      }
      if (chunk.assembled) {
        return Promise.resolve({ status: "duplicate" })
      }
    } else {
      chunk.parts.set(input.part, { data: input.data, dataHash, eventId: input.eventId })
    }

    if (chunk.assembled || chunk.parts.size < chunk.totalParts) {
      return Promise.resolve({ status: existing ? "duplicate" : "stored" })
    }

    chunk.assembled = true
    const ordered = Array.from({ length: chunk.totalParts }, (_, index) => chunk!.parts.get(index + 1)!)
    return Promise.resolve({
      status: "complete",
      parts: ordered.map((part) => part.data),
      partEventIds: ordered.map((part) => part.eventId),
    })
  }

  deleteChunk(chunkId: string): Promise<void> {
    this.chunks.delete(chunkId)
    return Promise.resolve()
  }

  private cleanupExpired(): void {
    const now = Date.now()
    for (const [id, chunk] of this.chunks) {
      if (chunk.expiresAt <= now) {
        this.chunks.delete(id)
      }
    }
  }
}
