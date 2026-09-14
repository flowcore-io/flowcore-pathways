import { createHash } from "node:crypto"
import { Buffer } from "node:buffer"

/**
 * Flowcore rejects a single event whose payload exceeds this many bytes
 * (`400 Event size exceeds maximum limit of 64000 bytes`). The limit is 64 000, not 65 536.
 */
export const DEFAULT_MAX_EVENT_BYTES = 64_000

/**
 * Default byte budget for one serialized part payload. Kept well under
 * {@link DEFAULT_MAX_EVENT_BYTES} so the metadata header and any server-side
 * framing never push a part over the cap.
 */
export const DEFAULT_PART_BUDGET_BYTES = 45_000

/** Payload field that carries the chunk header on every part event. */
export const CHUNK_ENVELOPE_FIELD = "pathwaysChunk"
/** Payload field that carries the slice (plaintext or ciphertext) on every part event. */
export const CHUNK_DATA_FIELD = "data"
/** Metadata marker present on every part event. */
export const PATHWAY_CHUNKED_METADATA_KEY = "pathways/chunked"
/** Transport scheme identifier written into every chunk header. */
export const PATHWAY_CHUNK_SCHEME = "utf8-split-sha256-v1"

/** Plaintext header carried on every part event. */
export interface ChunkHeader {
  /** Full UUID shared by every part of one logical event. */
  id: string
  /** 1-based part number. */
  part: number
  /** Total number of parts. */
  totalParts: number
  /** SHA-256 hex digest of the full plaintext JSON. */
  digest: string
  /** Transport scheme, see {@link PATHWAY_CHUNK_SCHEME}. */
  scheme: string
}

/** Wire shape of one part event payload. */
export interface ChunkEnvelope {
  [CHUNK_ENVELOPE_FIELD]: ChunkHeader
  [CHUNK_DATA_FIELD]: string
}

/** Options for automatic event chunking on the write side. */
export interface PathwayChunkingConfig {
  /**
   * Turns automatic chunking off even when a chunk store is configured.
   * Default `true`. Chunking is never active without a chunk store.
   */
  enabled?: boolean
  /**
   * Serialized payload size above which an event is split. Default
   * {@link DEFAULT_MAX_EVENT_BYTES}.
   */
  maxEventBytes?: number
  /**
   * Maximum serialized size of one part payload. Default
   * {@link DEFAULT_PART_BUDGET_BYTES}. Must be below `maxEventBytes`.
   */
  partBudgetBytes?: number
}

/** Fully resolved chunking configuration. */
export interface ResolvedPathwayChunkingConfig {
  enabled: boolean
  maxEventBytes: number
  partBudgetBytes: number
}

/**
 * Applies defaults to a chunking configuration and validates the budgets.
 *
 * @throws {Error} When `partBudgetBytes` is not below `maxEventBytes`, or either is not positive.
 */
export function resolvePathwayChunkingConfig(config?: PathwayChunkingConfig): ResolvedPathwayChunkingConfig {
  const resolved: ResolvedPathwayChunkingConfig = {
    enabled: config?.enabled ?? true,
    maxEventBytes: config?.maxEventBytes ?? DEFAULT_MAX_EVENT_BYTES,
    partBudgetBytes: config?.partBudgetBytes ?? DEFAULT_PART_BUDGET_BYTES,
  }
  if (!Number.isInteger(resolved.maxEventBytes) || resolved.maxEventBytes <= 0) {
    throw new Error("chunking.maxEventBytes must be a positive integer")
  }
  if (!Number.isInteger(resolved.partBudgetBytes) || resolved.partBudgetBytes <= 0) {
    throw new Error("chunking.partBudgetBytes must be a positive integer")
  }
  if (resolved.partBudgetBytes > resolved.maxEventBytes) {
    throw new Error("chunking.partBudgetBytes must not exceed chunking.maxEventBytes")
  }
  return resolved
}

/** Size in bytes of the JSON serialization of `value`. */
export function serializedByteLength(value: unknown): number {
  return Buffer.byteLength(JSON.stringify(value), "utf8")
}

/** SHA-256 hex digest of a UTF-8 string. */
export function sha256Hex(text: string): string {
  return createHash("sha256").update(text, "utf8").digest("hex")
}

/**
 * Splits a string into slices whose UTF-8 encoding is at most `maxBytes` long.
 * Never splits inside a multi-byte character or a surrogate pair.
 *
 * @throws {Error} When `maxBytes` is smaller than 4 (the largest UTF-8 character).
 */
export function splitUtf8(text: string, maxBytes: number): string[] {
  if (maxBytes < 4) {
    throw new Error("splitUtf8 needs a budget of at least 4 bytes")
  }
  const slices: string[] = []
  let current = ""
  let currentBytes = 0
  for (const char of text) {
    const charBytes = Buffer.byteLength(char, "utf8")
    if (currentBytes + charBytes > maxBytes) {
      slices.push(current)
      current = ""
      currentBytes = 0
    }
    current += char
    currentBytes += charBytes
  }
  if (current.length > 0 || slices.length === 0) {
    slices.push(current)
  }
  return slices
}

/**
 * Builds one part envelope. Exported for tests and for custom transports.
 */
export function buildChunkEnvelope(header: ChunkHeader, data: string): ChunkEnvelope {
  return {
    [CHUNK_ENVELOPE_FIELD]: header,
    [CHUNK_DATA_FIELD]: data,
  }
}

/**
 * Splits a plaintext payload into part envelopes that each serialize to at most
 * `partBudgetBytes`.
 *
 * `transform` runs on every slice before it is placed in the envelope. Pass the
 * encryption function for encrypted pathways. The transformed slice, not the
 * plaintext slice, is measured against the budget, so ciphertext expansion is
 * accounted for automatically.
 *
 * @param payload The full plaintext payload (any JSON value)
 * @param partBudgetBytes Maximum serialized size of one part payload
 * @param transform Optional per-slice transform, e.g. encryption
 * @param chunkId Optional fixed chunk id (defaults to a fresh UUID)
 * @returns The ordered part envelopes
 * @throws {Error} When no slice size satisfies the budget
 */
export function buildChunkParts(
  payload: unknown,
  partBudgetBytes: number,
  transform: (slice: string) => string = (slice) => slice,
  chunkId: string = crypto.randomUUID(),
): ChunkEnvelope[] {
  const plaintext = JSON.stringify(payload)
  const digest = sha256Hex(plaintext)

  // Envelope overhead with the largest plausible header values, so the first guess is close.
  const probeHeader: ChunkHeader = {
    id: chunkId,
    part: 999_999,
    totalParts: 999_999,
    digest,
    scheme: PATHWAY_CHUNK_SCHEME,
  }
  const envelopeOverhead = serializedByteLength(buildChunkEnvelope(probeHeader, ""))
  // Transformed data grows: base64 (4/3) plus IV, tag and separators for AES-GCM; leave room.
  let sliceBudget = Math.floor((partBudgetBytes - envelopeOverhead) * 3 / 4) - 64

  for (let attempt = 0; attempt < 8; attempt++) {
    if (sliceBudget < 4) {
      break
    }
    const slices = splitUtf8(plaintext, sliceBudget)
    const parts = slices.map((slice, index) =>
      buildChunkEnvelope(
        {
          id: chunkId,
          part: index + 1,
          totalParts: slices.length,
          digest,
          scheme: PATHWAY_CHUNK_SCHEME,
        },
        transform(slice),
      )
    )
    const largest = Math.max(...parts.map(serializedByteLength))
    if (largest <= partBudgetBytes) {
      return parts
    }
    // Shrink proportionally and retry.
    sliceBudget = Math.floor(sliceBudget * partBudgetBytes / largest) - 16
  }

  throw new Error(
    `Unable to split event into parts of at most ${partBudgetBytes} bytes; ` +
      `increase chunking.partBudgetBytes or reduce the payload`,
  )
}

/**
 * Returns the chunk header when `payload` is a part envelope, otherwise `null`.
 *
 * A payload without the envelope field is a plain event. A payload that carries the envelope
 * field but does not form a valid envelope is malformed and throws, so a corrupted part can
 * never fall through to schema validation as if it were a plain event.
 *
 * @throws {Error} When the envelope field is present but malformed, or the scheme is unknown
 */
export function parseChunkEnvelope(payload: unknown): { header: ChunkHeader; data: string } | null {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    return null
  }
  const record = payload as Record<string, unknown>
  if (!(CHUNK_ENVELOPE_FIELD in record)) {
    return null
  }
  const header = record[CHUNK_ENVELOPE_FIELD]
  const data = record[CHUNK_DATA_FIELD]
  const h = header && typeof header === "object" ? header as Record<string, unknown> : null
  if (
    !h ||
    typeof data !== "string" ||
    typeof h.id !== "string" ||
    typeof h.part !== "number" ||
    typeof h.totalParts !== "number" ||
    typeof h.digest !== "string" ||
    typeof h.scheme !== "string"
  ) {
    throw new Error("Malformed pathway chunk envelope")
  }
  if (h.scheme !== PATHWAY_CHUNK_SCHEME) {
    throw new Error(`Unsupported pathway chunk scheme: ${h.scheme}`)
  }
  if (!Number.isInteger(h.part) || !Number.isInteger(h.totalParts) || h.part < 1 || h.part > h.totalParts) {
    throw new Error(`Invalid pathway chunk header: part ${h.part} of ${h.totalParts}`)
  }
  return {
    header: {
      id: h.id,
      part: h.part,
      totalParts: h.totalParts,
      digest: h.digest,
      scheme: h.scheme,
    },
    data,
  }
}

/**
 * Joins ordered plaintext slices, verifies the digest and parses the JSON.
 *
 * @throws {Error} When the digest does not match or the text is not valid JSON
 */
export function joinChunkParts(slices: string[], expectedDigest: string): unknown {
  const plaintext = slices.join("")
  const digest = sha256Hex(plaintext)
  if (digest !== expectedDigest) {
    throw new Error("Pathway chunk digest mismatch after reassembly (missing, corrupted or conflicting part)")
  }
  try {
    return JSON.parse(plaintext)
  } catch {
    throw new Error("Pathway chunk reassembly produced invalid JSON")
  }
}

/** True when the metadata carries the chunk marker. */
export function hasChunkedMetadata(metadata: unknown): boolean {
  if (!metadata || typeof metadata !== "object") {
    return false
  }
  const value = (metadata as Record<string, unknown>)[PATHWAY_CHUNKED_METADATA_KEY]
  return value === true || value === "true"
}

/**
 * Removes the chunk marker from event metadata. Returns a new object; the input is untouched.
 * The marker describes the wire form. Once the event is reassembled it no longer holds, and a
 * later pass (cluster mode re-enters `process()`) must not try to reassemble again.
 */
export function stripPathwayChunkMetadata(metadata: unknown): unknown {
  if (!metadata || typeof metadata !== "object") {
    return metadata
  }
  const { [PATHWAY_CHUNKED_METADATA_KEY]: _chunked, ...rest } = metadata as Record<string, unknown>
  return rest
}
