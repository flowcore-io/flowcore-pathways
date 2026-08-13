import { createCipheriv, createDecipheriv, createHash, randomBytes } from "node:crypto"
import { Buffer } from "node:buffer"

const ALGORITHM = "aes-256-gcm"
const IV_LENGTH = 12
const TAG_LENGTH = 16
const MIN_KEY_LENGTH = 32

export const ENCRYPTED_PAYLOAD_FIELD = "encryptedPayload"
export const PATHWAY_ENCRYPTED_METADATA_KEY = "pathways/encrypted"
export const PATHWAY_ENCRYPTION_SCHEME_METADATA_KEY = "pathways/encryption-scheme"
export const PATHWAY_ENCRYPTION_SCHEME = "aes-256-gcm-sha256-v1"

export type PathwayEncryptionMode = "none" | "symmetric"

export interface PathwayEncryptionConfig {
  mode?: PathwayEncryptionMode
  key?: string
}

export interface PathwayEncryptionProvider {
  encrypt(plaintext: string): string
  decrypt(payload: string): string
}

export function deriveEncryptionKey(secret: string): Buffer {
  return createHash("sha256").update(secret).digest()
}

export function aesGcmEncrypt(plaintext: string, key: Buffer): string {
  const iv = randomBytes(IV_LENGTH)
  const cipher = createCipheriv(ALGORITHM, key, iv, { authTagLength: TAG_LENGTH })
  const ciphertext = Buffer.concat([cipher.update(plaintext, "utf8"), cipher.final()])
  const authTag = cipher.getAuthTag()
  return `${iv.toString("base64")}.${ciphertext.toString("base64")}.${authTag.toString("base64")}`
}

export function aesGcmDecrypt(payload: string, key: Buffer): string {
  const parts = payload.split(".")
  if (parts.length !== 3) {
    throw new Error("Invalid AES-256-GCM payload format")
  }

  const [ivB64, ciphertextB64, authTagB64] = parts
  try {
    const decipher = createDecipheriv(ALGORITHM, key, Buffer.from(ivB64, "base64"), {
      authTagLength: TAG_LENGTH,
    })
    decipher.setAuthTag(Buffer.from(authTagB64, "base64"))
    return Buffer.concat([
      decipher.update(Buffer.from(ciphertextB64, "base64")),
      decipher.final(),
    ]).toString("utf8")
  } catch {
    throw new Error("AES-256-GCM decryption failed: auth tag mismatch (wrong key or tampered payload)")
  }
}

export function createPathwayEncryptionProvider(
  config?: PathwayEncryptionConfig,
): PathwayEncryptionProvider | null {
  const mode = config?.mode ?? (config?.key ? "symmetric" : "none")
  if (mode === "none") {
    return null
  }

  if (mode !== "symmetric") {
    throw new Error(`Unknown encryption mode: ${String(mode)}`)
  }

  const secret = config?.key
  if (!secret) {
    return null
  }

  if (secret.length < MIN_KEY_LENGTH) {
    throw new Error(
      "Pathways symmetric encryption key must be at least 32 characters (generate with: openssl rand -hex 32)",
    )
  }

  const key = deriveEncryptionKey(secret)
  return {
    encrypt: (plaintext: string) => aesGcmEncrypt(plaintext, key),
    decrypt: (payload: string) => aesGcmDecrypt(payload, key),
  }
}

export function encryptPayloadEnvelope(payload: unknown, provider: PathwayEncryptionProvider): Record<string, string> {
  return {
    [ENCRYPTED_PAYLOAD_FIELD]: provider.encrypt(JSON.stringify(payload)),
  }
}

/**
 * Removes the pathway encryption markers from event metadata.
 *
 * The markers describe the payload as it was written to Flowcore. Once the payload has been
 * decrypted they no longer hold, so they are dropped to keep the event self-consistent and to stop
 * a later pass (cluster mode re-enters `process()` through the cluster event handler) from trying
 * to decrypt the now-plaintext payload.
 *
 * Returns a new object; the input is left untouched.
 */
export function stripPathwayEncryptionMetadata(metadata: unknown): unknown {
  if (!metadata || typeof metadata !== "object") {
    return metadata
  }

  const {
    [PATHWAY_ENCRYPTED_METADATA_KEY]: _encrypted,
    [PATHWAY_ENCRYPTION_SCHEME_METADATA_KEY]: _scheme,
    ...rest
  } = metadata as Record<string, unknown>

  return rest
}

export function decryptPayloadEnvelope(payload: unknown, provider: PathwayEncryptionProvider): unknown {
  const encryptedPayload = typeof payload === "string"
    ? payload
    : payload && typeof payload === "object" && !Array.isArray(payload)
    ? (payload as Record<string, unknown>)[ENCRYPTED_PAYLOAD_FIELD]
    : undefined

  if (typeof encryptedPayload !== "string") {
    throw new Error(`Encrypted pathway payload must be a string or an object with ${ENCRYPTED_PAYLOAD_FIELD}`)
  }

  try {
    return JSON.parse(provider.decrypt(encryptedPayload))
  } catch (error) {
    if (error instanceof SyntaxError) {
      throw new Error("Encrypted pathway payload decrypted to invalid JSON")
    }
    throw error
  }
}
