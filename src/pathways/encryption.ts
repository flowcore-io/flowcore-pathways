import { createCipheriv, createDecipheriv, createHash, randomBytes } from "node:crypto"
import { Buffer } from "node:buffer"

const ALGORITHM = "aes-256-gcm"
const IV_LENGTH = 12
const TAG_LENGTH = 16
const MIN_KEY_LENGTH = 32

export const ENCRYPTED_PAYLOAD_FIELD = "encryptedPayload"
export const PATHWAY_ENCRYPTED_METADATA_KEY = "pathways/encrypted"
export const PATHWAY_ENCRYPTION_SCHEME_METADATA_KEY = "pathways/encryption-scheme"
export const PATHWAY_ENCRYPTION_KEY_ID_METADATA_KEY = "pathways/encryption-key-id"
export const PATHWAY_ENCRYPTION_SCHEME = "aes-256-gcm-sha256-v1"

export type PathwayEncryptionMode = "none" | "symmetric"

export interface PathwayEncryptionConfig {
  mode?: PathwayEncryptionMode
  /** The legacy single key. It remains the fallback for markerless key IDs. */
  key?: string
  /** Optional opaque ID for the legacy single key. */
  keyId?: string
  /** Optional retained keyring. The active key is used for new writes. */
  keyring?: PathwayEncryptionKeyring
}

export interface PathwayEncryptionKeyring {
  /** ID stamped into new event metadata. */
  activeKeyId: string
  /** Opaque key IDs mapped to key material resolved by the application. */
  keys?: Readonly<Record<string, string>>
  /** Optional lazy lookup for retained key material. Return undefined for an unknown ID. */
  resolveKey?: PathwayEncryptionKeyResolver
}

export type PathwayEncryptionKeyResolver = (keyId: string) => string | undefined

export interface PathwayEncryptionProvider {
  encrypt(plaintext: string): string
  decrypt(payload: string, keyId?: string): string
  /** The ID stamped into new encrypted event metadata, when configured. */
  readonly activeKeyId?: string
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
  const mode = config?.mode ?? (config?.key || config?.keyring ? "symmetric" : "none")
  if (mode === "none") {
    return null
  }

  if (mode !== "symmetric") {
    throw new Error(`Unknown encryption mode: ${String(mode)}`)
  }

  const keyring = config?.keyring
  const legacySecret = config?.key
  if (!keyring && !legacySecret) {
    return null
  }

  const validateSecret = (secret: string, label: string): Buffer => {
    if (secret.length < MIN_KEY_LENGTH) {
      throw new Error(
        `Pathways symmetric encryption key for ${label} must be at least 32 characters (generate with: openssl rand -hex 32)`,
      )
    }
    return deriveEncryptionKey(secret)
  }

  const keys = new Map<string, Buffer>()
  const legacyKeyId = config?.keyId?.trim() || undefined
  if (config?.keyId !== undefined && !legacyKeyId) {
    throw new Error("Pathways symmetric encryption keyId must not be empty")
  }
  if (legacySecret) {
    keys.set(legacyKeyId ?? "__legacy__", validateSecret(legacySecret, legacyKeyId ?? "legacy key"))
  }

  let activeKeyId = legacyKeyId
  if (keyring) {
    const configuredActiveKeyId = keyring.activeKeyId.trim()
    if (!configuredActiveKeyId) {
      throw new Error("Pathways symmetric encryption activeKeyId must not be empty")
    }
    if (
      keyring.keys !== undefined &&
      (keyring.keys === null || typeof keyring.keys !== "object" || Array.isArray(keyring.keys))
    ) {
      throw new Error("Pathways symmetric encryption keyring.keys must be a key ID map")
    }
    if (keyring.keys === undefined && typeof keyring.resolveKey !== "function") {
      throw new Error("Pathways symmetric encryption keyring requires keys or resolveKey")
    }
    for (const [keyId, secret] of Object.entries(keyring.keys ?? {})) {
      if (!keyId.trim()) throw new Error("Pathways symmetric encryption keyring contains an empty key ID")
      if (typeof secret !== "string") {
        throw new Error(`Pathways symmetric encryption keyring value for ${keyId} must be a string`)
      }
      keys.set(keyId, validateSecret(secret, `key ID ${keyId}`))
    }
    if (!keys.has(configuredActiveKeyId) && typeof keyring.resolveKey === "function") {
      const resolved = keyring.resolveKey(configuredActiveKeyId)
      if (resolved !== undefined) {
        keys.set(configuredActiveKeyId, validateSecret(resolved, `key ID ${configuredActiveKeyId}`))
      }
    }
    if (!keys.has(configuredActiveKeyId)) {
      throw new Error(`Pathways symmetric encryption active key ID is not present in keyring: ${configuredActiveKeyId}`)
    }
    activeKeyId = configuredActiveKeyId
  }

  const resolveKey = (keyId?: string): Buffer => {
    if (!keyId) {
      if (legacySecret) return keys.get(legacyKeyId ?? "__legacy__") as Buffer
      throw new Error("Encrypted pathway payload is missing its encryption key ID")
    }
    const key = keys.get(keyId)
    if (key) return key
    if (keyring?.resolveKey) {
      const resolved = keyring.resolveKey(keyId)
      if (resolved !== undefined) return validateSecret(resolved, `key ID ${keyId}`)
    }
    throw new Error(`Unknown encryption key ID: ${keyId}`)
  }

  return {
    activeKeyId,
    encrypt: (plaintext: string) => aesGcmEncrypt(plaintext, resolveKey(activeKeyId)),
    decrypt: (payload: string, keyId?: string) => aesGcmDecrypt(payload, resolveKey(keyId)),
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
    [PATHWAY_ENCRYPTION_KEY_ID_METADATA_KEY]: _keyId,
    ...rest
  } = metadata as Record<string, unknown>

  return rest
}

export function getPathwayEncryptionKeyId(metadata: unknown): string | undefined {
  if (!metadata || typeof metadata !== "object") return undefined
  const keyId = (metadata as Record<string, unknown>)[PATHWAY_ENCRYPTION_KEY_ID_METADATA_KEY]
  return typeof keyId === "string" && keyId.length > 0 ? keyId : undefined
}

export function decryptPayloadEnvelope(
  payload: unknown,
  provider: PathwayEncryptionProvider,
  keyId?: string,
): unknown {
  const encryptedPayload = typeof payload === "string"
    ? payload
    : payload && typeof payload === "object" && !Array.isArray(payload)
    ? (payload as Record<string, unknown>)[ENCRYPTED_PAYLOAD_FIELD]
    : undefined

  if (typeof encryptedPayload !== "string") {
    throw new Error(`Encrypted pathway payload must be a string or an object with ${ENCRYPTED_PAYLOAD_FIELD}`)
  }

  try {
    return JSON.parse(provider.decrypt(encryptedPayload, keyId))
  } catch (error) {
    if (error instanceof SyntaxError) {
      throw new Error("Encrypted pathway payload decrypted to invalid JSON")
    }
    throw error
  }
}
