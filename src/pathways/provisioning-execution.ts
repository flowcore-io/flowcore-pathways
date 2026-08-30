export interface ProvisionRetryConfig {
  /** Total attempts, including the initial request. Default: 3. */
  maxAttempts?: number
  /** Initial exponential-backoff delay. Default: 250ms. */
  baseDelayMs?: number
  /** Maximum delay between attempts. Default: 5000ms. */
  maxDelayMs?: number
  /** Random delay variation as a ratio from 0 to 1. Default: 0.2. */
  jitterRatio?: number
}

export interface ResolvedProvisionRetryConfig {
  maxAttempts: number
  baseDelayMs: number
  maxDelayMs: number
  jitterRatio: number
}

export interface ProvisionRetryNotice {
  attempt: number
  maxAttempts: number
  delayMs: number
  status?: number
}

const DEFAULT_PROVISION_RETRY: ResolvedProvisionRetryConfig = {
  maxAttempts: 3,
  baseDelayMs: 250,
  maxDelayMs: 5_000,
  jitterRatio: 0.2,
}

const RETRYABLE_NETWORK_CODES = new Set([
  "ECONNRESET",
  "ECONNREFUSED",
  "EHOSTUNREACH",
  "ENETUNREACH",
  "EPIPE",
  "ETIMEDOUT",
  "EAI_AGAIN",
])

export function resolveProvisionRetryConfig(config?: ProvisionRetryConfig): ResolvedProvisionRetryConfig {
  const resolved = { ...DEFAULT_PROVISION_RETRY, ...config }

  if (!Number.isInteger(resolved.maxAttempts) || resolved.maxAttempts < 1) {
    throw new Error("provisionRetry.maxAttempts must be a positive integer")
  }
  if (!Number.isFinite(resolved.baseDelayMs) || resolved.baseDelayMs < 0) {
    throw new Error("provisionRetry.baseDelayMs must be a non-negative number")
  }
  if (!Number.isFinite(resolved.maxDelayMs) || resolved.maxDelayMs < resolved.baseDelayMs) {
    throw new Error("provisionRetry.maxDelayMs must be greater than or equal to baseDelayMs")
  }
  if (!Number.isFinite(resolved.jitterRatio) || resolved.jitterRatio < 0 || resolved.jitterRatio > 1) {
    throw new Error("provisionRetry.jitterRatio must be between 0 and 1")
  }

  return resolved
}

export function resolveProvisionConcurrency(concurrency?: number): number {
  const resolved = concurrency ?? 4
  if (!Number.isInteger(resolved) || resolved < 1) {
    throw new Error("provisionConcurrency must be a positive integer")
  }
  return resolved
}

export function getProvisionErrorStatus(error: unknown): number | undefined {
  if (typeof error !== "object" || error === null) return undefined

  const direct = (error as { status?: unknown }).status
  if (typeof direct === "number") return direct

  const response = (error as { response?: { status?: unknown } }).response
  return typeof response?.status === "number" ? response.status : undefined
}

function getErrorCode(error: unknown): string | undefined {
  if (typeof error !== "object" || error === null) return undefined
  const direct = (error as { code?: unknown }).code
  if (typeof direct === "string") return direct
  const cause = (error as { cause?: { code?: unknown } }).cause
  return typeof cause?.code === "string" ? cause.code : undefined
}

function getRetryAfterValue(error: unknown): string | null {
  if (typeof error !== "object" || error === null) return null
  const headers = (error as { response?: { headers?: unknown }; headers?: unknown }).response?.headers ??
    (error as { headers?: unknown }).headers
  if (!headers) return null

  if (headers instanceof Headers) return headers.get("retry-after")
  if (typeof headers === "object") {
    const record = headers as Record<string, unknown>
    const value = record["retry-after"] ?? record["Retry-After"]
    return typeof value === "string" || typeof value === "number" ? String(value) : null
  }
  return null
}

function retryAfterMs(error: unknown, now: () => number): number | undefined {
  const value = getRetryAfterValue(error)
  if (!value) return undefined

  const seconds = Number(value)
  if (Number.isFinite(seconds) && seconds >= 0) return seconds * 1_000

  const date = Date.parse(value)
  if (Number.isNaN(date)) return undefined
  return Math.max(0, date - now())
}

export function isRetryableProvisionError(error: unknown): boolean {
  const status = getProvisionErrorStatus(error)
  if (status !== undefined && status !== 0) {
    return status === 408 || status === 429 || status === 500 || status === 502 || status === 503 || status === 504
  }

  const code = getErrorCode(error)
  if (code && RETRYABLE_NETWORK_CODES.has(code)) return true

  if (error instanceof TypeError) return true
  return error instanceof Error &&
    /fetch failed|network error|socket hang up|connection reset|connection refused|timed? out/i.test(error.message)
}

export async function retryProvisionOperation<T>(
  operation: () => Promise<T>,
  config: ResolvedProvisionRetryConfig,
  onRetry?: (notice: ProvisionRetryNotice) => void,
  runtime: {
    sleep?: (delayMs: number) => Promise<void>
    random?: () => number
    now?: () => number
  } = {},
): Promise<T> {
  const sleep = runtime.sleep ?? ((delayMs) => new Promise((resolve) => setTimeout(resolve, delayMs)))
  const random = runtime.random ?? Math.random
  const now = runtime.now ?? Date.now

  for (let attempt = 1;; attempt++) {
    try {
      return await operation()
    } catch (error) {
      if (attempt >= config.maxAttempts || !isRetryableProvisionError(error)) throw error

      const exponentialDelay = Math.min(config.maxDelayMs, config.baseDelayMs * 2 ** (attempt - 1))
      const jitterMultiplier = 1 + (random() * 2 - 1) * config.jitterRatio
      const fallbackDelay = Math.max(0, Math.round(exponentialDelay * jitterMultiplier))
      const delayMs = Math.min(config.maxDelayMs, retryAfterMs(error, now) ?? fallbackDelay)

      onRetry?.({
        attempt,
        maxAttempts: config.maxAttempts,
        delayMs,
        status: getProvisionErrorStatus(error),
      })
      await sleep(delayMs)
    }
  }
}

export async function mapWithConcurrency<T, R>(
  values: readonly T[],
  concurrency: number,
  operation: (value: T, index: number) => Promise<R>,
): Promise<R[]> {
  if (values.length === 0) return []

  const results = new Array<R>(values.length)
  let nextIndex = 0
  let failed = false
  let firstError: unknown

  const workers = Array.from({ length: Math.min(concurrency, values.length) }, async () => {
    while (!failed) {
      const index = nextIndex++
      if (index >= values.length) return

      try {
        results[index] = await operation(values[index], index)
      } catch (error) {
        failed = true
        firstError = error
      }
    }
  })

  await Promise.all(workers)
  if (failed) throw firstError
  return results
}
