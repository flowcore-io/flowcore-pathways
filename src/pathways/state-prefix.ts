import { z } from "zod"

/**
 * Canonical, unprefixed names of every piece of shared state this library owns.
 *
 * These are the exact names used when no `statePrefix` is configured, so existing
 * deployments keep their tables and their lease key value with no migration.
 */
export const DEFAULT_STATE_NAMES = {
  /** Table used by `PostgresPathwayState` for processed-event deduplication. */
  pathwayState: "pathway_state",
  /** Table used by `PostgresPathwayCoordinator` for the leader lease. */
  leases: "pathway_leases",
  /** Table used by `PostgresPathwayCoordinator` for instance registration. */
  instances: "pathway_instances",
  /** Table used by the Postgres pump state manager for pump cursors. */
  pumpState: "pathway_pump_state",
  /** Table used by `PostgresPathwayChunkStore` for parts of oversized events. */
  chunks: "pathway_chunks",
  /** Row key of the cluster leader lease inside the leases table. */
  leaseKey: "pathway-cluster-leader",
} as const

/**
 * Maximum length of a raw `statePrefix`.
 *
 * PostgreSQL truncates identifiers at 63 bytes. The longest name we prefix is
 * `pathway_pump_state` (18 characters) plus the separating underscore, so 44
 * characters of prefix is the most that always fits. We cap at 40 to leave room.
 */
export const MAX_STATE_PREFIX_LENGTH = 40

/**
 * A `statePrefix` becomes part of a SQL identifier through string interpolation,
 * so it is restricted to the characters that are legal in an unquoted PostgreSQL
 * identifier: a leading letter or underscore, then letters, digits or underscores.
 */
const statePrefixSchema = z
  .string()
  .max(MAX_STATE_PREFIX_LENGTH)
  .regex(/^[A-Za-z_][A-Za-z0-9_]*$/)

/**
 * Shared shape for every state configuration that supports prefixing.
 */
export interface StatePrefixConfig {
  /**
   * Namespace for every table and key this library owns in the target database.
   *
   * Set this when two or more deployables share ONE connection string. Without
   * it they contend for the same `pathway_leases` row, only one of them becomes
   * cluster leader, and the others never start their pump.
   *
   * Default: no prefix, which keeps the historical names exactly as they are.
   *
   * Example: `"compute_api"` produces `compute_api_pathway_leases`,
   * `compute_api_pathway_instances`, `compute_api_pathway_pump_state` and the
   * lease key `compute_api_pathway-cluster-leader`.
   */
  statePrefix?: string
}

/**
 * Normalizes a configured state prefix into the literal string that is prepended
 * to each state name.
 *
 * Returns an empty string when no prefix is configured, so callers fall back to
 * the historical names. A configured prefix is returned with exactly one trailing
 * underscore, whether or not the caller supplied one.
 *
 * @param prefix The configured prefix, or `undefined` for no prefix
 * @returns `""` or the prefix with a single trailing underscore
 * @throws {Error} When the prefix contains characters that are illegal in a
 *   PostgreSQL identifier, or is longer than {@link MAX_STATE_PREFIX_LENGTH}
 *
 * @example
 * ```typescript
 * normalizeStatePrefix(undefined)      // ""
 * normalizeStatePrefix("compute_api")  // "compute_api_"
 * normalizeStatePrefix("compute_api_") // "compute_api_"
 * normalizeStatePrefix("drop table;")  // throws
 * ```
 */
export function normalizeStatePrefix(prefix?: string): string {
  if (prefix === undefined || prefix === "") return ""

  const parsed = statePrefixSchema.safeParse(prefix)
  if (!parsed.success) {
    throw new Error(
      `Invalid statePrefix "${prefix}": it must start with a letter or underscore, ` +
        `contain only letters, digits and underscores, and be at most ` +
        `${MAX_STATE_PREFIX_LENGTH} characters long.`,
    )
  }

  const trimmed = parsed.data.replace(/_+$/, "")
  if (trimmed === "") {
    throw new Error(`Invalid statePrefix "${prefix}": it must contain more than underscores.`)
  }

  return `${trimmed}_`
}

/**
 * Applies a configured state prefix to one state name.
 *
 * @param prefix The configured prefix, or `undefined` for no prefix
 * @param baseName One of the {@link DEFAULT_STATE_NAMES} values
 * @returns The prefixed name, or `baseName` unchanged when no prefix is configured
 *
 * @example
 * ```typescript
 * prefixStateName("compute_api", DEFAULT_STATE_NAMES.leases)
 * // "compute_api_pathway_leases"
 *
 * prefixStateName(undefined, DEFAULT_STATE_NAMES.leaseKey)
 * // "pathway-cluster-leader"
 * ```
 */
export function prefixStateName(prefix: string | undefined, baseName: string): string {
  return `${normalizeStatePrefix(prefix)}${baseName}`
}
