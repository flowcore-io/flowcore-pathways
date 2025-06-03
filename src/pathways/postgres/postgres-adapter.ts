/**
 * Connection pool configuration options for PostgreSQL
 */
export interface PostgresPoolConfig {
  /** Maximum number of connections in the pool (default: 10) */
  max?: number
  /** Maximum lifetime in seconds for connections (default: random between 45-90 minutes) */
  max_lifetime?: number
  /** Idle connection timeout in seconds (default: 0 - no timeout) */
  idle_timeout?: number
  /** Connect timeout in seconds (default: 30) */
  connect_timeout?: number
}

/**
 * Options that can be passed to postgres.js
 * @private
 */
interface PostgresJsOptions {
  /** Maximum number of connections in the pool */
  max?: number
  /** Maximum lifetime in seconds for connections */
  max_lifetime?: number
  /** Idle connection timeout in seconds */
  idle_timeout?: number
  /** Connect timeout in seconds */
  connect_timeout?: number
  /** Additional postgres.js options */
  [key: string]: unknown
}

/**
 * Configuration for PostgreSQL connection using a connection string
 */
export interface PostgresConnectionStringConfig {
  /** Complete PostgreSQL connection string (e.g., postgres://user:password@host:port/database?sslmode=require) */
  connectionString: string

  /** These properties are not used when a connection string is provided */
  host?: never
  port?: never
  user?: never
  password?: never
  database?: never
  ssl?: never

  /** Connection pool configuration */
  pool?: PostgresPoolConfig
}

/**
 * Configuration for PostgreSQL connection using individual parameters
 */
export interface PostgresParametersConfig {
  /** Not used when individual parameters are provided */
  connectionString?: never

  /** PostgreSQL server hostname */
  host: string
  /** PostgreSQL server port */
  port: number
  /** PostgreSQL username */
  user: string
  /** PostgreSQL password */
  password: string
  /** PostgreSQL database name */
  database: string
  /** Whether to use SSL for the connection */
  ssl?: boolean

  /** Connection pool configuration */
  pool?: PostgresPoolConfig
}

/**
 * Configuration options for PostgreSQL connection
 *
 * Can provide either:
 * 1. A complete connection string, or
 * 2. Individual connection parameters (host, port, user, etc.)
 */
export type PostgresConfig = PostgresConnectionStringConfig | PostgresParametersConfig

/**
 * Interface for PostgreSQL database operations
 *
 * Provides methods for connecting, querying, and executing SQL statements
 */
export interface PostgresAdapter {
  /**
   * Establishes a connection to the PostgreSQL database
   * @returns Promise that resolves when the connection is established
   */
  connect(): Promise<void>

  /**
   * Closes the connection to the PostgreSQL database
   * @returns Promise that resolves when the connection is closed
   */
  disconnect(): Promise<void>

  /**
   * Executes a SQL query and returns the results
   * @template T The expected result type
   * @param sql The SQL query to execute
   * @param params Optional parameters for the query
   * @returns Promise that resolves to the query results
   */
  query<T>(sql: string, params?: unknown[]): Promise<T>

  /**
   * Executes a SQL statement without returning results
   * @param sql The SQL statement to execute
   * @param params Optional parameters for the statement
   * @returns Promise that resolves when the statement has been executed
   */
  execute(sql: string, params?: unknown[]): Promise<void>
}

// Types for the postgres library
/**
 * Internal interface for the postgres library client
 * @private
 */
interface PostgresClient {
  end: () => Promise<void>
  unsafe: (sql: string, params?: unknown[]) => Promise<unknown>
}

/**
 * Internal interface for the postgres module
 * @private
 */
interface PostgresModule {
  default: (connectionString: string, options?: PostgresJsOptions) => PostgresClient
}

/**
 * Implementation of PostgresAdapter using the postgres.js library
 */
export class PostgresJsAdapter implements PostgresAdapter {
  /** The postgres.js client factory function */
  private postgres: ((connectionString: string, options?: PostgresJsOptions) => PostgresClient) | null = null
  /** The active postgres.js client */
  private sql: PostgresClient | null = null
  /** The PostgreSQL configuration */
  private config: PostgresConfig
  /** The connection string built from the configuration */
  private connectionString: string

  /**
   * Creates a new PostgresJsAdapter instance
   *
   * @param config The PostgreSQL connection configuration
   */
  constructor(config: PostgresConfig) {
    this.config = config

    if ("connectionString" in config && config.connectionString) {
      // Use the provided connection string directly
      this.connectionString = config.connectionString
    } else {
      // Build connection string from individual parameters
      this.connectionString =
        `postgres://${config.user}:${config.password}@${config.host}:${config.port}/${config.database}`
      if (config.ssl) {
        this.connectionString += "?sslmode=require"
      }
    }
  }

  /**
   * Establishes a connection to the PostgreSQL database
   *
   * @returns Promise that resolves when connection is established
   * @throws Error if connection fails
   */
  async connect(): Promise<void> {
    try {
      const module = await import("postgres") as PostgresModule
      this.postgres = module.default

      // Build postgres.js options from pool configuration
      const postgresOptions: PostgresJsOptions = {}

      if (this.config.pool) {
        if (this.config.pool.max !== undefined) {
          postgresOptions.max = this.config.pool.max
        }
        if (this.config.pool.max_lifetime !== undefined) {
          postgresOptions.max_lifetime = this.config.pool.max_lifetime
        }
        if (this.config.pool.idle_timeout !== undefined) {
          postgresOptions.idle_timeout = this.config.pool.idle_timeout
        }
        if (this.config.pool.connect_timeout !== undefined) {
          postgresOptions.connect_timeout = this.config.pool.connect_timeout
        }
      }

      this.sql = this.postgres(this.connectionString, postgresOptions)
    } catch (error) {
      console.error("Failed to connect to PostgreSQL:", error)
      throw error
    }
  }

  /**
   * Closes the connection to the PostgreSQL database
   * @returns Promise that resolves when the connection is closed
   */
  async disconnect(): Promise<void> {
    if (this.sql) {
      await this.sql.end()
      this.sql = null
    }
  }

  /**
   * Executes a SQL query and returns the results
   * @template T The expected result type
   * @param sql The SQL query to execute
   * @param params Optional parameters for the query
   * @returns Promise that resolves to the query results
   */
  async query<T>(sql: string, params: unknown[] = []): Promise<T> {
    if (!this.sql) {
      await this.connect()
    }
    return await this.sql!.unsafe(sql, params) as T
  }

  /**
   * Executes a SQL statement without returning results
   * @param sql The SQL statement to execute
   * @param params Optional parameters for the statement
   * @returns Promise that resolves when the statement has been executed
   */
  async execute(sql: string, params: unknown[] = []): Promise<void> {
    if (!this.sql) {
      await this.connect()
    }
    await this.sql!.unsafe(sql, params)
  }
}

/**
 * Creates and initializes a PostgreSQL adapter
 *
 * @param config The PostgreSQL connection configuration (either connectionString or individual parameters)
 * @returns An initialized PostgreSQL adapter
 */
export async function createPostgresAdapter(config: PostgresConfig): Promise<PostgresAdapter> {
  const adapter = new PostgresJsAdapter(config)
  await adapter.connect()
  return adapter
}
