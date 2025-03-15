/**
 * Configuration options for PostgreSQL connection
 * 
 * @interface PostgresConfig
 */
export interface PostgresConfig {
  /** PostgreSQL server hostname */
  host: string;
  /** PostgreSQL server port */
  port: number;
  /** PostgreSQL username */
  user: string;
  /** PostgreSQL password */
  password: string;
  /** PostgreSQL database name */
  database: string;
  /** Whether to use SSL for the connection */
  ssl?: boolean;
}

/**
 * Interface for PostgreSQL database operations
 * 
 * Provides methods for connecting, querying, and executing SQL statements
 * 
 * @interface PostgresAdapter
 */
export interface PostgresAdapter {
  /**
   * Establishes a connection to the PostgreSQL database
   * @returns {Promise<void>}
   */
  connect(): Promise<void>;
  
  /**
   * Closes the connection to the PostgreSQL database
   * @returns {Promise<void>}
   */
  disconnect(): Promise<void>;
  
  /**
   * Executes a SQL query and returns the results
   * @template T The expected result type
   * @param {string} sql The SQL query to execute
   * @param {unknown[]} [params] Optional parameters for the query
   * @returns {Promise<T>} The query results
   */
  query<T>(sql: string, params?: unknown[]): Promise<T>;
  
  /**
   * Executes a SQL statement without returning results
   * @param {string} sql The SQL statement to execute
   * @param {unknown[]} [params] Optional parameters for the statement
   * @returns {Promise<void>}
   */
  execute(sql: string, params?: unknown[]): Promise<void>;
}

// Types for the postgres library
/**
 * Internal interface for the postgres library client
 * @private
 */
interface PostgresClient {
  end: () => Promise<void>;
  unsafe: (sql: string, params?: unknown[]) => Promise<unknown>;
}

/**
 * Internal interface for the postgres module
 * @private
 */
interface PostgresModule {
  default: (connectionString: string) => PostgresClient;
}

/**
 * Implementation of PostgresAdapter using the postgres.js library
 * 
 * @implements {PostgresAdapter}
 */
export class PostgresJsAdapter implements PostgresAdapter {
  /** The postgres.js client factory function */
  private postgres: ((connectionString: string) => PostgresClient) | null = null;
  /** The active postgres.js client */
  private sql: PostgresClient | null = null;
  /** The PostgreSQL configuration */
  private config: PostgresConfig;
  /** The connection string built from the configuration */
  private connectionString: string;

  /**
   * Creates a new PostgresJsAdapter instance
   * 
   * @param {PostgresConfig} config The PostgreSQL connection configuration
   */
  constructor(config: PostgresConfig) {
    this.config = config;
    this.connectionString = `postgres://${config.user}:${config.password}@${config.host}:${config.port}/${config.database}`;
    if (config.ssl) {
      this.connectionString += "?sslmode=require";
    }
  }

  /**
   * Establishes a connection to the PostgreSQL database
   * 
   * @returns {Promise<void>}
   * @throws {Error} If connection fails
   */
  async connect(): Promise<void> {
    try {
      const module = await import("postgres") as PostgresModule;
      this.postgres = module.default;
      this.sql = this.postgres(this.connectionString);
    } catch (error) {
      console.error("Failed to connect to PostgreSQL:", error);
      throw error;
    }
  }

  /**
   * Closes the connection to the PostgreSQL database
   * 
   * @returns {Promise<void>}
   */
  async disconnect(): Promise<void> {
    if (this.sql) {
      await this.sql.end();
      this.sql = null;
    }
  }

  /**
   * Executes a SQL query and returns the results
   * 
   * @template T The expected result type
   * @param {string} sql The SQL query to execute
   * @param {unknown[]} [params=[]] Optional parameters for the query
   * @returns {Promise<T>} The query results
   */
  async query<T>(sql: string, params: unknown[] = []): Promise<T> {
    if (!this.sql) {
      await this.connect();
    }
    return await this.sql!.unsafe(sql, params) as T;
  }

  /**
   * Executes a SQL statement without returning results
   * 
   * @param {string} sql The SQL statement to execute
   * @param {unknown[]} [params=[]] Optional parameters for the statement
   * @returns {Promise<void>}
   */
  async execute(sql: string, params: unknown[] = []): Promise<void> {
    if (!this.sql) {
      await this.connect();
    }
    await this.sql!.unsafe(sql, params);
  }
}

/**
 * Creates and initializes a PostgreSQL adapter
 * 
 * @param {PostgresConfig} config The PostgreSQL connection configuration
 * @returns {Promise<PostgresAdapter>} An initialized PostgreSQL adapter
 */
export async function createPostgresAdapter(config: PostgresConfig): Promise<PostgresAdapter> {
  const adapter = new PostgresJsAdapter(config);
  await adapter.connect();
  return adapter;
} 