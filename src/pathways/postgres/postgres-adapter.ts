export interface PostgresConfig {
  host: string;
  port: number;
  user: string;
  password: string;
  database: string;
  ssl?: boolean;
}

export interface PostgresAdapter {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  query<T>(sql: string, params?: unknown[]): Promise<T>;
  execute(sql: string, params?: unknown[]): Promise<void>;
}

// Types for the postgres library
interface PostgresClient {
  end: () => Promise<void>;
  unsafe: (sql: string, params?: unknown[]) => Promise<unknown>;
}

interface PostgresModule {
  default: (connectionString: string) => PostgresClient;
}

export class PostgresJsAdapter implements PostgresAdapter {
  private postgres: ((connectionString: string) => PostgresClient) | null = null;
  private sql: PostgresClient | null = null;
  private config: PostgresConfig;
  private connectionString: string;

  constructor(config: PostgresConfig) {
    this.config = config;
    this.connectionString = `postgres://${config.user}:${config.password}@${config.host}:${config.port}/${config.database}`;
    if (config.ssl) {
      this.connectionString += "?sslmode=require";
    }
  }

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

  async disconnect(): Promise<void> {
    if (this.sql) {
      await this.sql.end();
      this.sql = null;
    }
  }

  async query<T>(sql: string, params: unknown[] = []): Promise<T> {
    if (!this.sql) {
      await this.connect();
    }
    return await this.sql!.unsafe(sql, params) as T;
  }

  async execute(sql: string, params: unknown[] = []): Promise<void> {
    if (!this.sql) {
      await this.connect();
    }
    await this.sql!.unsafe(sql, params);
  }
}

export async function createPostgresAdapter(config: PostgresConfig): Promise<PostgresAdapter> {
  const adapter = new PostgresJsAdapter(config);
  await adapter.connect();
  return adapter;
} 