/**
 * Logger interface that can be implemented by users to capture logs from pathways
 */
export interface Logger {
  /**
   * Log debug information
   * @param message The message to log
   * @param context Optional context data to include
   */
  debug(message: string, context?: Record<string, unknown>): void
  
  /**
   * Log informational messages
   * @param message The message to log
   * @param context Optional context data to include
   */
  info(message: string, context?: Record<string, unknown>): void
  
  /**
   * Log warning messages
   * @param message The message to log
   * @param context Optional context data to include
   */
  warn(message: string, context?: Record<string, unknown>): void
  
  /**
   * Log error messages
   * @param message The message to log
   * @param context Optional context data to include
   */
  error(message: string, context?: Record<string, unknown>): void
}

/**
 * A default console logger that logs to the console
 */
export class ConsoleLogger implements Logger {
  debug(message: string, context?: Record<string, unknown>): void {
    console.debug(message, context ? JSON.stringify(context) : '');
  }
  
  info(message: string, context?: Record<string, unknown>): void {
    console.info(message, context ? JSON.stringify(context) : '');
  }
  
  warn(message: string, context?: Record<string, unknown>): void {
    console.warn(message, context ? JSON.stringify(context) : '');
  }
  
  error(message: string, context?: Record<string, unknown>): void {
    console.error(message, context ? JSON.stringify(context) : '');
  }
}

/**
 * A no-operation logger that does nothing
 */
export class NoopLogger implements Logger {
  debug(_message: string, _context?: Record<string, unknown>): void {}
  info(_message: string, _context?: Record<string, unknown>): void {}
  warn(_message: string, _context?: Record<string, unknown>): void {}
  error(_message: string, _context?: Record<string, unknown>): void {}
} 