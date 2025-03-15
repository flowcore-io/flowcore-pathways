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
   * @param error Optional error object
   * @param context Optional context data to include
   */
  error(message: string, error?: Error, context?: Record<string, unknown>): void
}

/**
 * A default console logger that logs to the console
 */
export class ConsoleLogger implements Logger {
  /**
   * Log debug information to the console
   * @param message The message to log
   * @param context Optional context data to include
   */
  debug(message: string, context?: Record<string, unknown>): void {
    console.debug(message, context ? JSON.stringify(context) : '');
  }
  
  /**
   * Log informational messages to the console
   * @param message The message to log
   * @param context Optional context data to include
   */
  info(message: string, context?: Record<string, unknown>): void {
    console.info(message, context ? JSON.stringify(context) : '');
  }
  
  /**
   * Log warning messages to the console
   * @param message The message to log
   * @param context Optional context data to include
   */
  warn(message: string, context?: Record<string, unknown>): void {
    console.warn(message, context ? JSON.stringify(context) : '');
  }
  
  /**
   * Log error messages to the console
   * @param message The message to log
   * @param error Optional error object
   * @param context Optional context data to include
   */
  error(message: string, error?: Error, context?: Record<string, unknown>): void {
    console.error(message, error, context ? JSON.stringify(context) : '');
  }
}

/**
 * A no-operation logger that does nothing
 */
export class NoopLogger implements Logger {
  /**
   * No-op debug log
   * @param _message The message to log (ignored)
   * @param _context Optional context data (ignored)
   */
  debug(_message: string, _context?: Record<string, unknown>): void {}
  
  /**
   * No-op info log
   * @param _message The message to log (ignored)
   * @param _context Optional context data (ignored)
   */
  info(_message: string, _context?: Record<string, unknown>): void {}
  
  /**
   * No-op warning log
   * @param _message The message to log (ignored)
   * @param _context Optional context data (ignored)
   */
  warn(_message: string, _context?: Record<string, unknown>): void {}
  
  /**
   * No-op error log
   * @param _message The message to log (ignored)
   * @param _error Optional error object (ignored)
   * @param _context Optional context data (ignored)
   */
  error(_message: string, _error?: Error, _context?: Record<string, unknown>): void {}
} 