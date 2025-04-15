/**
 * Logger interface that can be implemented by users to capture logs from pathways
 */

/**
 * Metadata interface for logger context
 */
export interface LoggerMeta {
  [key: string]: unknown
  label?: never
  level?: never
  message?: never
  timestamp?: never
}

export interface Logger {
  /**
   * Log debug information
   * @param message The message to log
   * @param context Optional context data to include
   */
  debug(message: string, context?: LoggerMeta): void

  /**
   * Log informational messages
   * @param message The message to log
   * @param context Optional context data to include
   */
  info(message: string, context?: LoggerMeta): void

  /**
   * Log warning messages
   * @param message The message to log
   * @param context Optional context data to include
   */
  warn(message: string, context?: LoggerMeta): void

  /**
   * Log error messages - supports two different method signatures:
   * 1. error(message: string, error?: Error, context?: LoggerMeta)
   * 2. error(messageOrError: string | Error, meta?: LoggerMeta)
   *
   * @param messageOrError The message to log or Error object
   * @param errorOrContext Optional error object or context data
   * @param context Optional context data (only for signature 1)
   */
  error(messageOrError: string | Error, errorOrContext?: Error | LoggerMeta, context?: LoggerMeta): void
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
  debug(message: string, context?: LoggerMeta): void {
    console.debug(message, context ? JSON.stringify(context) : "")
  }

  /**
   * Log informational messages to the console
   * @param message The message to log
   * @param context Optional context data to include
   */
  info(message: string, context?: LoggerMeta): void {
    console.info(message, context ? JSON.stringify(context) : "")
  }

  /**
   * Log warning messages to the console
   * @param message The message to log
   * @param context Optional context data to include
   */
  warn(message: string, context?: LoggerMeta): void {
    console.warn(message, context ? JSON.stringify(context) : "")
  }

  /**
   * Log error messages to the console
   * Supports both signature formats:
   * 1. error(message: string, error?: Error, context?: LoggerMeta)
   * 2. error(messageOrError: string | Error, meta?: LoggerMeta)
   *
   * @param messageOrError The message to log or Error object
   * @param errorOrContext Optional error object or context data
   * @param context Optional context data (only for signature 1)
   */
  error(messageOrError: string | Error, errorOrContext?: Error | LoggerMeta, context?: LoggerMeta): void {
    if (typeof messageOrError === "string") {
      if (errorOrContext instanceof Error) {
        // Signature 1: error(message: string, error: Error, context?: LoggerMeta)
        console.error(messageOrError, errorOrContext, context ? JSON.stringify(context) : "")
      } else {
        // Signature 1 (no error) or Signature 2: error(message: string, context?: LoggerMeta)
        console.error(messageOrError, errorOrContext ? JSON.stringify(errorOrContext) : "")
      }
    } else {
      // Signature 2: error(error: Error, context?: LoggerMeta)
      console.error(messageOrError, errorOrContext ? JSON.stringify(errorOrContext) : "")
    }
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
  debug(_message: string, _context?: LoggerMeta): void {}

  /**
   * No-op info log
   * @param _message The message to log (ignored)
   * @param _context Optional context data (ignored)
   */
  info(_message: string, _context?: LoggerMeta): void {}

  /**
   * No-op warning log
   * @param _message The message to log (ignored)
   * @param _context Optional context data (ignored)
   */
  warn(_message: string, _context?: LoggerMeta): void {}

  /**
   * No-op error log
   * Supports both signature formats
   *
   * @param _messageOrError The message to log or Error object (ignored)
   * @param _errorOrContext Optional error object or context data (ignored)
   * @param _context Optional context data (ignored)
   */
  error(_messageOrError: string | Error, _errorOrContext?: Error | LoggerMeta, _context?: LoggerMeta): void {}
}
