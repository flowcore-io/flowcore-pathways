import type { Logger } from "./logger.ts"
import type { LogLevel } from "./builder.ts"

export interface PendingCommand {
  id: string
  type: string
  position: Record<string, unknown> | null
  sourceFlowTypes: string[] | null
  reason: string | null
  stopAt: string | null
}

interface PendingCommandsResponse {
  commands: PendingCommand[]
}

export interface CommandPollerOptions {
  cpBaseUrl: string
  pathwayId: string
  apiKey: string
  intervalMs: number
  logger: Logger
  onCommand: (command: PendingCommand) => Promise<void>
  logLevel: {
    pollSuccess: LogLevel
    pollFailure: LogLevel
  }
}

function formatAuthHeader(apiKey: string): string {
  return `ApiKey ${apiKey.startsWith("fc_") ? `${apiKey.split("_")[1]}:${apiKey}` : apiKey}`
}

export class CommandPoller {
  private readonly cpBaseUrl: string
  private readonly pathwayId: string
  private readonly apiKey: string
  private readonly intervalMs: number
  private readonly logger: Logger
  private readonly onCommand: (command: PendingCommand) => Promise<void>
  private readonly logLevel: { pollSuccess: LogLevel; pollFailure: LogLevel }
  private timer: ReturnType<typeof setInterval> | null = null
  private polling = false

  constructor(options: CommandPollerOptions) {
    this.cpBaseUrl = options.cpBaseUrl
    this.pathwayId = options.pathwayId
    this.apiKey = options.apiKey
    this.intervalMs = options.intervalMs
    this.logger = options.logger
    this.onCommand = options.onCommand
    this.logLevel = options.logLevel
  }

  start(): void {
    if (this.timer) return

    this.logger.debug("Command poller starting", {
      pathwayId: this.pathwayId,
      intervalMs: this.intervalMs,
    })

    // Poll immediately on start, then on interval
    this.poll()
    this.timer = setInterval(() => this.poll(), this.intervalMs)
  }

  stop(): void {
    if (this.timer) {
      clearInterval(this.timer)
      this.timer = null
    }
    this.logger.debug("Command poller stopped", { pathwayId: this.pathwayId })
  }

  private async poll(): Promise<void> {
    // Guard against overlapping polls
    if (this.polling) return
    this.polling = true

    try {
      const url = `${this.cpBaseUrl}/api/v1/pathways/${this.pathwayId}/commands/pending`
      const response = await fetch(url, {
        method: "GET",
        headers: {
          Authorization: formatAuthHeader(this.apiKey),
        },
      })

      if (!response.ok) {
        this.logger[this.logLevel.pollFailure]("Command poll failed", {
          pathwayId: this.pathwayId,
          status: response.status,
        })
        return
      }

      const body = (await response.json()) as PendingCommandsResponse

      if (body.commands.length === 0) {
        this.logger[this.logLevel.pollSuccess]("Command poll: no pending commands", {
          pathwayId: this.pathwayId,
        })
        return
      }

      this.logger.info("Command poll: received commands", {
        pathwayId: this.pathwayId,
        count: body.commands.length,
      })

      for (const command of body.commands) {
        await this.handleCommand(command)
      }
    } catch (err) {
      this.logger[this.logLevel.pollFailure]("Command poll error", {
        pathwayId: this.pathwayId,
        error: err instanceof Error ? err.message : String(err),
      })
    } finally {
      this.polling = false
    }
  }

  private async handleCommand(command: PendingCommand): Promise<void> {
    const statusUrl = `${this.cpBaseUrl}/api/v1/pathways/${this.pathwayId}/commands/${command.id}/status`
    const headers = {
      "Content-Type": "application/json",
      Authorization: formatAuthHeader(this.apiKey),
    }

    // Acknowledge receipt
    try {
      await fetch(statusUrl, {
        method: "POST",
        headers,
        body: JSON.stringify({ phase: "acknowledged" }),
      })
    } catch {
      // Best-effort ack — continue with execution
    }

    // Execute command
    try {
      await this.onCommand(command)

      // Report success
      await fetch(statusUrl, {
        method: "POST",
        headers,
        body: JSON.stringify({ phase: "running" }),
      })

      this.logger.info("Command executed successfully", {
        commandId: command.id,
        type: command.type,
        pathwayId: this.pathwayId,
      })
    } catch (err) {
      const details = err instanceof Error ? err.message : String(err)

      // Report failure
      try {
        await fetch(statusUrl, {
          method: "POST",
          headers,
          body: JSON.stringify({ phase: "failed", details }),
        })
      } catch {
        // Best-effort status report
      }

      this.logger.error("Command execution failed", err instanceof Error ? err : new Error(details), {
        commandId: command.id,
        type: command.type,
        pathwayId: this.pathwayId,
      })
    }
  }
}
