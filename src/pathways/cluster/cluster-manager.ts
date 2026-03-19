import type { FlowcoreEvent } from "../../contracts/event.ts"
import type { Logger } from "../logger.ts"
import { NoopLogger } from "../logger.ts"
import type {
  ClusterRole,
  PathwayClusterOptions,
  PathwayCoordinator,
  PendingDelivery,
  WsAckMessage,
  WsFailMessage,
  WsMessage,
} from "./types.ts"

const DEFAULT_LEASE_TTL_MS = 30_000
const DEFAULT_LEASE_RENEW_INTERVAL_MS = 10_000
const DEFAULT_HEARTBEAT_INTERVAL_MS = 5_000
const DEFAULT_STALE_THRESHOLD_MS = 15_000
const DEFAULT_DELIVERY_TIMEOUT_MS = 30_000
const LEASE_KEY = "pathway-cluster-leader"

/**
 * ClusterManager handles distributed event processing via leader election and WS workers.
 *
 * Lifecycle: register → heartbeat loop → leader election loop
 * - Leader: opens WS connections to workers, distributes events round-robin
 * - Worker: accepts WS from leader, processes events, sends ack/fail
 * - Fallback: leader processes locally when no workers available
 */
export class ClusterManager {
  private readonly coordinator: PathwayCoordinator
  private readonly instanceId: string
  private readonly advertisedAddress: string
  private readonly port: number
  private readonly leaseTtlMs: number
  private readonly leaseRenewIntervalMs: number
  private readonly heartbeatIntervalMs: number
  private readonly staleThresholdMs: number
  private readonly deliveryTimeoutMs: number
  private readonly workerConcurrency: number
  private readonly logger: Logger

  private role: ClusterRole = "unknown"
  private running = false
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null
  private leaseTimer: ReturnType<typeof setInterval> | null = null
  private deliveryTimeoutTimer: ReturnType<typeof setInterval> | null = null

  // Leader state
  private workerConnections: Map<string, WebSocket> = new Map()
  private pendingDeliveries: Map<string, PendingDelivery> = new Map()
  private workerAddresses: string[] = []
  private roundRobinIndex = 0

  // Worker state
  private wsServer: ReturnType<typeof Deno.serve> | null = null
  private leaderConnection: WebSocket | null = null
  private eventHandler: ((pathway: string, event: FlowcoreEvent) => Promise<void>) | null = null

  constructor(options: PathwayClusterOptions, logger?: Logger) {
    this.coordinator = options.coordinator
    this.instanceId = crypto.randomUUID()
    this.advertisedAddress = options.advertisedAddress
    this.port = options.port
    this.leaseTtlMs = options.leaseTtlMs ?? DEFAULT_LEASE_TTL_MS
    this.leaseRenewIntervalMs = options.leaseRenewIntervalMs ?? DEFAULT_LEASE_RENEW_INTERVAL_MS
    this.heartbeatIntervalMs = options.heartbeatIntervalMs ?? DEFAULT_HEARTBEAT_INTERVAL_MS
    this.staleThresholdMs = options.staleThresholdMs ?? DEFAULT_STALE_THRESHOLD_MS
    this.deliveryTimeoutMs = options.deliveryTimeoutMs ?? DEFAULT_DELIVERY_TIMEOUT_MS
    this.workerConcurrency = options.workerConcurrency ?? 5
    this.logger = logger ?? new NoopLogger()
  }

  /**
   * Set the handler that processes events locally (used by both leader fallback and workers)
   */
  setEventHandler(handler: (pathway: string, event: FlowcoreEvent) => Promise<void>) {
    this.eventHandler = handler
  }

  /**
   * Start the cluster: register instance, begin heartbeat, attempt leader election
   */
  async start(): Promise<void> {
    if (this.running) return
    this.running = true

    this.logger.info("Starting cluster manager", {
      instanceId: this.instanceId,
      advertisedAddress: this.advertisedAddress,
    })

    // Register this instance
    await this.coordinator.register(this.instanceId, this.advertisedAddress)

    // Start WS server for accepting connections (both leader and worker need this)
    this.startWsServer()

    // Start heartbeat loop
    this.heartbeatTimer = setInterval(async () => {
      try {
        await this.coordinator.heartbeat(this.instanceId)
      } catch (err) {
        this.logger.error("Heartbeat failed", err instanceof Error ? err : new Error(String(err)))
      }
    }, this.heartbeatIntervalMs)

    // Start leader election loop
    await this.tryAcquireLease()
    this.leaseTimer = setInterval(async () => {
      try {
        await this.leaseLoop()
      } catch (err) {
        this.logger.error("Lease loop error", err instanceof Error ? err : new Error(String(err)))
      }
    }, this.leaseRenewIntervalMs)

    // Start delivery timeout checker
    this.deliveryTimeoutTimer = setInterval(() => {
      this.checkDeliveryTimeouts()
    }, 5_000)
  }

  /**
   * Stop the cluster: release lease, unregister, close connections
   */
  async stop(): Promise<void> {
    if (!this.running) return
    this.running = false

    this.logger.info("Stopping cluster manager", { instanceId: this.instanceId })

    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer)
    if (this.leaseTimer) clearInterval(this.leaseTimer)
    if (this.deliveryTimeoutTimer) clearInterval(this.deliveryTimeoutTimer)

    // Release lease if leader
    if (this.role === "leader") {
      try {
        await this.coordinator.releaseLease(this.instanceId, LEASE_KEY)
      } catch (err) {
        this.logger.error("Error releasing lease", err instanceof Error ? err : new Error(String(err)))
      }
    }

    // Close worker connections (leader side)
    for (const [addr, ws] of this.workerConnections) {
      try {
        ws.close()
      } catch {
        // ignore close errors
      }
    }
    this.workerConnections.clear()

    // Close leader connection (worker side)
    if (this.leaderConnection) {
      try {
        this.leaderConnection.close()
      } catch {
        // ignore
      }
      this.leaderConnection = null
    }

    // Stop WS server
    if (this.wsServer) {
      try {
        await this.wsServer.shutdown()
      } catch {
        // ignore
      }
      this.wsServer = null
    }

    // Reject pending deliveries
    for (const [, delivery] of this.pendingDeliveries) {
      delivery.reject(new Error("Cluster manager stopped"))
    }
    this.pendingDeliveries.clear()

    // Unregister
    try {
      await this.coordinator.unregister(this.instanceId)
    } catch (err) {
      this.logger.error("Error unregistering", err instanceof Error ? err : new Error(String(err)))
    }

    this.role = "unknown"
  }

  /**
   * Process an event through the cluster.
   * - Leader: distribute to a worker or process locally if no workers
   * - Worker: should not call this directly (receives events via WS)
   */
  async processEvent(pathway: string, event: FlowcoreEvent): Promise<void> {
    if (this.role !== "leader") {
      // If not leader, process locally (shouldn't normally happen for cluster-routed events)
      this.logger.warn("processEvent called on non-leader instance, processing locally", {
        instanceId: this.instanceId,
        role: this.role,
      })
      await this.processLocally(pathway, event)
      return
    }

    // Try to distribute to a worker
    const workerAddress = this.getNextWorker()
    if (workerAddress) {
      try {
        await this.distributeToWorker(workerAddress, pathway, event)
        return
      } catch (err) {
        this.logger.warn("Failed to distribute to worker, processing locally", {
          worker: workerAddress,
          error: err instanceof Error ? err.message : String(err),
        })
      }
    }

    // No workers available or distribution failed — process locally
    await this.processLocally(pathway, event)
  }

  get isLeader(): boolean {
    return this.role === "leader"
  }

  get isWorker(): boolean {
    return this.role === "worker"
  }

  get currentRole(): ClusterRole {
    return this.role
  }

  get isRunning(): boolean {
    return this.running
  }

  get currentInstanceId(): string {
    return this.instanceId
  }

  // --- Private: Leader Election ---

  private async tryAcquireLease(): Promise<void> {
    const acquired = await this.coordinator.acquireLease(this.instanceId, LEASE_KEY, this.leaseTtlMs)
    if (acquired) {
      if (this.role !== "leader") {
        this.logger.info("Acquired leader lease", { instanceId: this.instanceId })
        this.role = "leader"
        await this.onBecomeLeader()
      }
    } else {
      if (this.role !== "worker") {
        this.logger.info("Could not acquire lease, becoming worker", { instanceId: this.instanceId })
        this.role = "worker"
      }
    }
  }

  private async leaseLoop(): Promise<void> {
    if (this.role === "leader") {
      const renewed = await this.coordinator.renewLease(this.instanceId, LEASE_KEY, this.leaseTtlMs)
      if (!renewed) {
        this.logger.warn("Lost leader lease", { instanceId: this.instanceId })
        this.role = "worker"
        this.cleanupLeaderState()
      } else {
        // Refresh worker list
        await this.refreshWorkers()
      }
    } else {
      // Try to become leader
      await this.tryAcquireLease()
    }
  }

  private async onBecomeLeader(): Promise<void> {
    await this.refreshWorkers()
  }

  private async refreshWorkers(): Promise<void> {
    const instances = await this.coordinator.getInstances(this.staleThresholdMs)
    const newWorkers = instances
      .filter((i) => i.instanceId !== this.instanceId)
      .map((i) => i.address)

    // Connect to new workers, disconnect from removed workers
    const currentAddresses = new Set(this.workerConnections.keys())
    const newAddresses = new Set(newWorkers)

    // Disconnect from removed workers
    for (const addr of currentAddresses) {
      if (!newAddresses.has(addr)) {
        const ws = this.workerConnections.get(addr)
        if (ws) {
          try {
            ws.close()
          } catch {
            // ignore
          }
        }
        this.workerConnections.delete(addr)
      }
    }

    // Connect to new workers
    for (const addr of newAddresses) {
      if (!currentAddresses.has(addr)) {
        this.connectToWorker(addr)
      }
    }

    this.workerAddresses = [...newAddresses]
    this.logger.debug("Refreshed worker list", { workers: this.workerAddresses })
  }

  private cleanupLeaderState(): void {
    for (const [, ws] of this.workerConnections) {
      try {
        ws.close()
      } catch {
        // ignore
      }
    }
    this.workerConnections.clear()
    this.workerAddresses = []
    this.roundRobinIndex = 0
  }

  // --- Private: WS Server (accepts connections from leader) ---

  private startWsServer(): void {
    this.wsServer = Deno.serve({ port: this.port, hostname: "0.0.0.0" }, (req) => {
      if (req.headers.get("upgrade")?.toLowerCase() !== "websocket") {
        return new Response("Expected WebSocket", { status: 426 })
      }

      const { socket, response } = Deno.upgradeWebSocket(req)

      socket.onopen = () => {
        this.logger.debug("WS connection opened from leader")
        this.leaderConnection = socket
      }

      socket.onmessage = async (event) => {
        try {
          const msg: WsMessage = JSON.parse(event.data)
          await this.handleWorkerMessage(socket, msg)
        } catch (err) {
          this.logger.error(
            "Error handling WS message",
            err instanceof Error ? err : new Error(String(err)),
          )
        }
      }

      socket.onclose = () => {
        this.logger.debug("WS connection closed from leader")
        if (this.leaderConnection === socket) {
          this.leaderConnection = null
        }
      }

      socket.onerror = (err) => {
        this.logger.error("WS error", new Error(String(err)))
      }

      return response
    })

    this.logger.info("WS server started", { port: this.port })
  }

  private async handleWorkerMessage(socket: WebSocket, msg: WsMessage): Promise<void> {
    switch (msg.type) {
      case "events": {
        // Worker receives events from leader, process them
        const ackEventIds: string[] = []
        const failEventIds: string[] = []

        for (const event of msg.events) {
          const pathway = `${event.flowType}/${event.eventType}`
          try {
            await this.processLocally(pathway, event)
            ackEventIds.push(event.eventId)
          } catch {
            failEventIds.push(event.eventId)
          }
        }

        if (ackEventIds.length > 0) {
          const ack: WsAckMessage = {
            type: "ack",
            deliveryId: msg.deliveryId,
            eventIds: ackEventIds,
          }
          socket.send(JSON.stringify(ack))
        }

        if (failEventIds.length > 0) {
          const fail: WsFailMessage = {
            type: "fail",
            deliveryId: msg.deliveryId,
            eventIds: failEventIds,
          }
          socket.send(JSON.stringify(fail))
        }
        break
      }
      case "ping": {
        socket.send(JSON.stringify({ type: "pong" }))
        break
      }
      default:
        break
    }
  }

  // --- Private: WS Client (leader connects to workers) ---

  private connectToWorker(address: string): void {
    try {
      const ws = new WebSocket(address)

      ws.onopen = () => {
        this.logger.info("Connected to worker", { address })
        this.workerConnections.set(address, ws)
      }

      ws.onmessage = (event) => {
        try {
          const msg: WsMessage = JSON.parse(event.data)
          this.handleLeaderMessage(address, msg)
        } catch (err) {
          this.logger.error(
            "Error handling worker response",
            err instanceof Error ? err : new Error(String(err)),
          )
        }
      }

      ws.onclose = () => {
        this.logger.info("Disconnected from worker", { address })
        this.workerConnections.delete(address)
        this.workerAddresses = this.workerAddresses.filter((a) => a !== address)
      }

      ws.onerror = (err) => {
        this.logger.error("Worker WS error", new Error(String(err)), { address })
        this.workerConnections.delete(address)
      }
    } catch (err) {
      this.logger.error(
        "Failed to connect to worker",
        err instanceof Error ? err : new Error(String(err)),
        { address },
      )
    }
  }

  private handleLeaderMessage(workerAddress: string, msg: WsMessage): void {
    switch (msg.type) {
      case "ack": {
        const delivery = this.pendingDeliveries.get(msg.deliveryId)
        if (delivery) {
          this.pendingDeliveries.delete(msg.deliveryId)
          delivery.resolve(msg.eventIds)
        }
        break
      }
      case "fail": {
        const delivery = this.pendingDeliveries.get(msg.deliveryId)
        if (delivery) {
          this.pendingDeliveries.delete(msg.deliveryId)
          delivery.reject(new Error(`Worker ${workerAddress} failed to process events: ${msg.eventIds.join(", ")}`))
        }
        break
      }
      case "pong": {
        // heartbeat response, no action needed
        break
      }
      default:
        break
    }
  }

  // --- Private: Event Distribution ---

  private getNextWorker(): string | null {
    const connectedWorkers = this.workerAddresses.filter((addr) => {
      const ws = this.workerConnections.get(addr)
      return ws && ws.readyState === WebSocket.OPEN
    })

    if (connectedWorkers.length === 0) return null

    const worker = connectedWorkers[this.roundRobinIndex % connectedWorkers.length]
    this.roundRobinIndex = (this.roundRobinIndex + 1) % connectedWorkers.length
    return worker
  }

  private async distributeToWorker(workerAddress: string, pathway: string, event: FlowcoreEvent): Promise<void> {
    const ws = this.workerConnections.get(workerAddress)
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      throw new Error(`Worker ${workerAddress} not connected`)
    }

    const deliveryId = crypto.randomUUID()

    return new Promise<void>((resolve, reject) => {
      const delivery: PendingDelivery = {
        deliveryId,
        events: [event],
        workerAddress,
        sentAt: Date.now(),
        resolve: () => resolve(),
        reject,
      }
      this.pendingDeliveries.set(deliveryId, delivery)

      const msg: WsMessage = {
        type: "events",
        deliveryId,
        events: [event],
      }

      try {
        ws.send(JSON.stringify(msg))
      } catch (err) {
        this.pendingDeliveries.delete(deliveryId)
        reject(err instanceof Error ? err : new Error(String(err)))
      }
    })
  }

  private async processLocally(pathway: string, event: FlowcoreEvent): Promise<void> {
    if (!this.eventHandler) {
      throw new Error("No event handler set on ClusterManager")
    }
    await this.eventHandler(pathway, event)
  }

  private checkDeliveryTimeouts(): void {
    const now = Date.now()
    for (const [id, delivery] of this.pendingDeliveries) {
      if (now - delivery.sentAt > this.deliveryTimeoutMs) {
        this.pendingDeliveries.delete(id)
        delivery.reject(new Error(`Delivery ${id} to ${delivery.workerAddress} timed out`))
      }
    }
  }
}
