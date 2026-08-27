import type { FlowcoreEvent } from "../../contracts/event.ts"
import type { PumpState } from "../pump/types.ts"
import type { StatePrefixConfig } from "../state-prefix.ts"

/**
 * Coordinator interface for distributed cluster coordination
 * Handles instance registration, heartbeating, and leader election via leases
 */
export interface PathwayCoordinator {
  /**
   * Optional leader lease key this coordinator is namespaced to.
   *
   * `ClusterManager` uses it when neither `leaseKey` nor `statePrefix` is set in
   * the cluster options, so configuring a `statePrefix` on the coordinator alone
   * is enough to isolate a cluster. Coordinators that omit it keep the default
   * key `"pathway-cluster-leader"`.
   */
  readonly leaseKey?: string
  acquireLease(instanceId: string, key: string, ttlMs: number): Promise<boolean>
  renewLease(instanceId: string, key: string, ttlMs: number): Promise<boolean>
  releaseLease(instanceId: string, key: string): Promise<void>
  register(instanceId: string, address: string): Promise<void>
  heartbeat(instanceId: string): Promise<void>
  unregister(instanceId: string): Promise<void>
  getInstances(staleThresholdMs: number): Promise<Array<{ instanceId: string; address: string }>>
}

/**
 * Options for starting a cluster
 *
 * The leader lease key is resolved in this order, first hit wins:
 *   1. `leaseKey`
 *   2. `statePrefix` applied to `"pathway-cluster-leader"`
 *   3. `coordinator.leaseKey`
 *   4. `"pathway-cluster-leader"`
 *
 * Two clusters that share ONE database MUST resolve to different lease keys.
 * Otherwise only one of them elects a leader and the others never start a pump.
 */
export interface PathwayClusterOptions extends StatePrefixConfig {
  coordinator: PathwayCoordinator
  advertisedAddress: string
  port: number
  transport?: ClusterTransport
  workerConcurrency?: number
  leaseTtlMs?: number
  leaseRenewIntervalMs?: number
  heartbeatIntervalMs?: number
  staleThresholdMs?: number
  deliveryTimeoutMs?: number
  /**
   * Explicit leader lease key. Overrides `statePrefix` and the coordinator's own
   * key. Default: `"pathway-cluster-leader"`.
   */
  leaseKey?: string
}

/**
 * WebSocket protocol message types for cluster communication
 */
export type WsMessage =
  | WsEventsMessage
  | WsAckMessage
  | WsFailMessage
  | WsPingMessage
  | WsPongMessage
  | WsResetMessage
  | WsResetAckMessage
  | WsResetFailMessage

export interface WsEventsMessage {
  type: "events"
  deliveryId: string
  events: FlowcoreEvent[]
}

export interface WsAckMessage {
  type: "ack"
  deliveryId: string
  eventIds: string[]
}

export interface WsFailMessage {
  type: "fail"
  deliveryId: string
  eventIds: string[]
}

export interface WsPingMessage {
  type: "ping"
}

export interface WsPongMessage {
  type: "pong"
}

export interface WsResetMessage {
  type: "reset"
  resetId: string
  position?: PumpState
}

export interface WsResetAckMessage {
  type: "reset-ack"
  resetId: string
}

export interface WsResetFailMessage {
  type: "reset-fail"
  resetId: string
  error: string
}

/**
 * Minimal WebSocket-like interface for cross-platform compatibility
 */
export interface ClusterSocket {
  send(data: string): void
  close(): void
  readonly readyState: number
  onopen: ((event: unknown) => void) | null
  onmessage: ((event: { data: string }) => void) | null
  onclose: ((event: unknown) => void) | null
  onerror: ((event: unknown) => void) | null
}

/**
 * Transport abstraction for cluster WS server/client.
 * Implement this interface for different runtimes (Deno, Node.js, etc.)
 */
export interface ClusterTransport {
  /** Start a WS server on the given port. Call onConnection for each new socket. */
  startServer(
    port: number,
    onConnection: (socket: ClusterSocket) => void,
  ): Promise<{ shutdown(): Promise<void> }>

  /** Connect to a remote WS address and return a socket. */
  connect(address: string): ClusterSocket
}

/**
 * Cluster role for this instance
 */
export type ClusterRole = "leader" | "worker" | "unknown"

/**
 * Pending delivery tracking
 */
export interface PendingDelivery {
  deliveryId: string
  events: FlowcoreEvent[]
  workerAddress: string
  sentAt: number
  resolve: (eventIds: string[]) => void
  reject: (error: Error) => void
}
