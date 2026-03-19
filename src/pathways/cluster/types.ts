import type { FlowcoreEvent } from "../../contracts/event.ts"

/**
 * Coordinator interface for distributed cluster coordination
 * Handles instance registration, heartbeating, and leader election via leases
 */
export interface PathwayCoordinator {
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
 */
export interface PathwayClusterOptions {
  coordinator: PathwayCoordinator
  advertisedAddress: string
  port: number
  workerConcurrency?: number
  leaseTtlMs?: number
  leaseRenewIntervalMs?: number
  heartbeatIntervalMs?: number
  staleThresholdMs?: number
  deliveryTimeoutMs?: number
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
