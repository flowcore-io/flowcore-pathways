import { assert, assertEquals, assertGreater, assertGreaterOrEqual } from "https://deno.land/std@0.224.0/assert/mod.ts"
import { z } from "zod"
import type { FlowcoreEvent } from "../src/contracts/event.ts"
import { PathwaysBuilder } from "../src/pathways/builder.ts"
import type { PumpState, PumpStateManager, PumpStateManagerFactory } from "../src/pathways/pump/types.ts"
import { InMemoryCoordinator } from "./helpers/in-memory-coordinator.ts"

const BUCKET = "20260924230000"
const FLOW_TYPE = "local-flow.0"
const EVENT_TYPE = "source.0"
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

async function eventually<T>(
  read: () => T | Promise<T>,
  accept: (value: T) => boolean,
  timeoutMs = 15_000,
): Promise<T> {
  const deadline = Date.now() + timeoutMs
  let last!: T
  while (Date.now() < deadline) {
    last = await read()
    if (accept(last)) return last
    await sleep(20)
  }
  last = await read()
  throw new Error(`condition not met; last=${JSON.stringify(last)}`)
}

function event(index: number): FlowcoreEvent {
  return {
    eventId: `00000000-0000-1000-8000-${String(index).padStart(12, "0")}`,
    timeBucket: BUCKET,
    tenant: "local-tenant",
    dataCoreId: "local-core",
    flowType: FLOW_TYPE,
    eventType: EVENT_TYPE,
    metadata: {},
    payload: { sequence: index },
    validTime: `2026-09-24T23:00:${String(index % 60).padStart(2, "0")}.000Z`,
  }
}

function startSource(sourceEvents: FlowcoreEvent[]) {
  const requests: string[] = []
  const server = Deno.serve({ hostname: "127.0.0.1", port: 0, onListen: () => {} }, (request) => {
    const url = new URL(request.url)
    requests.push(`${request.method} ${url.pathname}${url.search}`)
    if (url.pathname === "/api/v1/data-cores") {
      return Response.json([{
        id: "00000000-0000-4000-8000-000000000101",
        tenantId: "00000000-0000-4000-8000-000000000100",
        tenant: "local-tenant",
        name: "local-core",
        description: "local fixture",
        accessControl: "private",
        deleteProtection: false,
        isDeleting: false,
        isFlowcoreManaged: false,
      }])
    }
    if (url.pathname === "/api/v1/flow-types") {
      return Response.json([{
        id: "00000000-0000-4000-8000-000000000102",
        tenantId: "00000000-0000-4000-8000-000000000100",
        dataCoreId: "00000000-0000-4000-8000-000000000101",
        name: FLOW_TYPE,
        description: "local fixture",
        isDeleting: false,
      }])
    }
    if (url.pathname === "/api/v1/event-types") {
      return Response.json([{
        id: "00000000-0000-4000-8000-000000000103",
        tenantId: "00000000-0000-4000-8000-000000000100",
        dataCoreId: "00000000-0000-4000-8000-000000000101",
        flowTypeId: "00000000-0000-4000-8000-000000000102",
        name: EVENT_TYPE,
        description: "local fixture",
        isTruncating: false,
        isDeleting: false,
        createdAt: "2026-09-24T00:00:00.000Z",
        updatedAt: null,
      }])
    }
    if (url.pathname === "/api/v1/tenants/by-name/local-tenant/instance") {
      return Response.json({ isDedicated: false, instance: null })
    }
    if (url.pathname === "/api/v1/time-buckets") return Response.json({ timeBuckets: [BUCKET] })
    if (url.pathname === "/api/v1/events") {
      const after = url.searchParams.get("afterEventId")
      const index = after ? sourceEvents.findIndex((item) => item.eventId === after) : -1
      return Response.json({ events: index >= 0 ? sourceEvents.slice(index + 1) : sourceEvents })
    }
    return Response.json({ error: "not found", path: url.pathname }, { status: 404 })
  })
  return {
    baseUrl: `http://127.0.0.1:${server.addr.port}`,
    requests,
    stop: () => server.shutdown(),
  }
}

function createStateFactory(initial: PumpState): {
  factory: PumpStateManagerFactory
  manager: PumpStateManager
  writes: PumpState[]
} {
  let state: PumpState | null = { ...initial }
  const writes: PumpState[] = []
  const manager: PumpStateManager = {
    getState: () => state,
    setState: (next) => {
      state = { ...next }
      writes.push({ ...next })
    },
    clearState: () => {
      state = null
    },
  }
  return { factory: () => manager, manager, writes }
}

function freePort(): number {
  const listener = Deno.listen({ hostname: "127.0.0.1", port: 0 })
  const port = (listener.addr as Deno.NetAddr).port
  listener.close()
  return port
}

type LocalPathways = Record<string, { input: { sequence: number }; output: { sequence: number } }>

function createBuilder(baseUrl: string) {
  return new PathwaysBuilder<LocalPathways, string>({
    baseUrl,
    tenant: "local-tenant",
    dataCore: "local-core",
    apiKey: ["fc", "sy", "local"].join("_"),
    runtimeEnv: "test",
    pathwayMode: "virtual",
    autoProvision: { dataCore: false, flowType: false, eventType: false, pathway: false },
  })
}

Deno.test({
  name: "startPump three-node cluster increases delayed-target throughput with concurrent useful workers",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: async () => {
    const backlog = Array.from({ length: 24 }, (_, index) => event(index + 1))
    const delayMs = 80
    const pumpConcurrency = 4

    async function run(instanceCount: 1 | 3) {
      const available: FlowcoreEvent[] = []
      const source = startSource(available)
      const state = createStateFactory({ timeBucket: BUCKET })
      const coordinator = new InMemoryCoordinator()
      const nodes: Array<{
        name: string
        builder: PathwaysBuilder<LocalPathways, string>
        manager: Awaited<ReturnType<PathwaysBuilder<LocalPathways, string>["startCluster"]>>
      }> = []
      const timeline: Array<{ eventId: string; node: string; startedAt: number; finishedAt: number }> = []
      let active = 0
      let peakActive = 0
      let firstStartedAt = 0
      let lastFinishedAt = 0

      try {
        for (let index = 0; index < instanceCount; index++) {
          const name = `node-${index + 1}`
          const builder = createBuilder(source.baseUrl)
          const pathway = builder.register({
            flowType: FLOW_TYPE,
            eventType: EVENT_TYPE,
            schema: z.object({ sequence: z.number() }),
            writable: false,
            subscribe: true,
            maxRetries: 0,
          })

          let localTail = Promise.resolve()
          pathway.handle(`${FLOW_TYPE}/${EVENT_TYPE}`, async (incoming) => {
            const previous = localTail
            let release!: () => void
            localTail = new Promise<void>((resolve) => {
              release = resolve
            })
            await previous
            const startedAt = performance.now()
            if (!firstStartedAt) firstStartedAt = startedAt
            active++
            peakActive = Math.max(peakActive, active)
            try {
              await sleep(delayMs)
              const finishedAt = performance.now()
              lastFinishedAt = Math.max(lastFinishedAt, finishedAt)
              timeline.push({ eventId: incoming.eventId, node: name, startedAt, finishedAt })
            } finally {
              active--
              release()
            }
          })

          const port = freePort()
          const manager = await builder.startCluster({
            coordinator,
            advertisedAddress: "127.0.0.1",
            port,
            leaseTtlMs: 2_000,
            leaseRenewIntervalMs: 100,
            heartbeatIntervalMs: 50,
            staleThresholdMs: 500,
            deliveryTimeoutMs: 5_000,
          })
          nodes.push({ name, builder, manager })
        }

        for (const node of nodes) {
          await node.builder.startPump({
            stateManagerFactory: state.factory,
            notifier: { type: "poller", pollerIntervalMs: 20 },
            bufferSize: backlog.length,
            maxRedeliveryCount: -1,
            concurrency: pumpConcurrency,
            autoProvision: { dataCore: false, flowType: false, eventType: false, pathway: false },
          })
        }

        await eventually(
          () => nodes.map((node) => node.manager.currentRole),
          (roles) =>
            roles.filter((role) => role === "leader").length === 1 &&
            roles.filter((role) => role === "worker").length === instanceCount - 1,
        )
        if (instanceCount === 3) await sleep(300)

        available.push(...backlog)
        await eventually(
          () => state.manager.getState(),
          (checkpoint) => checkpoint?.eventId === backlog.at(-1)!.eventId,
          20_000,
        )

        const elapsedMs = lastFinishedAt - firstStartedAt
        const throughput = backlog.length / (elapsedMs / 1000)
        const usefulNodes = [...new Set(timeline.map((item) => item.node))]
        assertEquals(timeline.length, backlog.length)
        assertEquals(new Set(timeline.map((item) => item.eventId)).size, backlog.length)
        return { instanceCount, elapsedMs, throughput, peakActive, usefulNodes, timeline }
      } finally {
        for (const node of nodes) await node.builder.stopPump().catch(() => {})
        for (const node of nodes) await node.builder.stopCluster().catch(() => {})
        await source.stop().catch(() => {})
      }
    }

    const single = await run(1)
    const clustered = await run(3)
    const gain = clustered.throughput / single.throughput

    console.log(JSON.stringify({
      proof: "genuine-startPump-1-vs-3-throughput",
      backlog: backlog.length,
      delayMs,
      pumpConcurrency,
      single: {
        elapsedMs: single.elapsedMs,
        throughput: single.throughput,
        peakActive: single.peakActive,
        usefulNodes: single.usefulNodes,
      },
      clustered: {
        elapsedMs: clustered.elapsedMs,
        throughput: clustered.throughput,
        peakActive: clustered.peakActive,
        usefulNodes: clustered.usefulNodes,
      },
      gain,
    }))
    assertEquals(single.usefulNodes.length, 1)
    assertGreaterOrEqual(clustered.usefulNodes.length, 2)
    assertGreaterOrEqual(clustered.peakActive, 2)
    assertGreater(gain, 1.5)
  },
})

Deno.test({
  name: "concurrent startPump batch retains the checkpoint on one failure and replays without missing valid events",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: async () => {
    const backlog = [event(101), event(102), event(103)]
    const source = startSource(backlog)
    const state = createStateFactory({ timeBucket: BUCKET })
    const accepted: string[] = []
    let rejectFirst = true
    let rejectedAttempts = 0
    const builder = createBuilder(source.baseUrl)
    const pathway = builder.register({
      flowType: FLOW_TYPE,
      eventType: EVENT_TYPE,
      schema: z.object({ sequence: z.number() }),
      writable: false,
      subscribe: true,
      maxRetries: 0,
    })
    pathway.handle(`${FLOW_TYPE}/${EVENT_TYPE}`, async (incoming) => {
      await sleep(20)
      if (incoming.eventId === backlog[0].eventId && rejectFirst) {
        rejectedAttempts++
        throw new Error("injected target rejection")
      }
      accepted.push(incoming.eventId)
    })

    try {
      await builder.startPump({
        stateManagerFactory: state.factory,
        notifier: { type: "poller", pollerIntervalMs: 20 },
        bufferSize: backlog.length,
        maxRedeliveryCount: -1,
        concurrency: 3,
        autoProvision: { dataCore: false, flowType: false, eventType: false, pathway: false },
      })
      await eventually(() => rejectedAttempts, (count) => count >= 2)
      await eventually(
        () => accepted,
        (ids) => ids.includes(backlog[1].eventId) && ids.includes(backlog[2].eventId),
      )
      assertEquals(await state.manager.getState(), { timeBucket: BUCKET })
      assertEquals(state.writes, [])

      rejectFirst = false
      await eventually(() => state.manager.getState(), (checkpoint) => checkpoint?.eventId === backlog.at(-1)!.eventId)
      const missing = backlog.filter((item) => !accepted.includes(item.eventId)).map((item) => item.eventId)
      assertEquals(missing, [])
      assertGreaterOrEqual(accepted.filter((id) => id === backlog[1].eventId).length, 2)
      assertGreaterOrEqual(accepted.filter((id) => id === backlog[2].eventId).length, 2)
      console.log(JSON.stringify({
        proof: "concurrent-startPump-checkpoint-retry",
        rejectedAttempts,
        checkpoint: await state.manager.getState(),
        checkpointWrites: state.writes,
        accepted,
        missing,
      }))
    } finally {
      await builder.stopPump().catch(() => {})
      await source.stop().catch(() => {})
    }
  },
})
