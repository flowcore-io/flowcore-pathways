import { assertEquals, assertRejects } from "https://deno.land/std@0.224.0/assert/mod.ts"
import { stub } from "https://deno.land/std@0.224.0/testing/mock.ts"
import { z } from "zod"
import { CommandPoller } from "../src/pathways/command-poller.ts"
import { type PathwaysBuilderConfig, PathwaysBuilder } from "../src/pathways/builder.ts"
import { PathwayPump } from "../src/pathways/pump/pathway-pump.ts"
import { PathwayProvisioner } from "../src/pathways/provisioner.ts"

const baseOpts = {
  baseUrl: "https://api.flowcore.io",
  tenant: "test-tenant",
  dataCore: "test-dc",
  apiKey: "fc_testid_testsecret",
  dataCoreDescription: "Test data core",
}

function createBuilder(overrides: Partial<PathwaysBuilderConfig> = {}) {
  return new PathwaysBuilder({
    ...baseOpts,
    ...overrides,
  }).register({
    flowType: "user.0",
    eventType: "created.0",
    schema: z.object({
      id: z.string(),
    }),
    flowTypeDescription: "User events",
    description: "User created",
  })
}

function createPumpOptions() {
  return {
    stateManagerFactory: () => ({
      getState: () => null,
      setState: () => {},
    }),
    notifier: { type: "poller" as const, pollerIntervalMs: 1000 },
  }
}

Deno.test({
  name: "PathwaysBuilder startPump runtime behavior",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: async (t) => {
    await t.step("development auto-provision provisions shared resources only", async () => {
      const provisionCalls: string[] = []
      const startCalls: number[] = []
      const fetchCalls: RequestInit[] = []

      const provisionStub = stub(PathwayProvisioner.prototype, "provision", async () => {
        provisionCalls.push("shared")
      })
      const startStub = stub(PathwayPump.prototype, "start", async () => {
        startCalls.push(1)
      })
      const fetchStub = stub(globalThis, "fetch", async (_input, init) => {
        fetchCalls.push(init ?? {})
        return new Response(JSON.stringify({ pathwayId: crypto.randomUUID(), status: "created" }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        })
      })

      try {
        const builder = createBuilder({
          runtimeEnv: "development",
          pathwayName: "dev-service",
        })

        await builder.startPump(createPumpOptions())

        assertEquals(provisionCalls.length, 1)
        assertEquals(startCalls.length, 1)
        assertEquals(fetchCalls.length, 0)
      } finally {
        provisionStub.restore()
        startStub.restore()
        fetchStub.restore()
      }
    })

    await t.step("defaultAutoProvision=false skips all remote provisioning", async () => {
      let provisionCalls = 0
      let startCalls = 0
      let fetchCalls = 0

      const provisionStub = stub(PathwayProvisioner.prototype, "provision", async () => {
        provisionCalls++
      })
      const startStub = stub(PathwayPump.prototype, "start", async () => {
        startCalls++
      })
      const fetchStub = stub(globalThis, "fetch", async () => {
        fetchCalls++
        return new Response("{}", { status: 200 })
      })

      try {
        const builder = createBuilder({
          runtimeEnv: "development",
          pathwayName: "dev-service",
          defaultAutoProvision: false,
        })

        await builder.startPump(createPumpOptions())

        assertEquals(provisionCalls, 0)
        assertEquals(fetchCalls, 0)
        assertEquals(startCalls, 1)
      } finally {
        provisionStub.restore()
        startStub.restore()
        fetchStub.restore()
      }
    })

    await t.step("production virtual mode requires an active cluster before startup", async () => {
      let provisionCalls = 0

      const provisionStub = stub(PathwayProvisioner.prototype, "provision", async () => {
        provisionCalls++
      })

      try {
        const builder = createBuilder({
          runtimeEnv: "production",
          pathwayName: "virtual-service",
        })

        await assertRejects(
          () => builder.startPump(createPumpOptions()),
          Error,
          "Cluster mode must be started before production virtual pump startup",
        )

        assertEquals(provisionCalls, 0)
      } finally {
        provisionStub.restore()
      }
    })

    await t.step(
      "production virtual mode provisions pathway resources and starts the local pump on the leader",
      async () => {
        let provisionCalls = 0
        let startCalls = 0
        let commandPollerStarts = 0
        const fetchBodies: Array<Record<string, unknown>> = []

        const provisionStub = stub(PathwayProvisioner.prototype, "provision", async () => {
          provisionCalls++
        })
        const startStub = stub(PathwayPump.prototype, "start", async () => {
          startCalls++
        })
        const pollerStub = stub(CommandPoller.prototype, "start", () => {
          commandPollerStarts++
        })
        const fetchStub = stub(globalThis, "fetch", async (_input, init) => {
          fetchBodies.push(JSON.parse(String(init?.body ?? "{}")))
          return new Response(JSON.stringify({ pathwayId: crypto.randomUUID(), status: "created" }), {
            status: 200,
            headers: { "Content-Type": "application/json" },
          })
        })

        try {
          const builder = createBuilder({
            runtimeEnv: "production",
            pathwayName: "virtual-service",
            pathwayMode: "virtual",
          })
          ;(builder as unknown as { clusterManager: { isRunning: boolean; isLeader: boolean } }).clusterManager = {
            isRunning: true,
            isLeader: true,
          }

          await builder.startPump(createPumpOptions())

          assertEquals(provisionCalls, 1)
          assertEquals(startCalls, 1)
          assertEquals(commandPollerStarts, 1)
          assertEquals(fetchBodies.length, 1)
          assertEquals(fetchBodies[0].type, "virtual")
        } finally {
          provisionStub.restore()
          startStub.restore()
          pollerStub.restore()
          fetchStub.restore()
        }
      },
    )

    await t.step("production managed mode provisions a managed pathway and does not start a local pump", async () => {
      let provisionCalls = 0
      let startCalls = 0
      const fetchBodies: Array<Record<string, unknown>> = []

      const provisionStub = stub(PathwayProvisioner.prototype, "provision", async () => {
        provisionCalls++
      })
      const startStub = stub(PathwayPump.prototype, "start", async () => {
        startCalls++
      })
      const fetchStub = stub(globalThis, "fetch", async (_input, init) => {
        fetchBodies.push(JSON.parse(String(init?.body ?? "{}")))
        return new Response(JSON.stringify({ pathwayId: crypto.randomUUID(), status: "created" }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        })
      })

      try {
        const builder = createBuilder({
          runtimeEnv: "production",
          pathwayMode: "managed",
          pathwayName: "managed-service",
          managedConfig: {
            endpointUrl: "https://app.example.com/flowcore",
            authHeaders: { authorization: "Bearer secret" },
            sizeClass: "medium",
          },
        })

        await builder.startPump(createPumpOptions())

        assertEquals(provisionCalls, 1)
        assertEquals(startCalls, 0)
        assertEquals(fetchBodies.length, 1)
        assertEquals(fetchBodies[0].type, "managed")
        assertEquals((fetchBodies[0].config as { sources: unknown[] }).sources.length, 1)
      } finally {
        provisionStub.restore()
        startStub.restore()
        fetchStub.restore()
      }
    })
  },
})
