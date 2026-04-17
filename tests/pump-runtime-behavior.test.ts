import { assertEquals, assertRejects } from "https://deno.land/std@0.224.0/assert/mod.ts"
import { stub } from "https://deno.land/std@0.224.0/testing/mock.ts"
import { z } from "zod"
import { CommandPoller } from "../src/pathways/command-poller.ts"
import { PathwaysBuilder, type PathwaysBuilderConfig } from "../src/pathways/builder.ts"
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
          // Explicit virtual — overrides the prod-default "managed" so the cluster check fires.
          pathwayMode: "virtual",
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
      "production virtual mode starts the local pump before registering the virtual pathway on the leader",
      async () => {
        let provisionCalls = 0
        let startCalls = 0
        let setPulseCalls = 0
        let commandPollerStarts = 0
        const lifecycle: string[] = []
        const fetchBodies: Array<Record<string, unknown>> = []

        const provisionStub = stub(PathwayProvisioner.prototype, "provision", async () => {
          provisionCalls++
          lifecycle.push("provision")
        })
        const startStub = stub(PathwayPump.prototype, "start", async function (this: PathwayPump) {
          startCalls++
          ;(this as unknown as { running: boolean }).running = true
          lifecycle.push("start")
        })
        const setPulseStub = stub(PathwayPump.prototype, "setPulseConfig", async () => {
          setPulseCalls++
          lifecycle.push("setPulse")
        })
        const pollerStub = stub(CommandPoller.prototype, "start", () => {
          commandPollerStarts++
          lifecycle.push("pollerStart")
        })
        const fetchStub = stub(globalThis, "fetch", async (_input, init) => {
          lifecycle.push("fetch")
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
            // Opt in to pathway registration — default is resources-only.
            autoProvision: { pathway: true },
          })
          ;(builder as unknown as { clusterManager: { isRunning: boolean; isLeader: boolean } }).clusterManager = {
            isRunning: true,
            isLeader: true,
          }

          await builder.startPump(createPumpOptions())

          assertEquals(provisionCalls, 1)
          assertEquals(startCalls, 1)
          assertEquals(setPulseCalls, 1)
          assertEquals(commandPollerStarts, 1)
          assertEquals(fetchBodies.length, 1)
          assertEquals(fetchBodies[0].type, "virtual")
          assertEquals(lifecycle, ["provision", "start", "fetch", "setPulse", "pollerStart"])
        } finally {
          provisionStub.restore()
          startStub.restore()
          setPulseStub.restore()
          pollerStub.restore()
          fetchStub.restore()
        }
      },
    )

    await t.step(
      "production virtual mode skips by-name registration on non-leaders and defers it until leadership gain",
      async () => {
        let provisionCalls = 0
        let startCalls = 0
        let stopCalls = 0
        let setPulseCalls = 0
        let commandPollerStarts = 0
        let commandPollerStops = 0
        const lifecycle: string[] = []
        const fetchBodies: Array<Record<string, unknown>> = []

        const provisionStub = stub(PathwayProvisioner.prototype, "provision", async () => {
          provisionCalls++
          lifecycle.push("provision")
        })
        const startStub = stub(PathwayPump.prototype, "start", async function (this: PathwayPump) {
          startCalls++
          ;(this as unknown as { running: boolean }).running = true
          lifecycle.push("start")
        })
        const stopStub = stub(PathwayPump.prototype, "stop", async function (this: PathwayPump) {
          stopCalls++
          ;(this as unknown as { running: boolean }).running = false
          lifecycle.push("stop")
        })
        const setPulseStub = stub(PathwayPump.prototype, "setPulseConfig", async () => {
          setPulseCalls++
          lifecycle.push("setPulse")
        })
        const pollerStartStub = stub(CommandPoller.prototype, "start", () => {
          commandPollerStarts++
          lifecycle.push("pollerStart")
        })
        const pollerStopStub = stub(CommandPoller.prototype, "stop", () => {
          commandPollerStops++
          lifecycle.push("pollerStop")
        })
        const fetchStub = stub(globalThis, "fetch", async (_input, init) => {
          lifecycle.push("fetch")
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
            autoProvision: { pathway: true },
          })
          const clusterManager = { isRunning: true, isLeader: false }
          ;(builder as unknown as { clusterManager: typeof clusterManager }).clusterManager = clusterManager

          await builder.startPump(createPumpOptions())

          assertEquals(provisionCalls, 1)
          assertEquals(startCalls, 0)
          assertEquals(fetchBodies.length, 0)
          assertEquals(setPulseCalls, 0)
          assertEquals(commandPollerStarts, 0)

          clusterManager.isLeader = true
          await (builder as unknown as { handleLeadershipChange(isLeader: boolean): Promise<void> })
            .handleLeadershipChange(true)

          assertEquals(startCalls, 1)
          assertEquals(fetchBodies.length, 1)
          assertEquals(setPulseCalls, 1)
          assertEquals(commandPollerStarts, 1)

          await (builder as unknown as { handleLeadershipChange(isLeader: boolean): Promise<void> })
            .handleLeadershipChange(false)

          assertEquals(commandPollerStops, 1)
          assertEquals(stopCalls, 1)
          assertEquals(lifecycle, [
            "provision",
            "start",
            "fetch",
            "setPulse",
            "pollerStart",
            "pollerStop",
            "stop",
          ])
        } finally {
          provisionStub.restore()
          startStub.restore()
          stopStub.restore()
          setPulseStub.restore()
          pollerStartStub.restore()
          pollerStopStub.restore()
          fetchStub.restore()
        }
      },
    )

    await t.step(
      "production virtual mode stops the local pump when registration fails after startup",
      async () => {
        let startCalls = 0
        let stopCalls = 0

        const provisionStub = stub(PathwayProvisioner.prototype, "provision", async () => {})
        const startStub = stub(PathwayPump.prototype, "start", async function (this: PathwayPump) {
          startCalls++
          ;(this as unknown as { running: boolean }).running = true
        })
        const stopStub = stub(PathwayPump.prototype, "stop", async function (this: PathwayPump) {
          if (!(this as unknown as { running: boolean }).running) {
            return
          }
          stopCalls++
          ;(this as unknown as { running: boolean }).running = false
        })
        const fetchStub = stub(globalThis, "fetch", async () => {
          return new Response(
            JSON.stringify({
              status: 500,
              code: "INTERNAL_SERVER_ERROR",
              message: "Internal server error",
            }),
            {
              status: 500,
              headers: { "Content-Type": "application/json" },
            },
          )
        })

        try {
          const builder = createBuilder({
            runtimeEnv: "production",
            pathwayName: "virtual-service",
            pathwayMode: "virtual",
            autoProvision: { pathway: true },
          })
          ;(builder as unknown as { clusterManager: { isRunning: boolean; isLeader: boolean } }).clusterManager = {
            isRunning: true,
            isLeader: true,
          }

          await assertRejects(
            () => builder.startPump(createPumpOptions()),
            Error,
            'Failed to register virtual pathway "virtual-service"',
          )

          assertEquals(startCalls, 1)
          assertEquals(stopCalls, 1)
        } finally {
          provisionStub.restore()
          startStub.restore()
          stopStub.restore()
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
          autoProvision: { pathway: true },
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

    await t.step(
      "production default (no explicit mode) uses managed mode, provisions resources only, skips local pump",
      async () => {
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
          // No explicit pathwayMode; prod defaults to "managed" now.
          const builder = createBuilder({
            runtimeEnv: "production",
            pathwayName: "managed-service",
            managedConfig: {
              endpointUrl: "https://app.example.com/flowcore",
            },
          })

          await builder.startPump(createPumpOptions())

          // Resources on, pathway registration off by default.
          assertEquals(provisionCalls, 1)
          assertEquals(startCalls, 0)
          assertEquals(fetchBodies.length, 0)
        } finally {
          provisionStub.restore()
          startStub.restore()
          fetchStub.restore()
        }
      },
    )

    await t.step(
      "development default provisions resources, does NOT register pathway, starts local pump",
      async () => {
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
          })

          await builder.startPump(createPumpOptions())

          assertEquals(provisionCalls, 1)
          assertEquals(fetchCalls, 0)
          assertEquals(startCalls, 1)
        } finally {
          provisionStub.restore()
          startStub.restore()
          fetchStub.restore()
        }
      },
    )

    await t.step("autoProvision.pathway=true triggers registerPathwayInstance", async () => {
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
          runtimeEnv: "development",
          pathwayName: "dev-service",
          autoProvision: { pathway: true },
        })

        await builder.startPump(createPumpOptions())

        assertEquals(provisionCalls, 1)
        assertEquals(startCalls, 1)
        assertEquals(fetchBodies.length, 1)
        assertEquals(fetchBodies[0].type, "virtual")
      } finally {
        provisionStub.restore()
        startStub.restore()
        fetchStub.restore()
      }
    })

    await t.step("per-startPump autoProvision override wins over builder-level config", async () => {
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
        // Builder-level: resources on, pathway off (default). Override via startPump.
        const builder = createBuilder({
          runtimeEnv: "development",
          pathwayName: "dev-service",
        })

        await builder.startPump({
          ...createPumpOptions(),
          autoProvision: { pathway: true },
        })

        assertEquals(provisionCalls, 1)
        assertEquals(startCalls, 1)
        assertEquals(fetchBodies.length, 1)
      } finally {
        provisionStub.restore()
        startStub.restore()
        fetchStub.restore()
      }
    })

    await t.step("defaultAutoProvision=true maps to resources-on, pathway-off (new default semantics)", async () => {
      let provisionCalls = 0
      let fetchCalls = 0

      const provisionStub = stub(PathwayProvisioner.prototype, "provision", async () => {
        provisionCalls++
      })
      const startStub = stub(PathwayPump.prototype, "start", async () => {})
      const fetchStub = stub(globalThis, "fetch", async () => {
        fetchCalls++
        return new Response("{}", { status: 200 })
      })

      try {
        const builder = createBuilder({
          runtimeEnv: "development",
          pathwayName: "dev-service",
          defaultAutoProvision: true,
        })

        await builder.startPump(createPumpOptions())

        // Legacy `true` → resources only, no pathway registration.
        assertEquals(provisionCalls, 1)
        assertEquals(fetchCalls, 0)
      } finally {
        provisionStub.restore()
        startStub.restore()
        fetchStub.restore()
      }
    })
  },
})
