import { assertEquals } from "https://deno.land/std@0.224.0/assert/mod.ts"
import { stub } from "https://deno.land/std@0.224.0/testing/mock.ts"
import { z } from "zod"
import { PathwaysBuilder, type PathwaysBuilderConfig } from "../src/pathways/builder.ts"
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
    schema: z.object({ id: z.string() }),
    flowTypeDescription: "User events",
    description: "User created",
  })
}

Deno.test({
  name: "PathwaysBuilder.provision honors autoProvision",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: async (t) => {
    await t.step(
      "managed prod default — provisions shared resources, skips pathway upsert (no endpointUrl needed)",
      async () => {
        let provisionCalls = 0
        let fetchCalls = 0
        const provisionStub = stub(PathwayProvisioner.prototype, "provision", async () => {
          provisionCalls++
        })
        const fetchStub = stub(globalThis, "fetch", async () => {
          fetchCalls++
          return new Response("{}", { status: 200 })
        })

        try {
          const builder = createBuilder({
            runtimeEnv: "production",
            // pathwayMode defaults to "managed" in production; intentionally NO managedConfig
          })

          await builder.provision()

          assertEquals(provisionCalls, 1, "shared resources should be provisioned once")
          assertEquals(fetchCalls, 0, "no pathway upsert HTTP call when pathway autoProvision is off")
        } finally {
          provisionStub.restore()
          fetchStub.restore()
        }
      },
    )

    await t.step(
      "autoProvision.pathway=true upserts the managed pathway (requires managedConfig)",
      async () => {
        let provisionCalls = 0
        const fetchBodies: Array<Record<string, unknown>> = []
        const provisionStub = stub(PathwayProvisioner.prototype, "provision", async () => {
          provisionCalls++
        })
        const fetchStub = stub(globalThis, "fetch", async (_input, init) => {
          fetchBodies.push(JSON.parse(String((init as RequestInit | undefined)?.body ?? "{}")))
          return new Response(JSON.stringify({ pathwayId: crypto.randomUUID(), status: "created" }), {
            status: 200,
            headers: { "Content-Type": "application/json" },
          })
        })

        try {
          const builder = createBuilder({
            runtimeEnv: "production",
            pathwayName: "managed-service",
            autoProvision: { pathway: true },
            managedConfig: {
              endpointUrl: "https://example.com/api/transformer",
              authHeaders: { "x-test": "1" },
            },
          })

          await builder.provision()

          assertEquals(provisionCalls, 1)
          assertEquals(fetchBodies.length, 1)
          assertEquals(fetchBodies[0].type, "managed")
        } finally {
          provisionStub.restore()
          fetchStub.restore()
        }
      },
    )

    await t.step(
      "virtual pathway default — provisions shared resources, skips by-name pathway upsert",
      async () => {
        let provisionCalls = 0
        let fetchCalls = 0
        const provisionStub = stub(PathwayProvisioner.prototype, "provision", async () => {
          provisionCalls++
        })
        const fetchStub = stub(globalThis, "fetch", async () => {
          fetchCalls++
          return new Response("{}", { status: 200 })
        })

        try {
          const builder = createBuilder({
            runtimeEnv: "production",
            pathwayMode: "virtual",
            pathwayName: "virtual-service",
          })

          await builder.provision()

          assertEquals(provisionCalls, 1)
          assertEquals(fetchCalls, 0, "by-name virtual upsert should be skipped when pathway autoProvision is off")
        } finally {
          provisionStub.restore()
          fetchStub.restore()
        }
      },
    )

    await t.step("autoProvision=false skips everything", async () => {
      let provisionCalls = 0
      let fetchCalls = 0
      const provisionStub = stub(PathwayProvisioner.prototype, "provision", async () => {
        provisionCalls++
      })
      const fetchStub = stub(globalThis, "fetch", async () => {
        fetchCalls++
        return new Response("{}", { status: 200 })
      })

      try {
        const builder = createBuilder({
          runtimeEnv: "production",
          defaultAutoProvision: false,
        })

        await builder.provision()

        assertEquals(provisionCalls, 0)
        assertEquals(fetchCalls, 0)
      } finally {
        provisionStub.restore()
        fetchStub.restore()
      }
    })

    await t.step("per-call override wins over builder-level autoProvision", async () => {
      let provisionCalls = 0
      const fetchBodies: Array<Record<string, unknown>> = []
      const provisionStub = stub(PathwayProvisioner.prototype, "provision", async () => {
        provisionCalls++
      })
      const fetchStub = stub(globalThis, "fetch", async (_input, init) => {
        fetchBodies.push(JSON.parse(String((init as RequestInit | undefined)?.body ?? "{}")))
        return new Response(JSON.stringify({ pathwayId: crypto.randomUUID(), status: "created" }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        })
      })

      try {
        // Builder says pathway: false (default), per-call override flips it on.
        const builder = createBuilder({
          runtimeEnv: "production",
          pathwayName: "managed-service",
          managedConfig: { endpointUrl: "https://example.com/api/transformer" },
        })

        await builder.provision({ pathway: true })

        assertEquals(provisionCalls, 1)
        assertEquals(fetchBodies.length, 1)
        assertEquals(fetchBodies[0].type, "managed")
      } finally {
        provisionStub.restore()
        fetchStub.restore()
      }
    })
  },
})
