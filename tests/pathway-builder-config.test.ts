import { assertEquals } from "https://deno.land/std@0.224.0/assert/mod.ts"
import { PathwaysBuilder } from "../src/pathways/builder.ts"

const baseOpts = {
  baseUrl: "https://api.flowcore.io",
  tenant: "test-tenant",
  dataCore: "test-dc",
  apiKey: "fc_testid_testsecret",
}

Deno.test({
  name: "PathwaysBuilder config — virtual pathway fields",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: async (t) => {
    await t.step("should accept all new fields without error", () => {
      const builder = new PathwaysBuilder({
        ...baseOpts,
        pathwayName: "my-service",
        pulseUrl: "https://custom-cp.example.com",
        pulseIntervalMs: 15000,
        commandPollingIntervalMs: 3000,
      })

      assertEquals(typeof builder, "object")
    })

    await t.step("should accept deprecated fields without error (backward compat)", () => {
      const builder = new PathwaysBuilder({
        ...baseOpts,
        pathwayName: "my-service",
        advertisedUrl: "https://my-service.example.com",
        resetSecret: "my-reset-secret",
        resetPath: "/admin/reset",
      })

      assertEquals(typeof builder, "object")
    })

    await t.step("should accept pathwayName without advertisedUrl or resetSecret", () => {
      const builder = new PathwaysBuilder({
        ...baseOpts,
        pathwayName: "my-service",
      })

      assertEquals(typeof builder, "object")
    })

    await t.step("should work without pathwayName (backward compat)", () => {
      const builder = new PathwaysBuilder(baseOpts)
      assertEquals(typeof builder, "object")
    })

    await t.step("should work with only pulseUrl/pulseIntervalMs (no pathwayName)", () => {
      const builder = new PathwaysBuilder({
        ...baseOpts,
        pulseUrl: "https://custom-cp.example.com/pulse",
        pulseIntervalMs: 60000,
      })
      assertEquals(typeof builder, "object")
    })
  },
})
