import { assertEquals, assertThrows } from "https://deno.land/std@0.224.0/assert/mod.ts"
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
        advertisedUrl: "https://my-service.example.com",
        resetSecret: "my-reset-secret",
        resetPath: "/admin/reset",
        pulseUrl: "https://custom-cp.example.com/api/v1/pump-pulse",
        pulseIntervalMs: 15000,
      })

      assertEquals(typeof builder, "object")
    })

    await t.step("should throw when pathwayName set but advertisedUrl missing", () => {
      assertThrows(
        () =>
          new PathwaysBuilder({
            ...baseOpts,
            pathwayName: "my-service",
            resetSecret: "my-reset-secret",
          }),
        Error,
        "advertisedUrl is required when pathwayName is set",
      )
    })

    await t.step("should throw when pathwayName set but resetSecret missing", () => {
      assertThrows(
        () =>
          new PathwaysBuilder({
            ...baseOpts,
            pathwayName: "my-service",
            advertisedUrl: "https://my-service.example.com",
          }),
        Error,
        "resetSecret is required when pathwayName is set",
      )
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
