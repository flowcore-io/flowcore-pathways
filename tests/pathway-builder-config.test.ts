import { assertEquals } from "https://deno.land/std@0.224.0/assert/mod.ts"
import { PathwaysBuilder } from "../src/pathways/builder.ts"

const baseOpts = {
  baseUrl: "https://api.flowcore.io",
  tenant: "test-tenant",
  dataCore: "test-dc",
  apiKey: "fc_testid_testsecret",
}

type InternalBuilderShape = {
  pathwayMode: "virtual" | "managed"
  autoProvision: { dataCore: boolean; flowType: boolean; eventType: boolean; pathway: boolean }
}

// deno-lint-ignore no-explicit-any
function inspect(builder: PathwaysBuilder<any>): InternalBuilderShape {
  return builder as unknown as InternalBuilderShape
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
        runtimeEnv: "production",
        pathwayMode: "managed",
        defaultAutoProvision: false,
        managedConfig: {
          endpointUrl: "https://app.example.com/flowcore",
          authHeaders: {
            "x-service-token": "secret",
          },
          sizeClass: "medium",
        },
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

    await t.step("autoProvision object merges with resources-on/pathway-off defaults", () => {
      const builder = inspect(
        new PathwaysBuilder({
          ...baseOpts,
          runtimeEnv: "development",
          autoProvision: { pathway: true },
          // deno-lint-ignore no-explicit-any
        }) as unknown as PathwaysBuilder<any>,
      )
      assertEquals(builder.autoProvision, {
        dataCore: true,
        flowType: true,
        eventType: true,
        pathway: true,
      })
    })

    await t.step("autoProvision individual field overrides produce expected merge", () => {
      const builder = inspect(
        new PathwaysBuilder({
          ...baseOpts,
          runtimeEnv: "development",
          autoProvision: { dataCore: false, pathway: true },
          // deno-lint-ignore no-explicit-any
        }) as unknown as PathwaysBuilder<any>,
      )
      assertEquals(builder.autoProvision, {
        dataCore: false,
        flowType: true,
        eventType: true,
        pathway: true,
      })
    })

    await t.step("defaultAutoProvision=false maps to all-false", () => {
      const builder = inspect(
        new PathwaysBuilder({
          ...baseOpts,
          runtimeEnv: "development",
          defaultAutoProvision: false,
          // deno-lint-ignore no-explicit-any
        }) as unknown as PathwaysBuilder<any>,
      )
      assertEquals(builder.autoProvision, {
        dataCore: false,
        flowType: false,
        eventType: false,
        pathway: false,
      })
    })

    await t.step("defaultAutoProvision=true maps to resources-on, pathway-off", () => {
      const builder = inspect(
        new PathwaysBuilder({
          ...baseOpts,
          runtimeEnv: "development",
          defaultAutoProvision: true,
          // deno-lint-ignore no-explicit-any
        }) as unknown as PathwaysBuilder<any>,
      )
      assertEquals(builder.autoProvision, {
        dataCore: true,
        flowType: true,
        eventType: true,
        pathway: false,
      })
    })

    await t.step("both unset defaults to resources-on, pathway-off", () => {
      const builder = inspect(
        new PathwaysBuilder({
          ...baseOpts,
          runtimeEnv: "development",
          // deno-lint-ignore no-explicit-any
        }) as unknown as PathwaysBuilder<any>,
      )
      assertEquals(builder.autoProvision, {
        dataCore: true,
        flowType: true,
        eventType: true,
        pathway: false,
      })
    })

    await t.step("autoProvision overrides defaultAutoProvision when both are set", () => {
      const builder = inspect(
        new PathwaysBuilder({
          ...baseOpts,
          runtimeEnv: "development",
          defaultAutoProvision: false,
          autoProvision: { pathway: true },
          // deno-lint-ignore no-explicit-any
        }) as unknown as PathwaysBuilder<any>,
      )
      // autoProvision object wins — resources back on, pathway explicitly on.
      assertEquals(builder.autoProvision, {
        dataCore: true,
        flowType: true,
        eventType: true,
        pathway: true,
      })
    })

    await t.step("pathwayMode default is 'managed' in production", () => {
      const builder = inspect(
        new PathwaysBuilder({
          ...baseOpts,
          runtimeEnv: "production",
          // deno-lint-ignore no-explicit-any
        }) as unknown as PathwaysBuilder<any>,
      )
      assertEquals(builder.pathwayMode, "managed")
    })

    await t.step("pathwayMode default is 'virtual' in development", () => {
      const builder = inspect(
        new PathwaysBuilder({
          ...baseOpts,
          runtimeEnv: "development",
          // deno-lint-ignore no-explicit-any
        }) as unknown as PathwaysBuilder<any>,
      )
      assertEquals(builder.pathwayMode, "virtual")
    })

    await t.step("pathwayMode default is 'virtual' in test", () => {
      const builder = inspect(
        new PathwaysBuilder({
          ...baseOpts,
          runtimeEnv: "test",
          // deno-lint-ignore no-explicit-any
        }) as unknown as PathwaysBuilder<any>,
      )
      assertEquals(builder.pathwayMode, "virtual")
    })

    await t.step("explicit pathwayMode still wins over env-aware default", () => {
      const builder = inspect(
        new PathwaysBuilder({
          ...baseOpts,
          runtimeEnv: "production",
          pathwayMode: "virtual",
          // deno-lint-ignore no-explicit-any
        }) as unknown as PathwaysBuilder<any>,
      )
      assertEquals(builder.pathwayMode, "virtual")
    })
  },
})
