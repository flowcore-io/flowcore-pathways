import { assertEquals, assertThrows } from "https://deno.land/std@0.224.0/assert/mod.ts"
import { z } from "npm:zod@^3.25.63"
import { PathwaysBuilder } from "../src/pathways/builder.ts"

const baseOpts = {
  baseUrl: "https://api.flowcore.io",
  tenant: "test-tenant",
  dataCore: "test-dc",
  apiKey: "fc_testid_testsecret",
}

const flowOrders = "orders.0"
const eventPlaced = "placed.0"
const eventPlacedFast = "placed.fast.0"
const pathPlaced = `${flowOrders}/${eventPlaced}` as const
const pathPlacedFast = `${flowOrders}/${eventPlacedFast}` as const

type PumpRegistration = { flowType: string; eventType: string; pumpGroup: string }

type BuilderInternals = {
  pumpGroups: Record<string, string>
  buildPumpRegistrations(): PumpRegistration[]
}

// deno-lint-ignore no-explicit-any
function inspect(builder: PathwaysBuilder<any>): BuilderInternals {
  return builder as unknown as BuilderInternals
}

const schema = z.object({ id: z.string() })

Deno.test({
  name: "PathwaysBuilder.register — pumpGroup",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: async (t) => {
    await t.step("defaults pumpGroup to 'default' when omitted", () => {
      const builder = new PathwaysBuilder(baseOpts).register({
        flowType: flowOrders,
        eventType: eventPlaced,
        schema,
      })

      const internals = inspect(builder)
      assertEquals(internals.pumpGroups[pathPlaced], "default")
    })

    await t.step("persists explicit pumpGroup value", () => {
      const builder = new PathwaysBuilder(baseOpts).register({
        flowType: flowOrders,
        eventType: eventPlaced,
        schema,
        pumpGroup: "hot",
      })

      const internals = inspect(builder)
      assertEquals(internals.pumpGroups[pathPlaced], "hot")
    })

    await t.step("trims whitespace in pumpGroup", () => {
      const builder = new PathwaysBuilder(baseOpts).register({
        flowType: flowOrders,
        eventType: eventPlaced,
        schema,
        pumpGroup: "  hot  ",
      })

      const internals = inspect(builder)
      assertEquals(internals.pumpGroups[pathPlaced], "hot")
    })

    await t.step("rejects empty pumpGroup", () => {
      assertThrows(
        () => {
          new PathwaysBuilder(baseOpts).register({
            flowType: flowOrders,
            eventType: eventPlaced,
            schema,
            pumpGroup: "",
          })
        },
        Error,
        "pumpGroup must be a non-empty string",
      )
    })

    await t.step("rejects whitespace-only pumpGroup", () => {
      assertThrows(
        () => {
          new PathwaysBuilder(baseOpts).register({
            flowType: flowOrders,
            eventType: eventPlaced,
            schema,
            pumpGroup: "   ",
          })
        },
        Error,
        "pumpGroup must be a non-empty string",
      )
    })

    await t.step(
      "buildPumpRegistrations emits pumpGroup and produces two distinct entries for two groups on one flow type",
      () => {
        const builder = new PathwaysBuilder(baseOpts)
          .register({ flowType: flowOrders, eventType: eventPlaced, schema })
          .register({
            flowType: flowOrders,
            eventType: eventPlacedFast,
            schema,
            pumpGroup: "hot",
          })

        const regs = inspect(builder).buildPumpRegistrations()
        const sorted = [...regs].sort((a, b) => a.eventType.localeCompare(b.eventType))
        assertEquals(sorted, [
          { flowType: flowOrders, eventType: eventPlaced, pumpGroup: "default" },
          { flowType: flowOrders, eventType: eventPlacedFast, pumpGroup: "hot" },
        ])
      },
    )

    await t.step("buildPumpRegistrations excludes subscribe: false pathways", () => {
      const builder = new PathwaysBuilder(baseOpts)
        .register({ flowType: flowOrders, eventType: eventPlaced, schema })
        .register({
          flowType: flowOrders,
          eventType: eventPlacedFast,
          schema,
          pumpGroup: "hot",
          subscribe: false,
        })

      const regs = inspect(builder).buildPumpRegistrations()
      assertEquals(regs.length, 1)
      assertEquals(regs[0].eventType, eventPlaced)
    })

    await t.step("explicit 'default' pumpGroup is accepted as a no-op alias", () => {
      const builder = new PathwaysBuilder(baseOpts).register({
        flowType: flowOrders,
        eventType: eventPlaced,
        schema,
        pumpGroup: "default",
      })

      const internals = inspect(builder)
      assertEquals(internals.pumpGroups[pathPlaced], "default")
    })
  },
})
