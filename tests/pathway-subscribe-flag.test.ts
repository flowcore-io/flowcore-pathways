import { assertEquals } from "https://deno.land/std@0.224.0/assert/mod.ts"
import { z } from "npm:zod@^3.25.63"
import { PathwaysBuilder } from "../src/pathways/builder.ts"

const baseOpts = {
  baseUrl: "https://api.flowcore.io",
  tenant: "test-tenant",
  dataCore: "test-dc",
  apiKey: "fc_testid_testsecret",
}

const flowA = "flow-a.0"
const flowB = "flow-b.0"
const eventCreated = "thing.created.0"
const pathA = `${flowA}/${eventCreated}` as const
const pathB = `${flowB}/${eventCreated}` as const

type ProvisionerRegistration = {
  flowType: string
  eventType: string
  flowTypeDescription?: string
  eventTypeDescription?: string
}

type BuilderInternals = {
  subscribed: Record<string, boolean>
  writable: Record<string, boolean>
  writers: Record<string, unknown>
  buildRegistrations(): ProvisionerRegistration[]
  buildSubscribedRegistrations(): ProvisionerRegistration[]
}

// deno-lint-ignore no-explicit-any
function inspect(builder: PathwaysBuilder<any>): BuilderInternals {
  return builder as unknown as BuilderInternals
}

const schema = z.object({ id: z.string() })

Deno.test({
  name: "PathwaysBuilder.register — subscribe flag",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: async (t) => {
    await t.step("defaults subscribe to true when omitted", () => {
      const builder = new PathwaysBuilder(baseOpts).register({
        flowType: flowA,
        eventType: eventCreated,
        schema,
      })

      const internals = inspect(builder)
      assertEquals(internals.subscribed[pathA], true)
      assertEquals(internals.writable[pathA], true)
    })

    await t.step("records subscribe: false when explicitly opted out", () => {
      const builder = new PathwaysBuilder(baseOpts).register({
        flowType: flowA,
        eventType: eventCreated,
        schema,
        subscribe: false,
      })

      const internals = inspect(builder)
      assertEquals(internals.subscribed[pathA], false)
    })

    await t.step("subscribe: false still wires write capability", () => {
      const builder = new PathwaysBuilder(baseOpts).register({
        flowType: flowA,
        eventType: eventCreated,
        schema,
        subscribe: false,
      })

      const internals = inspect(builder)
      // writable defaults to true; .write() must remain available even when
      // subscribe is false — the whole point of the flag.
      assertEquals(internals.writable[pathA], true)
      assertEquals(typeof internals.writers[pathA], "function")
    })

    await t.step("buildRegistrations includes write-only pathways (provisioning)", () => {
      const builder = new PathwaysBuilder(baseOpts)
        .register({ flowType: flowA, eventType: eventCreated, schema })
        .register({ flowType: flowB, eventType: eventCreated, schema, subscribe: false })

      const all = inspect(builder).buildRegistrations()
      const flowTypes = all.map((r) => r.flowType).sort()
      assertEquals(flowTypes, [flowA, flowB])
    })

    await t.step("buildSubscribedRegistrations excludes subscribe: false pathways", () => {
      const builder = new PathwaysBuilder(baseOpts)
        .register({ flowType: flowA, eventType: eventCreated, schema })
        .register({ flowType: flowB, eventType: eventCreated, schema, subscribe: false })

      const subscribed = inspect(builder).buildSubscribedRegistrations()
      const flowTypes = subscribed.map((r) => r.flowType)
      assertEquals(flowTypes, [flowA])
    })

    await t.step("buildSubscribedRegistrations matches buildRegistrations when no opt-out", () => {
      const builder = new PathwaysBuilder(baseOpts)
        .register({ flowType: flowA, eventType: eventCreated, schema })
        .register({ flowType: flowB, eventType: eventCreated, schema })

      const internals = inspect(builder)
      const all = internals.buildRegistrations()
      const subscribed = internals.buildSubscribedRegistrations()
      assertEquals(subscribed.length, all.length)
      assertEquals(
        subscribed.map((r) => r.flowType).sort(),
        all.map((r) => r.flowType).sort(),
      )
    })
  },
})
