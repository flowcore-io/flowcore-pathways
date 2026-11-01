import { assertEquals } from "https://deno.land/std@0.224.0/assert/mod.ts"
import { stub } from "https://deno.land/std@0.224.0/testing/mock.ts"
import { FlowcoreDataPump } from "@flowcore/data-pump"
import { z } from "zod"
import { PathwaysBuilder } from "../src/pathways/builder.ts"

Deno.test("pump read routing preserves the explicit webhook write URL", async (t) => {
  for (
    const [name, config, expected] of [
      ["omitted keeps single-host compatibility", {}, "https://tenant.webhook.example"],
      ["null uses dedicated SDK read hosts", { pumpBaseUrl: null }, ""],
      ["a string selects a read proxy", { pumpBaseUrl: "https://read-proxy.example" }, "https://read-proxy.example"],
    ] as const
  ) {
    await t.step(name, async () => {
      let readUrl: string | undefined
      let writeUrl = ""
      const createStub = stub(FlowcoreDataPump, "create", (options) => {
        readUrl = options.baseUrlOverride
        return { start: async () => {}, stop: async () => {} } as unknown as ReturnType<
          typeof FlowcoreDataPump.create
        >
      })
      const fetchStub = stub(globalThis, "fetch", async (input) => {
        writeUrl = String(input)
        return Response.json({ eventId: crypto.randomUUID() })
      })
      const builder = new PathwaysBuilder({
        baseUrl: "https://tenant.webhook.example",
        tenant: "synthetic",
        dataCore: "synthetic",
        apiKey: "fc_synthetic_synthetic",
        runtimeEnv: "test",
        ...config,
      }).register({ flowType: "sample.0", eventType: "created.0", schema: z.object({ id: z.string() }) })
      try {
        await builder.write("sample.0/created.0", { data: { id: "synthetic" }, options: { fireAndForget: true } })
        assertEquals(writeUrl, "https://tenant.webhook.example/event/synthetic/synthetic/sample.0/created.0")
        await builder.startPump({ stateManagerFactory: () => ({ getState: () => null, setState: () => {} }) })
        assertEquals(readUrl, expected)
      } finally {
        await builder.stopPump()
        fetchStub.restore()
        createStub.restore()
      }
    })
  }
})
