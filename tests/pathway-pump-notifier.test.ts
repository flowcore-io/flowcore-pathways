import { assertEquals } from "https://deno.land/std@0.224.0/assert/mod.ts"
import { PathwayPump } from "../src/pathways/pump/pathway-pump.ts"
import type { PumpNotifierConfig, PumpStateManagerFactory } from "../src/pathways/pump/types.ts"

/**
 * Regression coverage for the notifier options handed to `@flowcore/data-pump`.
 *
 * The pump discriminates on `notifier.type` and reads `servers` / `intervalMs` off the
 * matching variant (`FlowcoreDataPumpNotifierOptions`). Before this suite existed we
 * emitted `{ dataSource, auth, pollerIntervalMs }` — no `type`, wrong key names — so both
 * discriminator checks resolved to `undefined` and every pump silently fell back to the
 * websocket notifier no matter what the caller configured.
 *
 * These assertions compare the emitted object exactly, so an extra or renamed key fails
 * here rather than degrading a consumer's pump in production.
 */

function stateFactory(): PumpStateManagerFactory {
  return () => ({
    getState: () => null,
    setState: () => {},
  })
}

interface InternalPump {
  dataPumpConstructor: {
    create(options: Record<string, unknown>): Promise<{ start(cb?: unknown): Promise<void> }>
  }
  startPumpForGroup(meta: { flowType: string; pumpGroup: string; eventTypes: string[] }): Promise<void>
}

/** Start one pump with the given notifier config and return the full options object it built. */
async function capturePumpOptions(notifier?: PumpNotifierConfig): Promise<Record<string, unknown>> {
  const pump = new PathwayPump({
    stateManagerFactory: stateFactory(),
    ...(notifier ? { notifier } : {}),
  })

  pump.configure({
    tenant: "test-tenant",
    dataCore: "test-dc",
    apiKey: "fc_test-key-id_test-secret",
    baseUrl: "https://api.flowcore.io",
    processEvent: async () => {},
  })

  let captured: Record<string, unknown> | undefined
  const internal = pump as unknown as InternalPump
  internal.dataPumpConstructor = {
    create: (options: Record<string, unknown>) => {
      captured = options
      return Promise.resolve({ start: async () => {} })
    },
  }

  await internal.startPumpForGroup({ flowType: "user", pumpGroup: "default", eventTypes: ["created"] })

  if (!captured) {
    throw new Error("data pump was never constructed")
  }
  return captured
}

Deno.test({
  name: "PathwayPump notifier options",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: async (t) => {
    await t.step("websocket is emitted with its type discriminator", async () => {
      const options = await capturePumpOptions({ type: "websocket" })

      assertEquals(options.notifier, { type: "websocket" })
    })

    await t.step("omitting the notifier defaults to websocket", async () => {
      const options = await capturePumpOptions()

      assertEquals(options.notifier, { type: "websocket" })
    })

    await t.step("poller maps pollerIntervalMs to the pump's intervalMs", async () => {
      const options = await capturePumpOptions({ type: "poller", pollerIntervalMs: 5000 })

      // Exact match: a stray `pollerIntervalMs` alongside `intervalMs` would mean the
      // pump silently ignores the interval and reverts to websocket.
      assertEquals(options.notifier, { type: "poller", intervalMs: 5000 })
    })

    await t.step("nats maps natsServers to the pump's servers", async () => {
      const servers = ["nats://one:4222", "nats://two:4222"]
      const options = await capturePumpOptions({ type: "nats", natsServers: servers })

      assertEquals(options.notifier, { type: "nats", servers })
    })

    await t.step("every notifier variant carries a type discriminator", async () => {
      const configs: PumpNotifierConfig[] = [
        { type: "websocket" },
        { type: "poller", pollerIntervalMs: 1000 },
        { type: "nats", natsServers: ["nats://one:4222"] },
      ]

      for (const config of configs) {
        const options = await capturePumpOptions(config)
        const notifier = options.notifier as { type?: string }

        // Without `type` the pump cannot select nats or poller — this is the exact
        // failure mode that made both configurations dead options.
        assertEquals(notifier.type, config.type)
      }
    })

    await t.step("auth and dataSource stay top-level, where the notifier reads them", async () => {
      const options = await capturePumpOptions({ type: "poller", pollerIntervalMs: 1000 })

      assertEquals(options.auth, { apiKey: "fc_test-key-id_test-secret" })
      assertEquals(options.dataSource, {
        tenant: "test-tenant",
        dataCore: "test-dc",
        flowType: "user",
        eventTypes: ["created"],
      })
    })
  },
})
