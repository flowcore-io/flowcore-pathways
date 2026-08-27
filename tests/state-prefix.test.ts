import { assertEquals, assertThrows } from "https://deno.land/std@0.224.0/assert/mod.ts"
import {
  DEFAULT_STATE_NAMES,
  MAX_STATE_PREFIX_LENGTH,
  normalizeStatePrefix,
  prefixStateName,
} from "../src/pathways/state-prefix.ts"

Deno.test({
  name: "State prefix normalization",
  fn: async (t) => {
    await t.step("no prefix produces an empty string", () => {
      assertEquals(normalizeStatePrefix(undefined), "")
      assertEquals(normalizeStatePrefix(""), "")
    })

    await t.step("a prefix gains exactly one trailing underscore", () => {
      assertEquals(normalizeStatePrefix("compute_api"), "compute_api_")
      assertEquals(normalizeStatePrefix("compute_api_"), "compute_api_")
      assertEquals(normalizeStatePrefix("compute_api___"), "compute_api_")
    })

    await t.step("a leading underscore is legal", () => {
      assertEquals(normalizeStatePrefix("_internal"), "_internal_")
    })

    await t.step("rejects characters that are illegal in a SQL identifier", () => {
      // The prefix reaches SQL through string interpolation, so anything outside
      // the unquoted-identifier character set MUST be refused.
      for (
        const bad of [
          "compute-api",
          "compute api",
          "1compute",
          'compute"api',
          "compute;drop table pathway_leases;--",
          "compute.api",
          "compute'api",
          "público",
        ]
      ) {
        assertThrows(() => normalizeStatePrefix(bad), Error, "Invalid statePrefix")
      }
    })

    await t.step("rejects a prefix made only of underscores", () => {
      assertThrows(() => normalizeStatePrefix("__"), Error, "more than underscores")
    })

    await t.step("rejects a prefix that is too long", () => {
      const tooLong = "a".repeat(MAX_STATE_PREFIX_LENGTH + 1)
      assertThrows(() => normalizeStatePrefix(tooLong), Error, "Invalid statePrefix")
      assertEquals(normalizeStatePrefix("a".repeat(MAX_STATE_PREFIX_LENGTH)).length, MAX_STATE_PREFIX_LENGTH + 1)
    })
  },
})

Deno.test({
  name: "State prefix application",
  fn: async (t) => {
    await t.step("no prefix keeps every historical name unchanged", () => {
      // Acceptance 3: existing deployments must not need a migration.
      assertEquals(prefixStateName(undefined, DEFAULT_STATE_NAMES.pathwayState), "pathway_state")
      assertEquals(prefixStateName(undefined, DEFAULT_STATE_NAMES.leases), "pathway_leases")
      assertEquals(prefixStateName(undefined, DEFAULT_STATE_NAMES.instances), "pathway_instances")
      assertEquals(prefixStateName(undefined, DEFAULT_STATE_NAMES.pumpState), "pathway_pump_state")
      assertEquals(prefixStateName(undefined, DEFAULT_STATE_NAMES.leaseKey), "pathway-cluster-leader")
    })

    await t.step("a prefix namespaces every state name, including the lease key", () => {
      assertEquals(prefixStateName("compute_api", DEFAULT_STATE_NAMES.pathwayState), "compute_api_pathway_state")
      assertEquals(prefixStateName("compute_api", DEFAULT_STATE_NAMES.leases), "compute_api_pathway_leases")
      assertEquals(prefixStateName("compute_api", DEFAULT_STATE_NAMES.instances), "compute_api_pathway_instances")
      assertEquals(prefixStateName("compute_api", DEFAULT_STATE_NAMES.pumpState), "compute_api_pathway_pump_state")
      assertEquals(prefixStateName("compute_api", DEFAULT_STATE_NAMES.leaseKey), "compute_api_pathway-cluster-leader")
    })

    await t.step("different prefixes never collide", () => {
      const api = prefixStateName("compute_api", DEFAULT_STATE_NAMES.leaseKey)
      const reconciler = prefixStateName("compute_reconciler", DEFAULT_STATE_NAMES.leaseKey)
      assertEquals(api === reconciler, false)
    })
  },
})
