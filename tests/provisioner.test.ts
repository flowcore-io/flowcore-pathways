import { assertEquals, assertRejects } from "https://deno.land/std@0.224.0/assert/mod.ts"
import { PathwayProvisioner } from "../src/pathways/provisioner.ts"
import type { FlowcoreClient } from "@flowcore/sdk"
import { NotFoundException } from "@flowcore/sdk"

// --- Mock FlowcoreClient ---

interface MockCommand {
  input: Record<string, unknown>
  constructor: { name: string }
}

type CommandHandler = (cmd: MockCommand) => unknown

function createMockClient(handlers: Record<string, CommandHandler>): FlowcoreClient {
  return {
    execute(cmd: MockCommand) {
      const name = cmd.constructor.name
      const handler = handlers[name]
      if (!handler) {
        throw new Error(`Unexpected command: ${name}`)
      }
      return Promise.resolve(handler(cmd))
    },
  } as unknown as FlowcoreClient
}

// --- Helpers ---

function baseTenant() {
  return {
    id: "tenant-id-001",
    name: "my-org",
    displayName: "My Org",
    description: "",
    websiteUrl: "",
    isDedicated: false,
    dedicated: null,
  }
}

function baseDataCore(overrides?: Partial<{ id: string; description: string }>) {
  return {
    id: overrides?.id ?? "dc-id-001",
    tenantId: "tenant-id-001",
    tenant: "my-org",
    name: "my-core",
    description: overrides?.description ?? "Original description",
    accessControl: "private" as const,
    deleteProtection: false,
    isDeleting: false,
    isFlowcoreManaged: false,
  }
}

function baseFlowType(name: string, id: string, description = "") {
  return { id, tenantId: "tenant-id-001", dataCoreId: "dc-id-001", name, description, isDeleting: false }
}

function baseEventType(name: string, id: string, flowTypeId: string, description = "") {
  return {
    id,
    tenantId: "tenant-id-001",
    dataCoreId: "dc-id-001",
    flowTypeId,
    name,
    description,
    isTruncating: false,
    isDeleting: false,
    createdAt: "2025-01-01T00:00:00Z",
    updatedAt: null,
  }
}

Deno.test({
  name: "Provisioner Tests",
  sanitizeResources: false,
  sanitizeOps: false,
  fn: async (t) => {
    await t.step("creates missing data core when description provided", async () => {
      const commands: string[] = []

      const client = createMockClient({
        TenantTranslateNameToIdCommand: () => baseTenant(),
        DataCoreFetchCommand: () => {
          throw new NotFoundException("DataCore", {})
        },
        DataCoreCreateCommand: (cmd) => {
          commands.push("DataCoreCreateCommand")
          assertEquals(cmd.input.name, "my-core")
          assertEquals(cmd.input.description, "My data core")
          assertEquals(cmd.input.tenantId, "tenant-id-001")
          return baseDataCore({ description: "My data core" })
        },
        FlowTypeListCommand: () => [],
        FlowTypeCreateCommand: (cmd) => {
          commands.push("FlowTypeCreateCommand")
          return baseFlowType(cmd.input.name as string, "ft-new", cmd.input.description as string)
        },
        EventTypeListCommand: () => [],
        EventTypeCreateCommand: (cmd) => {
          commands.push("EventTypeCreateCommand")
          return baseEventType(
            cmd.input.name as string,
            "et-new",
            cmd.input.flowTypeId as string,
            cmd.input.description as string,
          )
        },
      })

      const provisioner = new PathwayProvisioner({
        tenant: "my-org",
        dataCore: "my-core",
        apiKey: "fc_test_key",
        dataCoreDescription: "My data core",
        dataCoreAccessControl: "private",
        dataCoreDeleteProtection: false,
        registrations: [
          {
            flowType: "user",
            eventType: "created",
            flowTypeDescription: "User events",
            eventTypeDescription: "User created",
          },
        ],
        clientFactory: () => client,
      })

      await provisioner.provision()

      assertEquals(commands.includes("DataCoreCreateCommand"), true)
      assertEquals(commands.includes("FlowTypeCreateCommand"), true)
      assertEquals(commands.includes("EventTypeCreateCommand"), true)
    })

    await t.step("fails when data core missing and no description", async () => {
      const client = createMockClient({
        TenantTranslateNameToIdCommand: () => baseTenant(),
        DataCoreFetchCommand: () => {
          throw new NotFoundException("DataCore", {})
        },
      })

      const provisioner = new PathwayProvisioner({
        tenant: "my-org",
        dataCore: "my-core",
        apiKey: "fc_test_key",
        dataCoreAccessControl: "private",
        dataCoreDeleteProtection: false,
        registrations: [],
        clientFactory: () => client,
      })

      await assertRejects(
        () => provisioner.provision(),
        Error,
        'Data core "my-core" not found',
      )
    })

    await t.step("fails when flow type missing and no flowTypeDescription", async () => {
      const client = createMockClient({
        TenantTranslateNameToIdCommand: () => baseTenant(),
        DataCoreFetchCommand: () => baseDataCore({ description: "desc" }),
        FlowTypeListCommand: () => [],
      })

      const provisioner = new PathwayProvisioner({
        tenant: "my-org",
        dataCore: "my-core",
        apiKey: "fc_test_key",
        dataCoreDescription: "desc",
        dataCoreAccessControl: "private",
        dataCoreDeleteProtection: false,
        registrations: [
          { flowType: "user", eventType: "created", eventTypeDescription: "User created" },
        ],
        clientFactory: () => client,
      })

      await assertRejects(
        () => provisioner.provision(),
        Error,
        'Flow type "user" not found',
      )
    })

    await t.step("fails when event type missing and no description", async () => {
      const client = createMockClient({
        TenantTranslateNameToIdCommand: () => baseTenant(),
        DataCoreFetchCommand: () => baseDataCore({ description: "desc" }),
        FlowTypeListCommand: () => [baseFlowType("user", "ft-001")],
        EventTypeListCommand: () => [],
      })

      const provisioner = new PathwayProvisioner({
        tenant: "my-org",
        dataCore: "my-core",
        apiKey: "fc_test_key",
        dataCoreDescription: "desc",
        dataCoreAccessControl: "private",
        dataCoreDeleteProtection: false,
        registrations: [
          { flowType: "user", eventType: "created" },
        ],
        clientFactory: () => client,
      })

      await assertRejects(
        () => provisioner.provision(),
        Error,
        'Event type "created" not found in flow type "user"',
      )
    })

    await t.step("updates data core description when changed", async () => {
      let updatedDescription: string | undefined

      const client = createMockClient({
        TenantTranslateNameToIdCommand: () => baseTenant(),
        DataCoreFetchCommand: () => baseDataCore({ description: "Old desc" }),
        DataCoreUpdateCommand: (cmd) => {
          updatedDescription = cmd.input.description as string
          return baseDataCore({ description: "New desc" })
        },
        FlowTypeListCommand: () => [baseFlowType("user", "ft-001", "User events")],
        EventTypeListCommand: () => [baseEventType("created", "et-001", "ft-001", "User created")],
      })

      const provisioner = new PathwayProvisioner({
        tenant: "my-org",
        dataCore: "my-core",
        apiKey: "fc_test_key",
        dataCoreDescription: "New desc",
        dataCoreAccessControl: "private",
        dataCoreDeleteProtection: false,
        registrations: [
          {
            flowType: "user",
            eventType: "created",
            flowTypeDescription: "User events",
            eventTypeDescription: "User created",
          },
        ],
        clientFactory: () => client,
      })

      await provisioner.provision()
      assertEquals(updatedDescription, "New desc")
    })

    await t.step("updates flow type description when changed", async () => {
      let updatedFlowDesc: string | undefined

      const client = createMockClient({
        TenantTranslateNameToIdCommand: () => baseTenant(),
        DataCoreFetchCommand: () => baseDataCore(),
        FlowTypeListCommand: () => [baseFlowType("user", "ft-001", "Old flow desc")],
        FlowTypeUpdateCommand: (cmd) => {
          updatedFlowDesc = cmd.input.description as string
          return baseFlowType("user", "ft-001", "New flow desc")
        },
        EventTypeListCommand: () => [baseEventType("created", "et-001", "ft-001", "User created")],
      })

      const provisioner = new PathwayProvisioner({
        tenant: "my-org",
        dataCore: "my-core",
        apiKey: "fc_test_key",
        dataCoreAccessControl: "private",
        dataCoreDeleteProtection: false,
        registrations: [
          {
            flowType: "user",
            eventType: "created",
            flowTypeDescription: "New flow desc",
            eventTypeDescription: "User created",
          },
        ],
        clientFactory: () => client,
      })

      await provisioner.provision()
      assertEquals(updatedFlowDesc, "New flow desc")
    })

    await t.step("updates event type description when changed", async () => {
      let updatedEventDesc: string | undefined

      const client = createMockClient({
        TenantTranslateNameToIdCommand: () => baseTenant(),
        DataCoreFetchCommand: () => baseDataCore(),
        FlowTypeListCommand: () => [baseFlowType("user", "ft-001")],
        EventTypeListCommand: () => [baseEventType("created", "et-001", "ft-001", "Old event desc")],
        EventTypeUpdateCommand: (cmd) => {
          updatedEventDesc = cmd.input.description as string
          return baseEventType("created", "et-001", "ft-001", "New event desc")
        },
      })

      const provisioner = new PathwayProvisioner({
        tenant: "my-org",
        dataCore: "my-core",
        apiKey: "fc_test_key",
        dataCoreAccessControl: "private",
        dataCoreDeleteProtection: false,
        registrations: [
          { flowType: "user", eventType: "created", eventTypeDescription: "New event desc" },
        ],
        clientFactory: () => client,
      })

      await provisioner.provision()
      assertEquals(updatedEventDesc, "New event desc")
    })

    await t.step("handles already-in-sync gracefully (no updates)", async () => {
      const commands: string[] = []

      const client = createMockClient({
        TenantTranslateNameToIdCommand: () => {
          commands.push("TenantTranslateNameToIdCommand")
          return baseTenant()
        },
        DataCoreFetchCommand: () => {
          commands.push("DataCoreFetchCommand")
          return baseDataCore({ description: "My core" })
        },
        FlowTypeListCommand: () => {
          commands.push("FlowTypeListCommand")
          return [baseFlowType("user", "ft-001", "User events")]
        },
        EventTypeListCommand: () => {
          commands.push("EventTypeListCommand")
          return [baseEventType("created", "et-001", "ft-001", "User created")]
        },
      })

      const provisioner = new PathwayProvisioner({
        tenant: "my-org",
        dataCore: "my-core",
        apiKey: "fc_test_key",
        dataCoreDescription: "My core",
        dataCoreAccessControl: "private",
        dataCoreDeleteProtection: false,
        registrations: [
          {
            flowType: "user",
            eventType: "created",
            flowTypeDescription: "User events",
            eventTypeDescription: "User created",
          },
        ],
        clientFactory: () => client,
      })

      await provisioner.provision()

      // Should only have read commands, no create/update
      assertEquals(commands, [
        "TenantTranslateNameToIdCommand",
        "DataCoreFetchCommand",
        "FlowTypeListCommand",
        "EventTypeListCommand",
      ])
    })

    await t.step("never deletes anything (additive only)", async () => {
      const commands: string[] = []

      const client = createMockClient({
        TenantTranslateNameToIdCommand: () => baseTenant(),
        DataCoreFetchCommand: () => baseDataCore(),
        FlowTypeListCommand: () => {
          // Return extra flow types that are NOT in registrations
          return [
            baseFlowType("user", "ft-001", "User events"),
            baseFlowType("order", "ft-002", "Order events"),
          ]
        },
        EventTypeListCommand: () => {
          // Return extra event types
          return [
            baseEventType("created", "et-001", "ft-001", "User created"),
            baseEventType("updated", "et-002", "ft-001", "User updated"),
          ]
        },
      })

      const provisioner = new PathwayProvisioner({
        tenant: "my-org",
        dataCore: "my-core",
        apiKey: "fc_test_key",
        dataCoreAccessControl: "private",
        dataCoreDeleteProtection: false,
        registrations: [
          {
            flowType: "user",
            eventType: "created",
            flowTypeDescription: "User events",
            eventTypeDescription: "User created",
          },
        ],
        clientFactory: () => client,
      })

      await provisioner.provision()

      // No delete commands should appear
      assertEquals(commands.filter((c) => c.includes("Delete")).length, 0)
    })

    await t.step("does not update data core description when not provided", async () => {
      const commands: string[] = []

      const client = createMockClient({
        TenantTranslateNameToIdCommand: () => baseTenant(),
        DataCoreFetchCommand: () => {
          commands.push("DataCoreFetchCommand")
          return baseDataCore({ description: "Existing desc" })
        },
        FlowTypeListCommand: () => [baseFlowType("user", "ft-001", "User events")],
        EventTypeListCommand: () => [baseEventType("created", "et-001", "ft-001", "User created")],
      })

      const provisioner = new PathwayProvisioner({
        tenant: "my-org",
        dataCore: "my-core",
        apiKey: "fc_test_key",
        // dataCoreDescription NOT provided
        dataCoreAccessControl: "private",
        dataCoreDeleteProtection: false,
        registrations: [
          {
            flowType: "user",
            eventType: "created",
            flowTypeDescription: "User events",
            eventTypeDescription: "User created",
          },
        ],
        clientFactory: () => client,
      })

      await provisioner.provision()

      // DataCoreUpdateCommand should NOT be called
      assertEquals(commands.includes("DataCoreUpdateCommand"), false)
    })

    await t.step("creates multiple flow types and event types", async () => {
      const createdFlowTypes: string[] = []
      const createdEventTypes: string[] = []

      const client = createMockClient({
        TenantTranslateNameToIdCommand: () => baseTenant(),
        DataCoreFetchCommand: () => baseDataCore(),
        FlowTypeListCommand: () => [],
        FlowTypeCreateCommand: (cmd) => {
          const name = cmd.input.name as string
          createdFlowTypes.push(name)
          return baseFlowType(name, `ft-${name}`, cmd.input.description as string)
        },
        EventTypeListCommand: () => [],
        EventTypeCreateCommand: (cmd) => {
          const name = cmd.input.name as string
          createdEventTypes.push(name)
          return baseEventType(name, `et-${name}`, cmd.input.flowTypeId as string, cmd.input.description as string)
        },
      })

      const provisioner = new PathwayProvisioner({
        tenant: "my-org",
        dataCore: "my-core",
        apiKey: "fc_test_key",
        dataCoreAccessControl: "private",
        dataCoreDeleteProtection: false,
        registrations: [
          {
            flowType: "user",
            eventType: "created",
            flowTypeDescription: "User events",
            eventTypeDescription: "User created",
          },
          {
            flowType: "user",
            eventType: "deleted",
            flowTypeDescription: "User events",
            eventTypeDescription: "User deleted",
          },
          {
            flowType: "order",
            eventType: "placed",
            flowTypeDescription: "Order events",
            eventTypeDescription: "Order placed",
          },
        ],
        clientFactory: () => client,
      })

      await provisioner.provision()

      assertEquals(createdFlowTypes.sort(), ["order", "user"])
      assertEquals(createdEventTypes.sort(), ["created", "deleted", "placed"])
    })
  },
})
