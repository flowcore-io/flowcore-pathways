import { assertEquals, assertRejects } from "https://deno.land/std@0.224.0/assert/mod.ts"
import { PathwayProvisioner } from "../src/pathways/provisioner.ts"
import type { FlowcoreClient } from "@flowcore/sdk"
import { NotFoundException } from "@flowcore/sdk"
import type { Logger } from "../src/pathways/logger.ts"

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

function createTestLogger(errors: Array<{ message: string; stage?: unknown }>): Logger {
  return {
    debug() {},
    info() {},
    warn() {},
    error(messageOrError, errorOrContext, context) {
      const message = messageOrError instanceof Error ? messageOrError.message : messageOrError
      const meta = context ?? (errorOrContext instanceof Error ? undefined : errorOrContext)
      errors.push({ message, stage: meta?.stage })
    },
  }
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

    await t.step("continues when data core check fails unexpectedly", async () => {
      const commands: string[] = []
      const errors: Array<{ message: string; stage?: unknown }> = []

      const client = createMockClient({
        TenantTranslateNameToIdCommand: () => baseTenant(),
        DataCoreFetchCommand: () => {
          commands.push("DataCoreFetchCommand")
          throw new Error("service unavailable")
        },
        DataCoreCreateCommand: () => {
          commands.push("DataCoreCreateCommand")
          return baseDataCore()
        },
        FlowTypeListCommand: () => {
          commands.push("FlowTypeListCommand")
          return []
        },
      })

      const provisioner = new PathwayProvisioner({
        tenant: "my-org",
        dataCore: "my-core",
        apiKey: "fc_test_key",
        dataCoreDescription: "desc",
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
        logger: createTestLogger(errors),
        clientFactory: () => client,
      })

      await provisioner.provision()

      assertEquals(commands, ["DataCoreFetchCommand"])
      assertEquals(errors, [
        {
          message: "Provisioning check failed; treating as possible Flowcore outage",
          stage: "dataCore",
        },
      ])
    })

    await t.step("can throw when data core check fails unexpectedly", async () => {
      const client = createMockClient({
        TenantTranslateNameToIdCommand: () => baseTenant(),
        DataCoreFetchCommand: () => {
          throw new Error("service unavailable")
        },
      })

      const provisioner = new PathwayProvisioner({
        tenant: "my-org",
        dataCore: "my-core",
        apiKey: "fc_test_key",
        dataCoreDescription: "desc",
        dataCoreAccessControl: "private",
        dataCoreDeleteProtection: false,
        registrations: [],
        provisionFailure: { check: "throw" },
        clientFactory: () => client,
      })

      await assertRejects(
        () => provisioner.provision(),
        Error,
        "service unavailable",
      )
    })

    await t.step("not found data core still attempts create and create failure throws by default", async () => {
      const commands: string[] = []

      const client = createMockClient({
        TenantTranslateNameToIdCommand: () => baseTenant(),
        DataCoreFetchCommand: () => {
          commands.push("DataCoreFetchCommand")
          throw new NotFoundException("DataCore", {})
        },
        DataCoreCreateCommand: () => {
          commands.push("DataCoreCreateCommand")
          throw new Error("create failed")
        },
      })

      const provisioner = new PathwayProvisioner({
        tenant: "my-org",
        dataCore: "my-core",
        apiKey: "fc_test_key",
        dataCoreDescription: "desc",
        dataCoreAccessControl: "private",
        dataCoreDeleteProtection: false,
        registrations: [],
        clientFactory: () => client,
      })

      await assertRejects(
        () => provisioner.provision(),
        Error,
        "create failed",
      )
      assertEquals(commands, ["DataCoreFetchCommand", "DataCoreCreateCommand"])
    })

    await t.step("can log and continue when create fails after not found", async () => {
      const errors: Array<{ message: string; stage?: unknown }> = []

      const client = createMockClient({
        TenantTranslateNameToIdCommand: () => baseTenant(),
        DataCoreFetchCommand: () => {
          throw new NotFoundException("DataCore", {})
        },
        DataCoreCreateCommand: () => {
          throw new Error("create failed")
        },
      })

      const provisioner = new PathwayProvisioner({
        tenant: "my-org",
        dataCore: "my-core",
        apiKey: "fc_test_key",
        dataCoreDescription: "desc",
        dataCoreAccessControl: "private",
        dataCoreDeleteProtection: false,
        registrations: [],
        provisionFailure: { apply: "continue" },
        logger: createTestLogger(errors),
        clientFactory: () => client,
      })

      await provisioner.provision()

      assertEquals(errors, [
        {
          message: "Provisioning failed",
          stage: "dataCore.create",
        },
      ])
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

    await t.step("continues when flow type list check fails unexpectedly", async () => {
      const commands: string[] = []
      const errors: Array<{ message: string; stage?: unknown }> = []

      const client = createMockClient({
        TenantTranslateNameToIdCommand: () => baseTenant(),
        DataCoreFetchCommand: () => baseDataCore({ description: "desc" }),
        FlowTypeListCommand: () => {
          commands.push("FlowTypeListCommand")
          throw new Error("service unavailable")
        },
        EventTypeListCommand: () => {
          commands.push("EventTypeListCommand")
          return []
        },
      })

      const provisioner = new PathwayProvisioner({
        tenant: "my-org",
        dataCore: "my-core",
        apiKey: "fc_test_key",
        dataCoreDescription: "desc",
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
        logger: createTestLogger(errors),
        clientFactory: () => client,
      })

      await provisioner.provision()

      assertEquals(commands, ["FlowTypeListCommand"])
      assertEquals(errors, [
        {
          message: "Provisioning check failed; treating as possible Flowcore outage",
          stage: "flowType.list",
        },
      ])
    })

    await t.step("treats flow type list 404 as empty and provisions missing flow types", async () => {
      const commands: string[] = []

      const client = createMockClient({
        TenantTranslateNameToIdCommand: () => baseTenant(),
        DataCoreFetchCommand: () => baseDataCore({ description: "desc" }),
        FlowTypeListCommand: () => {
          commands.push("FlowTypeListCommand")
          throw { status: 404, message: "not found" }
        },
        FlowTypeCreateCommand: (cmd) => {
          commands.push("FlowTypeCreateCommand")
          return baseFlowType(cmd.input.name as string, "ft-new", cmd.input.description as string)
        },
        EventTypeListCommand: () => [baseEventType("created", "et-001", "ft-new", "User created")],
      })

      const provisioner = new PathwayProvisioner({
        tenant: "my-org",
        dataCore: "my-core",
        apiKey: "fc_test_key",
        dataCoreDescription: "desc",
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

      assertEquals(commands, ["FlowTypeListCommand", "FlowTypeCreateCommand"])
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

    await t.step("treats event type list 404 as empty and provisions missing event types", async () => {
      const commands: string[] = []

      const client = createMockClient({
        TenantTranslateNameToIdCommand: () => baseTenant(),
        DataCoreFetchCommand: () => baseDataCore({ description: "desc" }),
        FlowTypeListCommand: () => [baseFlowType("user", "ft-001")],
        EventTypeListCommand: () => {
          commands.push("EventTypeListCommand")
          throw { status: 404, message: "not found" }
        },
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
        dataCoreDescription: "desc",
        dataCoreAccessControl: "private",
        dataCoreDeleteProtection: false,
        registrations: [
          {
            flowType: "user",
            eventType: "created",
            eventTypeDescription: "User created",
          },
        ],
        clientFactory: () => client,
      })

      await provisioner.provision()

      assertEquals(commands, ["EventTypeListCommand", "EventTypeCreateCommand"])
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

    await t.step("skipDataCore: resolves id but does not update description", async () => {
      const commands: string[] = []

      const client = createMockClient({
        TenantTranslateNameToIdCommand: () => baseTenant(),
        DataCoreFetchCommand: () => {
          commands.push("DataCoreFetchCommand")
          return baseDataCore({ description: "Old desc" })
        },
        DataCoreUpdateCommand: () => {
          commands.push("DataCoreUpdateCommand")
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
        skipDataCore: true,
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

      assertEquals(commands.includes("DataCoreFetchCommand"), true)
      assertEquals(commands.includes("DataCoreUpdateCommand"), false)
    })

    await t.step("skipDataCore: fails loudly when data core missing and no description", async () => {
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
        skipDataCore: true,
        registrations: [],
        clientFactory: () => client,
      })

      await assertRejects(
        () => provisioner.provision(),
        Error,
        'Data core "my-core" not found',
      )
    })

    await t.step("skipDataCore: fails when data core missing even with description", async () => {
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
        dataCoreDescription: "desc",
        dataCoreAccessControl: "private",
        dataCoreDeleteProtection: false,
        skipDataCore: true,
        registrations: [],
        clientFactory: () => client,
      })

      await assertRejects(
        () => provisioner.provision(),
        Error,
        "skipDataCore is set",
      )
    })

    await t.step("skipFlowTypes: resolves ids via list but does not create/update", async () => {
      const commands: string[] = []

      const client = createMockClient({
        TenantTranslateNameToIdCommand: () => baseTenant(),
        DataCoreFetchCommand: () => baseDataCore(),
        FlowTypeListCommand: () => {
          commands.push("FlowTypeListCommand")
          return [baseFlowType("user", "ft-001", "Old flow desc")]
        },
        FlowTypeUpdateCommand: () => {
          commands.push("FlowTypeUpdateCommand")
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
        skipFlowTypes: true,
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

      assertEquals(commands.includes("FlowTypeListCommand"), true)
      assertEquals(commands.includes("FlowTypeUpdateCommand"), false)
    })

    await t.step("skipFlowTypes: fails when flow type missing", async () => {
      const client = createMockClient({
        TenantTranslateNameToIdCommand: () => baseTenant(),
        DataCoreFetchCommand: () => baseDataCore(),
        FlowTypeListCommand: () => [],
      })

      const provisioner = new PathwayProvisioner({
        tenant: "my-org",
        dataCore: "my-core",
        apiKey: "fc_test_key",
        dataCoreAccessControl: "private",
        dataCoreDeleteProtection: false,
        skipFlowTypes: true,
        registrations: [
          {
            flowType: "user",
            eventType: "created",
            flowTypeDescription: "User events",
          },
        ],
        clientFactory: () => client,
      })

      await assertRejects(
        () => provisioner.provision(),
        Error,
        "skipFlowTypes is set",
      )
    })

    await t.step("skipEventTypes: skips the event type loop entirely", async () => {
      const commands: string[] = []

      const client = createMockClient({
        TenantTranslateNameToIdCommand: () => baseTenant(),
        DataCoreFetchCommand: () => baseDataCore(),
        FlowTypeListCommand: () => [baseFlowType("user", "ft-001", "User events")],
        EventTypeListCommand: () => {
          commands.push("EventTypeListCommand")
          return []
        },
        EventTypeCreateCommand: () => {
          commands.push("EventTypeCreateCommand")
          return baseEventType("created", "et-new", "ft-001", "User created")
        },
      })

      const provisioner = new PathwayProvisioner({
        tenant: "my-org",
        dataCore: "my-core",
        apiKey: "fc_test_key",
        dataCoreAccessControl: "private",
        dataCoreDeleteProtection: false,
        skipEventTypes: true,
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

      assertEquals(commands.includes("EventTypeListCommand"), false)
      assertEquals(commands.includes("EventTypeCreateCommand"), false)
    })

    await t.step("skipFlowTypes + skipEventTypes: short-circuits after data core", async () => {
      const commands: string[] = []

      const client = createMockClient({
        TenantTranslateNameToIdCommand: () => baseTenant(),
        DataCoreFetchCommand: () => {
          commands.push("DataCoreFetchCommand")
          return baseDataCore()
        },
        FlowTypeListCommand: () => {
          commands.push("FlowTypeListCommand")
          return []
        },
      })

      const provisioner = new PathwayProvisioner({
        tenant: "my-org",
        dataCore: "my-core",
        apiKey: "fc_test_key",
        dataCoreAccessControl: "private",
        dataCoreDeleteProtection: false,
        skipFlowTypes: true,
        skipEventTypes: true,
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

      assertEquals(commands.includes("DataCoreFetchCommand"), true)
      assertEquals(commands.includes("FlowTypeListCommand"), false)
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

    await t.step("provisions sibling resources in bounded parallel stages", async () => {
      let activeFlowCreates = 0
      let maxFlowCreates = 0
      let completedFlowCreates = 0
      let activeEventLists = 0
      let maxEventLists = 0
      let activeEventCreates = 0
      let maxEventCreates = 0

      const client = createMockClient({
        TenantTranslateNameToIdCommand: () => baseTenant(),
        DataCoreFetchCommand: () => baseDataCore(),
        FlowTypeListCommand: () => [],
        FlowTypeCreateCommand: async (cmd) => {
          activeFlowCreates++
          maxFlowCreates = Math.max(maxFlowCreates, activeFlowCreates)
          await new Promise((resolve) => setTimeout(resolve, 5))
          activeFlowCreates--
          completedFlowCreates++
          const name = cmd.input.name as string
          return baseFlowType(name, `ft-${name}`, cmd.input.description as string)
        },
        EventTypeListCommand: async () => {
          assertEquals(completedFlowCreates, 3, "event-type stage must wait for all flow types")
          activeEventLists++
          maxEventLists = Math.max(maxEventLists, activeEventLists)
          await new Promise((resolve) => setTimeout(resolve, 5))
          activeEventLists--
          return []
        },
        EventTypeCreateCommand: async (cmd) => {
          activeEventCreates++
          maxEventCreates = Math.max(maxEventCreates, activeEventCreates)
          await new Promise((resolve) => setTimeout(resolve, 5))
          activeEventCreates--
          return baseEventType(
            cmd.input.name as string,
            `et-${cmd.input.name as string}`,
            cmd.input.flowTypeId as string,
            cmd.input.description as string,
          )
        },
      })

      const provisioner = new PathwayProvisioner({
        tenant: "my-org",
        dataCore: "my-core",
        apiKey: "fc_test_key",
        dataCoreAccessControl: "private",
        dataCoreDeleteProtection: false,
        provisionConcurrency: 2,
        registrations: ["user", "order", "invoice"].map((flowType) => ({
          flowType,
          eventType: "created",
          flowTypeDescription: `${flowType} events`,
          eventTypeDescription: `${flowType} created`,
        })),
        clientFactory: () => client,
      })

      await provisioner.provision()

      assertEquals(maxFlowCreates, 2)
      assertEquals(maxEventLists, 2)
      assertEquals(maxEventCreates, 2)
    })

    await t.step("retries transient SDK apply failures before applying failure policy", async () => {
      let createAttempts = 0
      const client = createMockClient({
        TenantTranslateNameToIdCommand: () => baseTenant(),
        DataCoreFetchCommand: () => baseDataCore(),
        FlowTypeListCommand: () => [],
        FlowTypeFetchCommand: () => {
          throw new NotFoundException("FlowType", {})
        },
        FlowTypeCreateCommand: (cmd) => {
          createAttempts++
          if (createAttempts < 3) {
            throw Object.assign(new Error("service unavailable"), { status: 500 })
          }
          return baseFlowType(cmd.input.name as string, "ft-new", cmd.input.description as string)
        },
      })

      const provisioner = new PathwayProvisioner({
        tenant: "my-org",
        dataCore: "my-core",
        apiKey: "fc_test_key",
        dataCoreAccessControl: "private",
        dataCoreDeleteProtection: false,
        skipEventTypes: true,
        provisionRetry: { maxAttempts: 3, baseDelayMs: 0, maxDelayMs: 0 },
        registrations: [{
          flowType: "user",
          eventType: "created",
          flowTypeDescription: "User events",
        }],
        clientFactory: () => client,
      })

      await provisioner.provision()
      assertEquals(createAttempts, 3)
    })

    await t.step("applies check failure policy after transient retries are exhausted", async () => {
      let checkAttempts = 0
      const errors: Array<{ message: string; stage?: unknown }> = []
      const client = createMockClient({
        TenantTranslateNameToIdCommand: () => baseTenant(),
        DataCoreFetchCommand: () => {
          checkAttempts++
          throw Object.assign(new Error("service unavailable"), { status: 503 })
        },
      })

      const provisioner = new PathwayProvisioner({
        tenant: "my-org",
        dataCore: "my-core",
        apiKey: "fc_test_key",
        dataCoreAccessControl: "private",
        dataCoreDeleteProtection: false,
        provisionRetry: { maxAttempts: 2, baseDelayMs: 0, maxDelayMs: 0 },
        registrations: [],
        logger: createTestLogger(errors),
        clientFactory: () => client,
      })

      await provisioner.provision()
      assertEquals(checkAttempts, 2)
      assertEquals(errors.at(-1)?.stage, "dataCore")
    })

    await t.step("applies continue policy after transient apply retries are exhausted", async () => {
      let applyAttempts = 0
      const errors: Array<{ message: string; stage?: unknown }> = []
      const client = createMockClient({
        TenantTranslateNameToIdCommand: () => baseTenant(),
        DataCoreFetchCommand: () => {
          throw new NotFoundException("DataCore", {})
        },
        DataCoreCreateCommand: () => {
          applyAttempts++
          throw Object.assign(new Error("service unavailable"), { status: 500 })
        },
      })

      const provisioner = new PathwayProvisioner({
        tenant: "my-org",
        dataCore: "my-core",
        apiKey: "fc_test_key",
        dataCoreDescription: "desc",
        dataCoreAccessControl: "private",
        dataCoreDeleteProtection: false,
        provisionFailure: { apply: "continue" },
        provisionRetry: { maxAttempts: 2, baseDelayMs: 0, maxDelayMs: 0 },
        registrations: [],
        logger: createTestLogger(errors),
        clientFactory: () => client,
      })

      await provisioner.provision()
      assertEquals(applyAttempts, 2)
      assertEquals(errors.at(-1)?.stage, "dataCore.create")
    })

    await t.step("reconciles all create levels after an ambiguous server failure", async () => {
      let dataCoreExists = false
      let flowTypeExists = false
      let eventTypeExists = false
      let dataCoreCreates = 0
      let flowTypeCreates = 0
      let eventTypeCreates = 0

      const client = createMockClient({
        TenantTranslateNameToIdCommand: () => baseTenant(),
        DataCoreFetchCommand: () => {
          if (!dataCoreExists) throw new NotFoundException("DataCore", {})
          return baseDataCore({ description: "My data core" })
        },
        DataCoreCreateCommand: () => {
          dataCoreCreates++
          dataCoreExists = true
          throw Object.assign(new Error("response lost after commit"), { status: 500 })
        },
        FlowTypeListCommand: () => [],
        FlowTypeFetchCommand: () => {
          if (!flowTypeExists) throw new NotFoundException("FlowType", {})
          return baseFlowType("user", "ft-user", "User events")
        },
        FlowTypeCreateCommand: () => {
          flowTypeCreates++
          flowTypeExists = true
          throw Object.assign(new Error("response lost after commit"), { status: 500 })
        },
        EventTypeListCommand: () => [],
        EventTypeFetchCommand: () => {
          if (!eventTypeExists) throw new NotFoundException("EventType", {})
          return baseEventType("created", "et-created", "ft-user", "User created")
        },
        EventTypeCreateCommand: () => {
          eventTypeCreates++
          eventTypeExists = true
          throw Object.assign(new Error("response lost after commit"), { status: 500 })
        },
      })

      const provisioner = new PathwayProvisioner({
        tenant: "my-org",
        dataCore: "my-core",
        apiKey: "fc_test_key",
        dataCoreDescription: "My data core",
        dataCoreAccessControl: "private",
        dataCoreDeleteProtection: false,
        provisionRetry: { maxAttempts: 3, baseDelayMs: 0, maxDelayMs: 0 },
        registrations: [{
          flowType: "user",
          eventType: "created",
          flowTypeDescription: "User events",
          eventTypeDescription: "User created",
        }],
        clientFactory: () => client,
      })

      await provisioner.provision()
      assertEquals(dataCoreCreates, 1)
      assertEquals(flowTypeCreates, 1)
      assertEquals(eventTypeCreates, 1)
    })

    await t.step("reconciles conflict after an ambiguous create failure", async () => {
      let createAttempts = 0
      let resourceExists = false
      const client = createMockClient({
        TenantTranslateNameToIdCommand: () => baseTenant(),
        DataCoreFetchCommand: () => baseDataCore(),
        FlowTypeListCommand: () => [],
        FlowTypeFetchCommand: () => {
          if (!resourceExists) throw new NotFoundException("FlowType", {})
          return baseFlowType("user", "ft-user", "User events")
        },
        FlowTypeCreateCommand: () => {
          createAttempts++
          if (createAttempts === 1) {
            throw Object.assign(new Error("service unavailable"), { status: 500 })
          }
          resourceExists = true
          throw Object.assign(new Error("already exists"), { status: 409 })
        },
      })

      const provisioner = new PathwayProvisioner({
        tenant: "my-org",
        dataCore: "my-core",
        apiKey: "fc_test_key",
        dataCoreAccessControl: "private",
        dataCoreDeleteProtection: false,
        skipEventTypes: true,
        provisionRetry: { maxAttempts: 3, baseDelayMs: 0, maxDelayMs: 0 },
        registrations: [{
          flowType: "user",
          eventType: "created",
          flowTypeDescription: "User events",
        }],
        clientFactory: () => client,
      })

      await provisioner.provision()
      assertEquals(createAttempts, 2)
    })

    await t.step("deduplicates direct provisioner registrations", async () => {
      let eventTypeCreates = 0
      let createdDescription: unknown
      const client = createMockClient({
        TenantTranslateNameToIdCommand: () => baseTenant(),
        DataCoreFetchCommand: () => baseDataCore(),
        FlowTypeListCommand: () => [baseFlowType("user", "ft-user", "User events")],
        EventTypeListCommand: () => [],
        EventTypeCreateCommand: (cmd) => {
          eventTypeCreates++
          createdDescription = cmd.input.description
          return baseEventType("created", "et-created", "ft-user", String(cmd.input.description))
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
            eventTypeDescription: "First description",
          },
          {
            flowType: "user",
            eventType: "created",
            flowTypeDescription: "Conflicting flow description",
            eventTypeDescription: "Conflicting event description",
          },
        ],
        clientFactory: () => client,
      })

      await provisioner.provision()
      assertEquals(eventTypeCreates, 1)
      assertEquals(createdDescription, "First description")
    })
  },
})
