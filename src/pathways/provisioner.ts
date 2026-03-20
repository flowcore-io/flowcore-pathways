import {
  DataCoreCreateCommand,
  DataCoreFetchCommand,
  DataCoreUpdateCommand,
  EventTypeCreateCommand,
  EventTypeListCommand,
  EventTypeUpdateCommand,
  FlowcoreClient,
  FlowTypeCreateCommand,
  FlowTypeListCommand,
  FlowTypeUpdateCommand,
  NotFoundException,
  TenantTranslateNameToIdCommand,
} from "@flowcore/sdk"
import type { Logger } from "./logger.ts"
import { NoopLogger } from "./logger.ts"

/**
 * Registration metadata for a single pathway, used by the provisioner
 */
export interface ProvisionerRegistration {
  flowType: string
  eventType: string
  flowTypeDescription?: string
  eventTypeDescription?: string
}

/**
 * Options for creating a PathwayProvisioner
 */
export interface PathwayProvisionerOptions {
  tenant: string
  dataCore: string
  apiKey: string
  dataCoreDescription?: string
  dataCoreAccessControl: string
  dataCoreDeleteProtection: boolean
  registrations: ProvisionerRegistration[]
  logger?: Logger
  /** Override FlowcoreClient for testing */
  clientFactory?: (apiKey: string) => FlowcoreClient
}

/**
 * Provisions Flowcore infrastructure (data core, flow types, event types).
 * Creates missing resources when descriptions are provided, updates descriptions
 * when they differ. Fails if a resource is missing and no description is provided.
 * Additive only — never deletes.
 */
export class PathwayProvisioner {
  private readonly tenant: string
  private readonly dataCore: string
  private readonly apiKey: string
  private readonly dataCoreDescription?: string
  private readonly dataCoreAccessControl: string
  private readonly dataCoreDeleteProtection: boolean
  private readonly registrations: ProvisionerRegistration[]
  private readonly logger: Logger
  private readonly clientFactory: (apiKey: string) => FlowcoreClient

  constructor(options: PathwayProvisionerOptions) {
    this.tenant = options.tenant
    this.dataCore = options.dataCore
    this.apiKey = options.apiKey
    this.dataCoreDescription = options.dataCoreDescription
    this.dataCoreAccessControl = options.dataCoreAccessControl
    this.dataCoreDeleteProtection = options.dataCoreDeleteProtection
    this.registrations = options.registrations
    this.logger = options.logger ?? new NoopLogger()
    this.clientFactory = options.clientFactory ?? ((apiKey: string) => new FlowcoreClient({ apiKey }))
  }

  /**
   * Run the provisioning flow:
   * 1. Fetch tenant → tenantId
   * 2. Fetch or create data core
   * 3. For each unique flow type: fetch or create
   * 4. For each event type: fetch or create
   * 5. Update descriptions where they differ
   */
  async provision(): Promise<void> {
    const client = this.clientFactory(this.apiKey)

    this.logger.info("Starting provisioning", { tenant: this.tenant, dataCore: this.dataCore })

    // Step 1: Fetch tenant
    const tenant = await client.execute(new TenantTranslateNameToIdCommand({ tenant: this.tenant }))
    const tenantId = tenant.id
    this.logger.info("Tenant resolved", { tenantId })

    // Step 2: Provision data core
    const dataCoreId = await this.provisionDataCore(client, tenantId)
    this.logger.info("Data core resolved", { dataCoreId })

    // Step 3: Provision flow types
    const flowTypeIds = await this.provisionFlowTypes(client, dataCoreId)
    this.logger.info("Flow types resolved", { count: flowTypeIds.size })

    // Step 4: Provision event types
    await this.provisionEventTypes(client, flowTypeIds)
    this.logger.info("Provisioning complete")
  }

  private async provisionDataCore(client: FlowcoreClient, tenantId: string): Promise<string> {
    let dataCore: { id: string; description: string } | null = null

    try {
      dataCore = await client.execute(
        new DataCoreFetchCommand({ tenant: this.tenant, dataCore: this.dataCore }),
      )
    } catch (error) {
      if (!(error instanceof NotFoundException)) {
        throw error
      }
    }

    if (dataCore) {
      // Data core exists — update description if provided and changed
      if (this.dataCoreDescription !== undefined && dataCore.description !== this.dataCoreDescription) {
        this.logger.info("Updating data core description", {
          dataCoreId: dataCore.id,
          from: dataCore.description,
          to: this.dataCoreDescription,
        })
        await client.execute(
          new DataCoreUpdateCommand({ dataCoreId: dataCore.id, description: this.dataCoreDescription }),
        )
      }
      return dataCore.id
    }

    // Data core doesn't exist
    if (this.dataCoreDescription === undefined) {
      throw new Error(
        `Data core "${this.dataCore}" not found on tenant "${this.tenant}". ` +
          `Provide dataCoreDescription in the PathwaysBuilder constructor to auto-create it.`,
      )
    }

    this.logger.info("Creating data core", { name: this.dataCore })
    const created = await client.execute(
      new DataCoreCreateCommand({
        tenantId,
        name: this.dataCore,
        description: this.dataCoreDescription,
        accessControl: this.dataCoreAccessControl as "public" | "private",
        deleteProtection: this.dataCoreDeleteProtection,
      }),
    )
    return created.id
  }

  private async provisionFlowTypes(
    client: FlowcoreClient,
    dataCoreId: string,
  ): Promise<Map<string, string>> {
    // Collect unique flow types with their descriptions
    const flowTypes = new Map<string, string | undefined>()
    for (const reg of this.registrations) {
      // First registration with a description wins
      if (!flowTypes.has(reg.flowType)) {
        flowTypes.set(reg.flowType, reg.flowTypeDescription)
      } else if (reg.flowTypeDescription !== undefined && flowTypes.get(reg.flowType) === undefined) {
        flowTypes.set(reg.flowType, reg.flowTypeDescription)
      }
    }

    // Fetch existing flow types
    const existing = await client.execute(new FlowTypeListCommand({ dataCoreId }))
    const existingByName = new Map(existing.map((ft) => [ft.name, ft]))

    const flowTypeIds = new Map<string, string>()

    for (const [name, description] of flowTypes) {
      const existingFt = existingByName.get(name)

      if (existingFt) {
        flowTypeIds.set(name, existingFt.id)

        // Update description if provided and changed
        if (description !== undefined && existingFt.description !== description) {
          this.logger.info("Updating flow type description", {
            flowType: name,
            from: existingFt.description,
            to: description,
          })
          await client.execute(new FlowTypeUpdateCommand({ flowTypeId: existingFt.id, description }))
        }
      } else if (description !== undefined) {
        // Create flow type
        this.logger.info("Creating flow type", { name, description })
        const created = await client.execute(
          new FlowTypeCreateCommand({ dataCoreId, name, description }),
        )
        flowTypeIds.set(name, created.id)
      } else {
        throw new Error(
          `Flow type "${name}" not found in data core. ` +
            `Provide flowTypeDescription in the register() call to auto-create it.`,
        )
      }
    }

    return flowTypeIds
  }

  private async provisionEventTypes(
    client: FlowcoreClient,
    flowTypeIds: Map<string, string>,
  ): Promise<void> {
    // Group registrations by flow type
    const byFlowType = new Map<string, ProvisionerRegistration[]>()
    for (const reg of this.registrations) {
      const list = byFlowType.get(reg.flowType) ?? []
      list.push(reg)
      byFlowType.set(reg.flowType, list)
    }

    for (const [flowTypeName, regs] of byFlowType) {
      const flowTypeId = flowTypeIds.get(flowTypeName)
      if (!flowTypeId) continue

      // Fetch existing event types
      const existing = await client.execute(new EventTypeListCommand({ flowTypeId }))
      const existingByName = new Map(existing.map((et) => [et.name, et]))

      for (const reg of regs) {
        const existingEt = existingByName.get(reg.eventType)

        if (existingEt) {
          // Update description if provided and changed
          if (reg.eventTypeDescription !== undefined && existingEt.description !== reg.eventTypeDescription) {
            this.logger.info("Updating event type description", {
              flowType: flowTypeName,
              eventType: reg.eventType,
              from: existingEt.description,
              to: reg.eventTypeDescription,
            })
            await client.execute(
              new EventTypeUpdateCommand({ eventTypeId: existingEt.id, description: reg.eventTypeDescription }),
            )
          }
        } else if (reg.eventTypeDescription !== undefined) {
          // Create event type
          this.logger.info("Creating event type", {
            flowType: flowTypeName,
            eventType: reg.eventType,
            description: reg.eventTypeDescription,
          })
          await client.execute(
            new EventTypeCreateCommand({ flowTypeId, name: reg.eventType, description: reg.eventTypeDescription }),
          )
        } else {
          throw new Error(
            `Event type "${reg.eventType}" not found in flow type "${flowTypeName}". ` +
              `Provide description in the register() call to auto-create it.`,
          )
        }
      }
    }
  }
}
