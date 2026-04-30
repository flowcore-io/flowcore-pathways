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

export type ProvisionFailureMode = "throw" | "continue"

export interface ProvisionFailureConfig {
  /**
   * How to handle unexpected lookup/list failures. These are treated as
   * possible Flowcore outages by default, logged, and skipped so startup can continue.
   */
  check?: ProvisionFailureMode
  /**
   * How to handle expected provisioning failures after a real not-found path
   * or create/update failures. Defaults to fail loud.
   */
  apply?: ProvisionFailureMode
}

type ResolvedProvisionFailureConfig = Required<ProvisionFailureConfig>

const DEFAULT_PROVISION_FAILURE_CONFIG: ResolvedProvisionFailureConfig = {
  check: "continue",
  apply: "throw",
}

function resolveProvisionFailureConfig(
  config?: ProvisionFailureMode | ProvisionFailureConfig,
): ResolvedProvisionFailureConfig {
  if (typeof config === "string") {
    return { check: config, apply: config }
  }

  return {
    check: config?.check ?? DEFAULT_PROVISION_FAILURE_CONFIG.check,
    apply: config?.apply ?? DEFAULT_PROVISION_FAILURE_CONFIG.apply,
  }
}

function isNotFoundError(error: unknown): boolean {
  if (error instanceof NotFoundException) {
    return true
  }

  if (typeof error === "object" && error !== null) {
    const maybeStatus = (error as { status?: unknown }).status
    if (maybeStatus === 404) {
      return true
    }

    const maybeResponseStatus = (error as { response?: { status?: unknown } }).response?.status
    if (maybeResponseStatus === 404) {
      return true
    }
  }

  return error instanceof Error && /\bnot found\b/i.test(error.message)
}

function errorMeta(error: unknown): Record<string, unknown> {
  if (error instanceof Error) {
    return {
      error: error.message,
      errorName: error.name,
    }
  }

  return { error: String(error) }
}

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
  /**
   * Skip data core create/update — still resolves the data core id via fetch so
   * downstream stages can run. Fails loudly if the data core doesn't exist and
   * no description was configured (same error as the non-skip path).
   */
  skipDataCore?: boolean
  /**
   * Skip flow type create/update. When event types are still provisioned, flow
   * type ids are resolved via `FlowTypeListCommand` (read-only). When event
   * types are skipped too, the flow type list fetch is skipped entirely.
   */
  skipFlowTypes?: boolean
  /** Skip the event type loop entirely (no list/fetch/create/update). */
  skipEventTypes?: boolean
  /**
   * Controls whether provisioning failures throw or are only logged.
   *
   * Defaults:
   * - `check`: `"continue"` for unexpected lookup/list failures, treated as possible outages.
   * - `apply`: `"throw"` for missing resources without descriptions and create/update failures.
   *
   * Passing `"throw"` or `"continue"` applies that mode to both categories.
   */
  provisionFailure?: ProvisionFailureMode | ProvisionFailureConfig
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
  private readonly skipDataCore: boolean
  private readonly skipFlowTypes: boolean
  private readonly skipEventTypes: boolean
  private readonly provisionFailure: ResolvedProvisionFailureConfig

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
    this.skipDataCore = options.skipDataCore ?? false
    this.skipFlowTypes = options.skipFlowTypes ?? false
    this.skipEventTypes = options.skipEventTypes ?? false
    this.provisionFailure = resolveProvisionFailureConfig(options.provisionFailure)
  }

  /**
   * Run the provisioning flow:
   * 1. Fetch tenant → tenantId
   * 2. Fetch or create data core (read-only when `skipDataCore` is set)
   * 3. For each unique flow type: fetch or create (read-only when `skipFlowTypes`)
   * 4. For each event type: fetch or create (entirely skipped when `skipEventTypes`)
   * 5. Update descriptions where they differ (unless skipped)
   */
  async provision(): Promise<void> {
    const client = this.clientFactory(this.apiKey)

    this.logger.info("Starting provisioning", {
      tenant: this.tenant,
      dataCore: this.dataCore,
      skipDataCore: this.skipDataCore,
      skipFlowTypes: this.skipFlowTypes,
      skipEventTypes: this.skipEventTypes,
    })

    // Step 1: Fetch tenant
    const tenant = await this.check(
      "tenant",
      { tenant: this.tenant },
      () => client.execute(new TenantTranslateNameToIdCommand({ tenant: this.tenant })),
    )
    if (!tenant) {
      this.logger.info("Provisioning skipped after tenant lookup failed")
      return
    }
    const tenantId = tenant.id
    this.logger.info("Tenant resolved", { tenantId })

    // Step 2: Provision (or resolve) data core
    const dataCoreId = await this.provisionDataCore(client, tenantId)
    if (!dataCoreId) {
      this.logger.info("Provisioning skipped after data core lookup failed")
      return
    }
    this.logger.info("Data core resolved", { dataCoreId })

    // Short-circuit when both flow-type and event-type stages are skipped —
    // no need to list flow types in that case.
    if (this.skipFlowTypes && this.skipEventTypes) {
      this.logger.info("Provisioning complete (flow types + event types skipped)")
      return
    }

    // Step 3: Provision (or resolve) flow types
    const flowTypeIds = await this.provisionFlowTypes(client, dataCoreId)
    if (!flowTypeIds) {
      this.logger.info("Provisioning skipped after flow type lookup failed")
      return
    }
    this.logger.info("Flow types resolved", { count: flowTypeIds.size })

    if (this.skipEventTypes) {
      this.logger.info("Provisioning complete (event types skipped)")
      return
    }

    // Step 4: Provision event types
    await this.provisionEventTypes(client, flowTypeIds)
    this.logger.info("Provisioning complete")
  }

  private async provisionDataCore(client: FlowcoreClient, tenantId: string): Promise<string | null> {
    let dataCore: { id: string; description: string } | null = null

    try {
      dataCore = await client.execute(
        new DataCoreFetchCommand({ tenant: this.tenant, dataCore: this.dataCore }),
      )
    } catch (error) {
      if (!isNotFoundError(error)) {
        this.handleCheckFailure("dataCore", error, {
          tenant: this.tenant,
          dataCore: this.dataCore,
        })
        return null
      }
    }

    if (dataCore) {
      // Data core exists — update description if provided and changed (unless skipping).
      if (
        !this.skipDataCore &&
        this.dataCoreDescription !== undefined &&
        dataCore.description !== this.dataCoreDescription
      ) {
        this.logger.info("Updating data core description", {
          dataCoreId: dataCore.id,
          from: dataCore.description,
          to: this.dataCoreDescription,
        })
        const updated = await this.apply(
          "dataCore.update",
          {
            dataCoreId: dataCore.id,
            dataCore: this.dataCore,
          },
          () =>
            client.execute(
              new DataCoreUpdateCommand({ dataCoreId: dataCore.id, description: this.dataCoreDescription }),
            ),
        )
        if (!updated) {
          return dataCore.id
        }
      }
      return dataCore.id
    }

    // Data core doesn't exist
    if (this.dataCoreDescription === undefined) {
      this.handleApplyFailure(
        "dataCore.missing",
        new Error(
          `Data core "${this.dataCore}" not found on tenant "${this.tenant}". ` +
            `Provide dataCoreDescription in the PathwaysBuilder constructor to auto-create it.`,
        ),
        { tenant: this.tenant, dataCore: this.dataCore },
      )
      return null
    }

    if (this.skipDataCore) {
      // Data core missing but create/update was skipped — can't resolve an id.
      this.handleApplyFailure(
        "dataCore.skipped",
        new Error(
          `Data core "${this.dataCore}" not found and skipDataCore is set. ` +
            `Pre-provision the data core or enable data core provisioning.`,
        ),
        { tenant: this.tenant, dataCore: this.dataCore },
      )
      return null
    }

    const dataCoreDescription = this.dataCoreDescription
    this.logger.info("Creating data core", { name: this.dataCore })
    const created = await this.apply(
      "dataCore.create",
      { tenantId, dataCore: this.dataCore },
      () =>
        client.execute(
          new DataCoreCreateCommand({
            tenantId,
            name: this.dataCore,
            description: dataCoreDescription,
            accessControl: this.dataCoreAccessControl as "public" | "private",
            deleteProtection: this.dataCoreDeleteProtection,
          }),
        ),
    )

    return created?.id ?? null
  }

  private async provisionFlowTypes(
    client: FlowcoreClient,
    dataCoreId: string,
  ): Promise<Map<string, string> | null> {
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
    const existing = await this.check(
      "flowType.list",
      { dataCoreId },
      () => client.execute(new FlowTypeListCommand({ dataCoreId })),
      { onNotFound: () => [] },
    )
    if (!existing) {
      return null
    }
    const existingByName = new Map(existing.map((ft) => [ft.name, ft]))

    const flowTypeIds = new Map<string, string>()

    for (const [name, description] of flowTypes) {
      const existingFt = existingByName.get(name)

      if (existingFt) {
        flowTypeIds.set(name, existingFt.id)

        // Update description if provided and changed (unless skipping).
        if (!this.skipFlowTypes && description !== undefined && existingFt.description !== description) {
          this.logger.info("Updating flow type description", {
            flowType: name,
            from: existingFt.description,
            to: description,
          })
          await this.apply(
            "flowType.update",
            { dataCoreId, flowType: name, flowTypeId: existingFt.id },
            () => client.execute(new FlowTypeUpdateCommand({ flowTypeId: existingFt.id, description })),
          )
        }
      } else if (!this.skipFlowTypes && description !== undefined) {
        // Create flow type
        this.logger.info("Creating flow type", { name, description })
        const created = await this.apply(
          "flowType.create",
          { dataCoreId, flowType: name },
          () => client.execute(new FlowTypeCreateCommand({ dataCoreId, name, description })),
        )
        if (created) {
          flowTypeIds.set(name, created.id)
        }
      } else if (this.skipFlowTypes) {
        // Flow type missing, create/update skipped — downstream event type stage cannot proceed.
        this.handleApplyFailure(
          "flowType.skipped",
          new Error(
            `Flow type "${name}" not found in data core and skipFlowTypes is set. ` +
              `Pre-provision the flow type or enable flow type provisioning.`,
          ),
          { dataCoreId, flowType: name },
        )
      } else {
        this.handleApplyFailure(
          "flowType.missing",
          new Error(
            `Flow type "${name}" not found in data core. ` +
              `Provide flowTypeDescription in the register() call to auto-create it.`,
          ),
          { dataCoreId, flowType: name },
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
      const existing = await this.check(
        "eventType.list",
        { flowType: flowTypeName, flowTypeId },
        () => client.execute(new EventTypeListCommand({ flowTypeId })),
        { onNotFound: () => [] },
      )
      if (!existing) {
        continue
      }
      const existingByName = new Map(existing.map((et) => [et.name, et]))

      for (const reg of regs) {
        const existingEt = existingByName.get(reg.eventType)

        if (existingEt) {
          // Update description if provided and changed
          if (reg.eventTypeDescription !== undefined && existingEt.description !== reg.eventTypeDescription) {
            const eventTypeDescription = reg.eventTypeDescription
            this.logger.info("Updating event type description", {
              flowType: flowTypeName,
              eventType: reg.eventType,
              from: existingEt.description,
              to: eventTypeDescription,
            })
            await this.apply(
              "eventType.update",
              { flowType: flowTypeName, eventType: reg.eventType, eventTypeId: existingEt.id },
              () =>
                client.execute(
                  new EventTypeUpdateCommand({ eventTypeId: existingEt.id, description: eventTypeDescription }),
                ),
            )
          }
        } else if (reg.eventTypeDescription !== undefined) {
          const eventTypeDescription = reg.eventTypeDescription
          // Create event type
          this.logger.info("Creating event type", {
            flowType: flowTypeName,
            eventType: reg.eventType,
            description: eventTypeDescription,
          })
          await this.apply(
            "eventType.create",
            { flowType: flowTypeName, flowTypeId, eventType: reg.eventType },
            () =>
              client.execute(
                new EventTypeCreateCommand({
                  flowTypeId,
                  name: reg.eventType,
                  description: eventTypeDescription,
                }),
              ),
          )
        } else {
          this.handleApplyFailure(
            "eventType.missing",
            new Error(
              `Event type "${reg.eventType}" not found in flow type "${flowTypeName}". ` +
                `Provide description in the register() call to auto-create it.`,
            ),
            { flowType: flowTypeName, eventType: reg.eventType },
          )
        }
      }
    }
  }

  private async check<T>(
    stage: string,
    context: Record<string, unknown>,
    operation: () => Promise<T>,
    options?: { onNotFound?: () => T },
  ): Promise<T | null> {
    try {
      return await operation()
    } catch (error) {
      if (isNotFoundError(error)) {
        if (options?.onNotFound) {
          return options.onNotFound()
        }
        throw error
      }

      this.handleCheckFailure(stage, error, context)
      return null
    }
  }

  private async apply<T>(
    stage: string,
    context: Record<string, unknown>,
    operation: () => Promise<T>,
  ): Promise<T | null> {
    try {
      return await operation()
    } catch (error) {
      this.handleApplyFailure(stage, error, context)
      return null
    }
  }

  private handleCheckFailure(stage: string, error: unknown, context: Record<string, unknown>): void {
    this.logFailure("Provisioning check failed; treating as possible Flowcore outage", stage, error, context)

    if (this.provisionFailure.check === "throw") {
      throw error
    }
  }

  private handleApplyFailure(stage: string, error: unknown, context: Record<string, unknown>): void {
    this.logFailure("Provisioning failed", stage, error, context)

    if (this.provisionFailure.apply === "throw") {
      throw error
    }
  }

  private logFailure(
    message: string,
    stage: string,
    error: unknown,
    context: Record<string, unknown>,
  ): void {
    const meta = {
      ...context,
      ...errorMeta(error),
      stage,
      tenant: this.tenant,
      dataCore: this.dataCore,
    }

    if (error instanceof Error) {
      this.logger.error(message, error, meta)
    } else {
      this.logger.error(message, meta)
    }
  }
}
