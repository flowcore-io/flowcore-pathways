# Flowcore Pathways

A TypeScript Library for creating Flowcore Pathways, simplifying the integration with the Flowcore platform. Flowcore
Pathways helps you build event-driven applications with type-safe pathways for processing and producing events.

## Table of Contents

- [Installation](#installation)
- [Getting Started](#getting-started)
- [Core Concepts](#core-concepts)
- [Usage](#usage)
  - [Creating a Pathways Builder](#creating-a-pathways-builder)
  - [Runtime Defaults and Auto-Provisioning](#runtime-defaults-and-auto-provisioning)
  - [Pump Concurrency](#pump-concurrency)
  - [Notification Delivery and Write Timeouts](#notification-delivery-and-write-timeouts)
  - [Registering Pathways](#registering-pathways)
  - [Handling Events](#handling-events)
  - [Writing Events](#writing-events)
  - [Error Handling](#error-handling)
  - [Event Observability](#event-observability)
  - [Setting up a Router](#setting-up-a-router)
  - [HTTP Server Integration](#http-server-integration)
  - [Persistence Options](#persistence-options)
  - [State Prefix (databases shared by several deployables)](#state-prefix-databases-shared-by-several-deployables)
- [Advanced Usage](#advanced-usage)
  - [Auditing](#auditing)
  - [Custom Loggers](#custom-loggers)
  - [Retry Mechanisms](#retry-mechanisms)
  - [Large Events (Automatic Chunking)](#large-events-automatic-chunking)
  - [Session Pathways](#session-pathways)
- [File Pathways](#file-pathways)
- [API Reference](#api-reference)

## Installation

```bash
# Bun
bunx jsr add @flowcore/pathways

# Deno
deno add jsr:@flowcore/pathways

# npm / yarn
npx jsr add @flowcore/pathways
```

or using npm:

```bash
npm install @flowcore/pathways
```

or using yarn:

```bash
yarn add @flowcore/pathways
```

## Getting Started

Here's a basic example to get you started with Flowcore Pathways:

```typescript
import { z } from "zod"
import { PathwaysBuilder } from "@flowcore/pathways"

// Define your event schema
const userSchema = z.object({
  id: z.string(),
  name: z.string(),
  email: z.string(),
})

// Create a pathways builder
const pathways = new PathwaysBuilder({
  baseUrl: "https://api.flowcore.io",
  tenant: "your-tenant",
  dataCore: "your-data-core",
  apiKey: "your-api-key",
})

// Register a pathway
pathways
  .register({
    flowType: "user",
    eventType: "created",
    schema: userSchema,
  })
  .handle("user/created", async (event) => {
    console.log(`Processing user created event: ${event.eventId}`)
    console.log(`User data:`, event.payload)

    // Process the event...

    // You can write to another pathway if needed
    await pathways.write("notifications/sent", {
      data: {
        userId: event.payload.id,
        message: `Welcome ${event.payload.name}!`,
        channel: "email",
      },
    })
  })
```

## Core Concepts

Flowcore Pathways is built around these core concepts:

- **PathwaysBuilder**: The main entry point for creating and managing pathways
- **Pathways**: Define event flows with schemas for type safety
- **Handlers**: Process incoming events
- **Writers**: Send events to pathways
- **Router**: Direct incoming events to the appropriate pathway
- **Persistence**: Store pathway state for reliable processing

## Usage

### Creating a Pathways Builder

The `PathwaysBuilder` is the main configuration point for your pathways:

```typescript
import { PathwaysBuilder } from "@flowcore/pathways"

const pathways = new PathwaysBuilder({
  baseUrl: "https://api.flowcore.io",
  tenant: "your-tenant",
  dataCore: "your-data-core",
  apiKey: "your-api-key",
  pathwayTimeoutMs: 10000, // Optional, default is 10000 (10s)
  logger: customLogger, // Optional, defaults to NoopLogger
})
```

### Runtime Defaults and Auto-Provisioning

`PathwaysBuilder` drives different startup behavior based on `runtimeEnv` (auto-detected from `NODE_ENV` when omitted):

| `runtimeEnv`  | `pathwayMode` default | Shared resources | Pathway registration                   | Local pump                           |
| ------------- | --------------------- | ---------------- | -------------------------------------- | ------------------------------------ |
| `production`  | `managed`             | provisioned      | opt-in (`autoProvision.pathway: true`) | not started (control plane delivers) |
| `development` | `virtual`             | provisioned      | skipped                                | started (single instance)            |
| `test`        | `virtual`             | skipped          | skipped                                | started                              |

> **`development` never registers a pathway instance**, even with `autoProvision.pathway: true`. A pathway instance is a
> shared control-plane resource: every developer boot would otherwise create one, pulse it, and poll for restart
> commands meant for a real deployment. Set `allowDevelopmentPathwayRegistration: true` if you deliberately need one for
> local control-plane work — it logs a warning on every boot. With no pathway instance there is no `pathwayId`, so pulse
> and command polling stay off in development too.

> **Why `managed` in production?** Virtual cluster mode requires long-lived processes with stable networking, which
> breaks serverless runtimes such as Next.js on Vercel (port collisions, instrumentation hook behavior, non-leader pod
> timeouts). `managed` routes event delivery through the Flowcore control plane and is safe in every runtime.

#### Granular `autoProvision`

Pass an `AutoProvisionConfig` to turn individual provisioning stages on or off:

```typescript
import { PathwaysBuilder } from "@flowcore/pathways"

const pathways = new PathwaysBuilder({
  baseUrl: "https://api.flowcore.io",
  tenant: "your-tenant",
  dataCore: "your-data-core",
  apiKey: process.env.FLOWCORE_API_KEY!,
  runtimeEnv: "production",
  pathwayName: "orders-service",
  // pathwayMode defaults to "managed" in production
  autoProvision: {
    dataCore: true, // create/update the data core (default: true)
    flowType: true, // create/update registered flow types (default: true)
    eventType: true, // create/update registered event types (default: true)
    pathway: true, // upsert the by-name pathway instance (default: false)
  },
})
```

Omitted fields fall back to resources-on / pathway-off, so most deployments only need to set `pathway: true` when they
want the by-name pathway registration. `pathway: true` has no effect when `runtimeEnv` is `"development"` — see the
runtime table above.

Unexpected lookup/list failures during shared-resource provisioning are treated as possible Flowcore outages by default:
they are logged and startup continues. Real not-found responses still follow the provisioning path. If create/update
then fails, the default is to throw.

Override this with `provisionFailure`:

```typescript
const pathways = new PathwaysBuilder({
  /* ... */
  provisionFailure: {
    check: "continue", // lookup/list outage behavior: "continue" (default) or "throw"
    apply: "throw", // missing resource/create/update behavior: "throw" (default) or "continue"
  },
})
```

Passing `provisionFailure: "throw"` or `"continue"` applies the mode to both categories. The `apply` setting also
controls by-name virtual/managed pathway registration failures.

Independent flow-type and event-type operations run with bounded parallelism. Parent-child stages remain ordered so the
data core exists before flow types, and flow types exist before event types. The default limit is four concurrent
operations. Transient request failures retry three times in total with exponential backoff and jitter. Retryable
failures are HTTP 408, 429, 500, 502, 503, and 504, plus common network connection errors. A valid `Retry-After` is
honored without shortening it to `maxDelayMs` when response headers are available. The current Flowcore SDK does not
expose response headers for shared-resource commands, so those commands use exponential backoff. Ordinary 4xx responses,
including 404, are not retried.

Tune both behaviors when needed:

```typescript
const pathways = new PathwaysBuilder({
  /* ... */
  provisionConcurrency: 4,
  provisionRetry: {
    maxAttempts: 3, // includes the initial request
    baseDelayMs: 250,
    maxDelayMs: 5_000, // caps exponential backoff, not Retry-After
    jitterRatio: 0.2,
  },
})
```

Retries run before `provisionFailure` is applied. Once attempts are exhausted, the existing check/apply policy decides
whether startup throws or logs and continues.

To disable everything (for CI or when resources are managed elsewhere):

```typescript
const pathways = new PathwaysBuilder({
  /* ... */
  autoProvision: false, // or per-stage: { dataCore: false, flowType: false, eventType: false, pathway: false }
})
```

#### Managed production example

```typescript
const pathways = new PathwaysBuilder({
  baseUrl: "https://api.flowcore.io",
  tenant: "your-tenant",
  dataCore: "your-data-core",
  apiKey: process.env.FLOWCORE_API_KEY!,
  runtimeEnv: "production",
  pathwayName: "orders-service",
  autoProvision: { pathway: true }, // register the managed pathway instance
  managedConfig: {
    endpointUrl: "https://app.example.com/api/flowcore",
    authHeaders: { authorization: `Bearer ${process.env.TRANSFORM_TOKEN!}` },
    sizeClass: "medium",
  },
})
```

#### Deprecated: `defaultAutoProvision`

`defaultAutoProvision: boolean` still works but is deprecated — prefer `autoProvision`. Mapping:

- `true` → `{ dataCore: true, flowType: true, eventType: true, pathway: false }`
- `false` → `{ dataCore: false, flowType: false, eventType: false, pathway: false }`

### Pump Concurrency

Control how many events each pump processes in parallel via `startPump({ concurrency })`. Accepts a number (shared
default) or a `PumpConcurrencyConfig` with per-flow-type and per-pump-group overrides:

```typescript
// Shared default across every pump
await pathways.startPump({ concurrency: 4 })

// Per-flow-type overrides — unlisted flow types fall back to `default` (or 1)
await pathways.startPump({
  concurrency: {
    default: 2,
    byFlowType: {
      orders: 8,
      audit: 1,
    },
    // Optional: per-(flowType, pumpGroup) override. Wins over byFlowType.
    // Key format: `${flowType}::${pumpGroup}`.
    byPumpGroup: {
      "orders::hot": 16,
    },
  },
})
```

Omit `concurrency` to keep the default of 1 per pump. `startPump()` also accepts a per-call `autoProvision` override
(same shape as the builder-level option) for overriding provisioning behavior at a specific call site.

> **Note**: this resolves to `processor.concurrency` on the underlying data pump, which is the in-flight batch width —
> not parallel handler invocations. Resolution order per pump: `byPumpGroup["${flowType}::${pumpGroup}"]` →
> `byFlowType[flowType]` → `default`.

### Splitting a flow type across multiple pumps

By default, every event type registered against the same `flowType` shares one pump. For high-throughput event types
that would otherwise starve their cold neighbours, register them with a distinct `pumpGroup` so they run on an isolated
pump with their own state cursor, processor concurrency, and restart backoff:

```typescript
// 8 cold event types share the default pump for orders.0
for (const eventType of ["paid", "fulfilled", "cancelled", "refunded", "archived", "audited", "tagged", "noted"]) {
  pathways.register({ flowType: "orders.0", eventType, schema })
}

// 2 hot event types run on a separate "hot" pump — same flow type, isolated pump
pathways.register({ flowType: "orders.0", eventType: "placed.0", schema, pumpGroup: "hot" })
pathways.register({ flowType: "orders.0", eventType: "shipped.0", schema, pumpGroup: "hot" })

await pathways.startPump({
  concurrency: { default: 2, byPumpGroup: { "orders.0::hot": 16 } },
})
```

What this gives you per pump group:

- **Isolated state cursor.** The Postgres `pathway_pump_state` table now has a composite primary key
  `(flow_type, pump_group)`; existing rows are auto-migrated into `pump_group='default'` on first use.
- **Independent processor concurrency** via `byPumpGroup`.
- **Independent restart backoff.** A failure in the `hot` pump does not reset the cold pump's attempt counter, and the
  restart loop keeps retrying with exponential backoff (capped at 30s) until the pump comes back — including when the
  restart attempt itself throws synchronously.
- **Pulse health.** When pulse reporting is configured, every group emits pulses with the resolved pathway UUID. The
  Data Pathways CP pulse route (`POST /api/v1/pump-pulse`) validates `pathwayId` as a strict UUID, so each pulse goes
  through unchanged regardless of pump group. Per-group health visibility (distinguishing `hot` from `default` for the
  same flowType in the CP) is not yet wired and will land as an additive `pumpGroup` field through the SDK + CP.

**Caveats (v2.4):**

- The WebSocket notifier subscribes at `flowType` scope, so two pump groups on the same flow type receive identical
  notifications and each pulls. Isolation is at processor + state, not bandwidth — checks are cheap, so this is usually
  fine. If you need bandwidth isolation, use distinct flow types instead.
- Cluster mode keeps a single global leader; per-pump-group leadership is not yet supported.
- Downstream consumers that mirror `pathway_pump_state` in their own Drizzle schema MUST add a
  `pump_group TEXT NOT NULL DEFAULT 'default'` column and update the primary key to `(flow_type, pump_group)` before
  running `drizzle-kit push`, otherwise drizzle will try to drop the new column.

Custom `PumpStateManagerFactory` implementations should accept a second `pumpGroup` argument:

```typescript
const factory: PumpStateManagerFactory = (flowType, pumpGroup) => createMyStateManager(flowType, pumpGroup)
```

Legacy single-argument factories continue to work but share state across pump groups on the same flow type — a
deprecation warning is logged once per pump.

### Notification Delivery and Write Timeouts

A non-`fireAndForget` `write()` returns only once the local pump has processed the event. The pump learns that an event
exists from an `event.stored.*` notification, so the write's latency is bounded by notification delivery — not by the
write itself.

**The write and the wait are separate concerns.** When `write()` throws a timeout, the event has already been stored
durably; only the acknowledgement is missing. Retrying such a write duplicates the event. If a notification is dropped
in transit, the pump's own safety re-poll recovers it — but that re-poll is currently `20000ms`, which is longer than
the default `pathwayTimeoutMs` of `10000ms`. Any dropped notification therefore surfaces as a timeout on a write that
actually succeeded.

**On request paths, use `fireAndForget`:**

```typescript
// An HTTP handler should not block on downstream processing.
await pathways.write("order/placed", {
  data: orderData,
  options: { fireAndForget: true },
})
```

Reach for the blocking form when the caller genuinely needs read-your-writes behaviour — a test, a migration, a CLI step
— and raise `pathwayTimeoutMs` above the re-poll interval if a dropped notification must not fail the call:

```typescript
const pathways = new PathwaysBuilder({
  // ...other config
  pathwayTimeoutMs: 25000, // above the 20s notifier re-poll
})
```

#### Choosing a notifier

`startPump({ notifier })` selects how the pump is told that new events exist. It defaults to `websocket`:

```typescript
// Default — long-lived websocket subscription, lowest latency.
await pathways.startPump({ notifier: { type: "websocket" } })

// Polling — no notification dependency at all. Useful for low-volume workloads with long
// idle windows, where a websocket can flap without surfacing an error.
await pathways.startPump({ notifier: { type: "poller", pollerIntervalMs: 60000 } })

// NATS — subscribes to stored-event subjects directly, bypassing websocket fan-out.
await pathways.startPump({ notifier: { type: "nats", natsServers: ["nats://nats:4222"] } })
```

> **Upgrade warning (2.5.4).** Before 2.5.4 the `poller` and `nats` options were accepted but silently ignored: the
> emitted configuration omitted the discriminator the data pump reads, so **every pump ran `websocket`** no matter what
> was configured. From 2.5.4 you get the notifier you asked for. If you configured one of these, treat the upgrade as a
> behaviour change, not a patch:
>
> - **`nats`** — the pump now really connects to your NATS servers. Confirm they are reachable from the workload before
>   upgrading; a failing connection puts the pump into its restart backoff loop.
> - **`poller`** — the pump now really polls. Note that `@flowcore/data-pump` currently waits
>   `Math.min(pollerIntervalMs, 1000)`, so any interval above one second still polls every second. Budget for the
>   request volume, and prefer `websocket` or `nats` if you chose polling to reduce load.

### Registering Pathways

Register pathways with their schemas for type-safe event handling:

```typescript
import { z } from "zod"

// Define your event schema
const orderSchema = z.object({
  orderId: z.string(),
  userId: z.string(),
  total: z.number(),
  items: z.array(
    z.Object({
      id: z.string(),
      quantity: z.number(),
    }),
  ),
})

// Register pathway
pathways.register({
  flowType: "order",
  eventType: "placed",
  schema: orderSchema,
  writable: true, // Optional, default is true
  maxRetries: 3, // Optional, default is 3
  retryDelayMs: 500, // Optional, default is 500
  // Optional: isolate this event type onto a dedicated pump for the same flow type.
  // Same (flowType, pumpGroup) pair shares one pump; different pumpGroup → different pumps.
  // Omit (or pass "default") for the legacy single-pump-per-flowType behavior.
  pumpGroup: "hot",
})
```

### Handling Events

Set up handlers to process events for specific pathways:

```typescript
const pathwayKey = "order/placed"

pathways.handle(pathwayKey, async (event) => {
  console.log(`Processing order ${event.payload.orderId}`)

  // Access typed payload data
  const { userId, total, items } = event.payload

  // Your business logic here
  await updateInventory(items)
  await notifyUser(userId, total)
})
```

### Writing Events

Send events to pathways:

```typescript
// Basic write
const eventId = await pathways.write("order/placed", {
  data: {
    orderId: "ord-123",
    userId: "user-456",
    total: 99.99,
    items: [
      { id: "item-1", quantity: 2 },
    ],
  },
})

// Write with metadata
const eventId2 = await pathways.write("order/placed", {
  data: orderData,
  metadata: {
    correlationId: "corr-789",
    source: "checkout-service",
  },
})

// Fire-and-forget mode (doesn't wait for processing).
// Prefer this on request paths — see "Notification Delivery and Write Timeouts".
const eventId3 = await pathways.write("order/placed", {
  data: orderData,
  options: {
    fireAndForget: true,
  },
})

// Batch write multiple events
const eventIds = await pathways.write("order/placed", {
  batch: true,
  data: [orderData1, orderData2, orderData3],
})

// Batch write with metadata
const eventIds2 = await pathways.write("order/placed", {
  batch: true,
  data: [orderData1, orderData2],
  metadata: {
    source: "bulk-import",
  },
})
```

### Error Handling

Handle errors in pathway processing:

```typescript
// Error handler for a specific pathway
pathways.onError("order/placed", (error, event) => {
  console.error(`Error processing order ${event.payload.orderId}:`, error)
  reportToMonitoring(error, event)
})

// Global error handler for all pathways
pathways.onAnyError((error, event, pathway) => {
  console.error(`Error in pathway ${pathway}:`, error)
  reportToMonitoring(error, event, pathway)
})
```

### Event Observability

Subscribe to events for observability at different stages:

```typescript
// Before processing
pathways.subscribe("order/placed", (event) => {
  console.log(`About to process order ${event.payload.orderId}`)
}, "before")

// After processing
pathways.subscribe("order/placed", (event) => {
  console.log(`Finished processing order ${event.payload.orderId}`)
}, "after")

// At both stages
pathways.subscribe("order/placed", (event) => {
  console.log(`Event ${event.eventId} at ${new Date().toISOString()}`)
}, "all")
```

### Setting up a Router

The `PathwayRouter` routes incoming events to the appropriate pathway:

```typescript
import { PathwayRouter } from "@flowcore/pathways"

// Create a router with a secret key for validation
const WEBHOOK_SECRET = "your-webhook-secret"
const router = new PathwayRouter(pathways, WEBHOOK_SECRET)

// Process an incoming event from a webhook
async function handleWebhook(req: Request) {
  const event = await req.json()
  const secret = req.headers.get("X-Webhook-Secret")

  try {
    // This validates the secret and routes to the right pathway
    await router.processEvent(event, secret)
    return new Response("Event processed", { status: 200 })
  } catch (error) {
    console.error("Error processing event:", error)
    return new Response("Error processing event", { status: 500 })
  }
}
```

### HTTP Server Integration

Integrate with Deno's HTTP server:

```typescript
import { serve } from "https://deno.land/std/http/server.ts"

serve(async (req: Request) => {
  const url = new URL(req.url)

  if (req.method === "POST" && url.pathname === "/webhook") {
    return handleWebhook(req)
  }

  return new Response("Not found", { status: 404 })
}, { port: 3000 })
```

### Persistence Options

Flowcore Pathways supports different persistence options to track processed events and ensure exactly-once processing.

#### Default In-Memory KV Store (Development)

By default, Flowcore Pathways uses an internal in-memory KV store for persistence:

```typescript
// The default persistence is used automatically, no explicit setup required
const pathways = new PathwaysBuilder({
  baseUrl: "https://api.flowcore.io",
  tenant: "your-tenant",
  dataCore: "your-data-core",
  apiKey: "your-api-key",
})
```

The internal store uses the appropriate KV adapter for your environment (Bun, Node, or Deno), but note that this state
is not persistent across application restarts and should be used primarily for development.

#### PostgreSQL Persistence (Production)

For production environments, you can use PostgreSQL for reliable and scalable persistence:

```typescript
import { createPostgresPathwayState, PostgresPathwayState } from "@flowcore/pathways"

// Create a PostgreSQL state handler
const postgresState = createPostgresPathwayState({
  host: "localhost",
  port: 5432,
  user: "postgres",
  password: "postgres",
  database: "pathway_db",
  tableName: "pathway_state", // Optional, defaults to "pathway_state"
  ttlMs: 300000, // Optional, defaults to 5 minutes (300000ms)
  ssl: false, // Optional, defaults to false
})

// Use PostgreSQL for pathway state
pathways.withPathwayState(postgresState)
```

The PostgreSQL implementation:

- Automatically creates the necessary table if it doesn't exist
- Includes TTL-based automatic cleanup of processed events
- Creates appropriate indexes for performance

### State Prefix (databases shared by several deployables)

**Set `statePrefix` when two or more deployables share ONE PostgreSQL connection string.** Without it they contend for a
single cluster leader lease. Only one of them starts a data pump. The others log
`Could not acquire lease, becoming worker` and their projections stall silently — writes still succeed, so the failure
is invisible at the API layer.

`statePrefix` namespaces every table and key this library owns:

| State               | No prefix (default)      | `statePrefix: "compute_api"`         |
| ------------------- | ------------------------ | ------------------------------------ |
| Pathway state table | `pathway_state`          | `compute_api_pathway_state`          |
| Lease table         | `pathway_leases`         | `compute_api_pathway_leases`         |
| Instance table      | `pathway_instances`      | `compute_api_pathway_instances`      |
| Pump state table    | `pathway_pump_state`     | `compute_api_pathway_pump_state`     |
| Chunk table         | `pathway_chunks`         | `compute_api_pathway_chunks`         |
| Leader lease key    | `pathway-cluster-leader` | `compute_api_pathway-cluster-leader` |

**The default is no prefix.** Existing deployments keep their exact table names and lease key value. There is no
migration.

```typescript
import {
  createPostgresPathwayCoordinator,
  createPostgresPathwayState,
  createPostgresPumpStateManagerFactory,
} from "@flowcore/pathways"

const STATE_PREFIX = "compute_api" // "compute_reconciler" in the sibling deployable

const coordinator = await createPostgresPathwayCoordinator(
  { connectionString: process.env.DATABASE_URL! },
  { statePrefix: STATE_PREFIX },
)

pathways.withPathwayState(
  createPostgresPathwayState({
    connectionString: process.env.DATABASE_URL!,
    statePrefix: STATE_PREFIX,
  }),
)

// The cluster reads the coordinator's lease key, so the prefix is set in one place only.
await pathways.startCluster({
  coordinator,
  advertisedAddress: `ws://${process.env.HOSTNAME}`,
  port: 9090,
})

await pathways.startPump({
  stateManagerFactory: await createPostgresPumpStateManagerFactory({
    connectionString: process.env.DATABASE_URL!,
    statePrefix: STATE_PREFIX,
  }),
})
```

Rules:

- A prefix must start with a letter or underscore and contain only letters, digits and underscores. It reaches SQL as
  part of an identifier, so anything else is rejected. Maximum length is 40 characters.
- An explicit `tableName`, `leasesTable`, `instancesTable` or `leaseKey` always overrides the prefix.
- `ClusterManager` resolves its lease key from `leaseKey`, then `statePrefix`, then `coordinator.leaseKey`, then the
  default. Setting the prefix on the coordinator alone is enough.
- Use the same prefix for every state library inside one deployable. Use a different prefix in each deployable.
- If you mirror these tables in Drizzle, update your schema and your `tablesFilter` to the prefixed names.
- `ClusterManager` logs its `leaseKey` on start and on every role change. Two deployables that report the same key are
  contending.

## Advanced Usage

### Auditing

Enable auditing to track events:

```typescript
// Set up auditing
pathways
  .withAudit((path, event) => {
    console.log(`Audit: ${path} event ${event.eventId}`)
    logToAuditSystem(path, event)
  })
  .withUserResolver(async () => {
    // Get the current user ID from context
    return {
      entityId: "user-123",
      entityType: "user",
    }
  })
```

### Custom Loggers

Create a custom logger:

```typescript
import { Logger } from "@flowcore/pathways"

class MyCustomLogger implements Logger {
  debug(message: string, context?: Record<string, unknown>): void {
    console.debug(`[DEBUG] ${message}`, context)
  }

  info(message: string, context?: Record<string, unknown>): void {
    console.info(`[INFO] ${message}`, context)
  }

  warn(message: string, context?: Record<string, unknown>): void {
    console.warn(`[WARN] ${message}`, context)
  }

  error(message: string, error?: Error, context?: Record<string, unknown>): void {
    console.error(`[ERROR] ${message}`, error, context)
  }
}

// Use custom logger
const pathways = new PathwaysBuilder({
  // ...other config
  logger: new MyCustomLogger(),
})
```

### Retry Mechanisms

Configure retry behavior for pathways:

```typescript
// Global timeout for pathway processing
const pathways = new PathwaysBuilder({
  // ...other config
  pathwayTimeoutMs: 15000, // 15 seconds
})

// Per-pathway retry configuration
pathways.register({
  flowType: "payment",
  eventType: "process",
  schema: paymentSchema,
  maxRetries: 5, // Retry up to 5 times
  retryDelayMs: 1000, // 1 second between retries
})
```

### Large Events (Automatic Chunking)

Flowcore rejects a single event whose payload is larger than 64 000 bytes
(`400 Event size exceeds maximum limit of 64000 bytes`). Pathways can split and reassemble such events for you.
**Chunking is off until you configure a chunk store with `withPathwayChunkStore()`.** Without a store, an oversized
write fails with the Flowcore error as before, and a consumer that receives a part event throws.

- `write()` measures the payload as it will go on the wire (after encryption, when the pathway is encrypted). A payload
  over the cap is split on UTF-8 boundaries into part events, each well under the cap, and sent in one batch request.
- `process()` collects the parts in a **chunk store**. The call that receives the last missing part reassembles the
  payload, verifies its SHA-256 digest, and continues with the normal path: schema validation, audit, cluster routing,
  and your handler. Your handler sees one event with the full payload. The other parts never reach it.
- A logical event keeps the event id of part 1. `write()` returns that id, and a write without `fireAndForget` waits
  until every part is processed.
- Batch writes work the same way. Only oversized items are expanded, and the returned array still has one id per input
  item.
- Encrypted pathways stay encrypted. Every slice is encrypted on its own. The chunk header (id, part number, part count,
  digest) is plaintext, the payload data is not.
- File pathways are not chunked here. The Flowcore file endpoint splits files on the server.

#### Chunk store

Configuring a chunk store turns chunking on for both writes and processing. `InternalPathwayChunkStore` is in memory and
only works for a single process. Any deployment with more than one instance must use the PostgreSQL store, so a part
received by one instance can be joined with parts received by another:

```typescript
import { createPostgresPathwayChunkStore, createPostgresPathwayState } from "@flowcore/pathways"

const pathways = new PathwaysBuilder({
  baseUrl: "https://webhook.api.flowcore.io",
  tenant: "your-tenant",
  dataCore: "your-data-core",
  apiKey: "your-api-key",
  // Optional budgets. Defaults shown.
  chunking: {
    maxEventBytes: 64_000, // split when the wire payload is larger than this
    partBudgetBytes: 45_000, // maximum serialized size of one part
  },
})
  .withPathwayState(createPostgresPathwayState({ connectionString }))
  .withPathwayChunkStore(
    createPostgresPathwayChunkStore({
      connectionString,
      statePrefix: "my_service", // optional, see State Prefix
      ttlMs: 60 * 60 * 1000, // optional, parts of an incomplete chunk expire after 1 hour
    }),
  )
```

The PostgreSQL store creates the `pathway_chunks` table on first use. Parts are collected under a per-chunk advisory
lock, so exactly one instance reassembles a given event. An exact replay of a part is accepted. A part with the same
number but different bytes is rejected as a conflict.

#### Wire format

Every part is a normal Flowcore event on the same flow type and event type, with the metadata marker
`pathways/chunked: "true"` and this payload:

```json
{
  "pathwaysChunk": {
    "id": "8b6d1b0c-4a7e-4a6f-9d84-1f7a2b3c4d5e",
    "part": 2,
    "totalParts": 3,
    "digest": "<sha256 hex of the full plaintext JSON>",
    "scheme": "utf8-split-sha256-v1"
  },
  "data": "<slice of the JSON text, or its AES-256-GCM ciphertext on encrypted pathways>"
}
```

Consumers that read the raw events outside Pathways must join the `data` slices in `part` order. Consumers that run
Pathways get the original event.

#### Limits

- Chunking requires a chunk store. `chunking.enabled: false` keeps it off even when a store is configured.
- Every consumer of a chunked pathway must run this library version with a chunk store. An older consumer, or one
  without a store, cannot reassemble the parts.
- The `eventTime` and `validTime` write options apply to every part of a batch. A key-based override (`eventTimeKey`)
  cannot be resolved for a chunked or encrypted item, because the server reads the key from the wire payload, which does
  not carry your fields.
- Parts of a chunk that never completes stay in the store until `ttlMs` expires, then they are removed on the next
  write.

### Session Pathways

The `SessionPathwayBuilder` provides a way to associate session IDs with pathway operations, making it easier to track
and manage user sessions in your application.

#### Setting Up Session Support

To use session-specific functionality, first configure your `PathwaysBuilder` with session support:

```typescript
import { PathwaysBuilder } from "@flowcore/pathways"

// Configure the builder with session support
const pathways = new PathwaysBuilder({
  baseUrl: "https://api.flowcore.io",
  tenant: "your-tenant",
  dataCore: "your-data-core",
  apiKey: "your-api-key",
  enableSessionUserResolvers: true, // Enable session-specific resolvers
})
```

#### Creating Session Pathways

Create a session-specific pathway wrapper:

```typescript
import { SessionPathwayBuilder } from "@flowcore/pathways"

// Create a session with an auto-generated session ID
const session = new SessionPathwayBuilder(pathways)
const sessionId = session.getSessionId() // Get the auto-generated ID

// Or create a session with a specific session ID
const customSession = new SessionPathwayBuilder(pathways, "user-session-123")
```

#### Session-Specific User Resolvers

You can register different user resolvers for different sessions, allowing you to associate users with specific
sessions:

```typescript
// Register a user resolver for a specific session
pathways.withSessionUserResolver("user-session-123", async () => {
  // Return the user ID for this session
  return {
    entityId: "user-456",
    entityType: "user",
  }
})

// Alternative: Register directly through the session instance
session.withUserResolver(async () => {
  return {
    entityId: "key-789",
    entityType: "key",
  }
})
```

#### Writing Events with Session Context

Events written through a session builder automatically include the session ID:

```typescript
// Write an event with session context
await session.write("order/placed", {
  data: {
    orderId: "ord-123",
    userId: "user-456",
    total: 99.99,
    items: [{ id: "item-1", quantity: 2 }],
  },
})

// You can override the session ID for a specific write
await session.write("order/placed", {
  data: orderData,
  options: { sessionId: "different-session" },
})

// Batch write events with session context
await session.write("user/actions", {
  batch: true,
  data: [actionData1, actionData2, actionData3],
})
```

#### Session ID in Audit Events

When auditing is enabled, the session ID is included in the audit metadata:

```typescript
// Enable auditing
pathways.withAudit((path, event) => {
  console.log(`Audit: ${path} event ${event.eventId}`)
  // The session ID will be included in event metadata
})

// Now when writing events through a session
await session.write("order/placed", { data: orderData })
// The session ID is automatically included in the audit metadata
```

### File Pathways

File pathways provide a specialized way to handle file uploads and processing in your Flowcore applications. They
automatically handle file type detection, binary content processing, and provide a structured approach to file
management.

#### Registering File Pathways

Register a file pathway by setting the `isFilePathway` flag to `true`:

```typescript
import { z } from "zod"

// Define additional properties schema for your file
const documentSchema = z.object({
  documentType: z.enum(["invoice", "receipt", "contract"]),
  department: z.string(),
  metadata: z.record(z.string()).optional(),
})

// Register a file pathway
pathways.register({
  flowType: "document",
  eventType: "uploaded",
  schema: documentSchema, // Additional properties beyond the file itself
  isFilePathway: true, // This marks it as a file pathway
  writable: true,
})
```

#### Writing Files to Pathways

File pathways use a special input format that includes file content and metadata:

```typescript
import { readFile } from "node:fs/promises"

// Read file content (as Buffer for Node.js/Bun, Uint8Array for Deno)
const fileContent = await readFile("./invoice.pdf")

// Write a file to a pathway
const eventId = await pathways.write("document/uploaded", {
  data: {
    fileId: "file-123", // Unique identifier for the file
    fileName: "invoice-2024.pdf", // Original filename
    fileContent: fileContent, // File content as Buffer/Uint8Array
    // Additional properties defined in your schema
    documentType: "invoice",
    department: "finance",
    metadata: {
      customer: "ACME Corp",
      amount: "1500.00",
    },
  },
})
```

#### File Input Schema

File pathways automatically include these required fields:

```typescript
// Built-in file fields (automatically added)
interface FileInput {
  fileId: string // Unique identifier for the file
  fileName: string // Original filename with extension
  fileContent: Buffer | Uint8Array // Binary file content
  // ... your additional schema properties
}
```

#### File Event Schema

When processed, file events include automatic file type detection:

```typescript
// Built-in file event fields (automatically added to your schema)
interface FileEvent {
  fileId: string // Unique identifier for the file
  fileName: string // Original filename
  fileType: string // MIME type (automatically detected)
  fileContent: Blob // File content as Blob
  // ... your additional schema properties
}
```

#### Handling File Events

Handle file events just like regular events, but with access to file-specific properties:

```typescript
pathways.handle("document/uploaded", async (event) => {
  const { fileId, fileName, fileType, fileContent, documentType, department } = event.payload

  console.log(`Processing file: ${fileName} (${fileType})`)
  console.log(`Document type: ${documentType}, Department: ${department}`)

  // Process the file content
  if (fileType === "application/pdf") {
    await processPDFDocument(fileContent, event.payload.metadata)
  } else if (fileType.startsWith("image/")) {
    await processImageFile(fileContent, documentType)
  }

  // Store file metadata
  await storeFileMetadata({
    fileId,
    fileName,
    fileType,
    documentType,
    department,
    processedAt: new Date(),
  })
})
```

#### File Pathway Limitations

File pathways have some specific limitations:

```typescript
// ❌ Batch writes are NOT supported for file pathways
// This will throw an error:
await pathways.write("document/uploaded", {
  batch: true, // Error: Batch is not possible for file pathways
  data: [fileData1, fileData2],
})

// ✅ Write files individually instead:
for (const fileData of fileDataArray) {
  await pathways.write("document/uploaded", { data: fileData })
}
```

#### Complete File Pathway Example

Here's a complete example of setting up and using file pathways:

```typescript
import { PathwaysBuilder } from "@flowcore/pathways"
import { z } from "zod"
import { readFile } from "node:fs/promises"

// Define schema for additional file properties
const documentSchema = z.object({
  documentType: z.enum(["invoice", "receipt", "contract", "report"]),
  department: z.string(),
  tags: z.array(z.string()).optional(),
  metadata: z.record(z.string()).optional(),
})

const pathways = new PathwaysBuilder({
  baseUrl: "https://api.flowcore.io",
  tenant: "your-tenant",
  dataCore: "your-data-core",
  apiKey: "your-api-key",
})

// Register file pathway
pathways
  .register({
    flowType: "document",
    eventType: "uploaded",
    schema: documentSchema,
    isFilePathway: true,
  })
  .handle("document/uploaded", async (event) => {
    const { fileId, fileName, fileType, documentType, department } = event.payload

    console.log(`Processing ${documentType} from ${department}: ${fileName}`)

    // File type-specific processing
    switch (fileType) {
      case "application/pdf":
        await extractPDFText(event.payload.fileContent)
        break
      case "image/jpeg":
      case "image/png":
        await extractImageMetadata(event.payload.fileContent)
        break
      default:
        console.log(`Unsupported file type: ${fileType}`)
    }

    // Trigger downstream processing
    await pathways.write("document/processed", {
      data: {
        fileId,
        fileName,
        documentType,
        department,
        processedAt: new Date().toISOString(),
        status: "completed",
      },
    })
  })

// Upload a file
async function uploadDocument(filePath: string, documentType: string, department: string) {
  const fileContent = await readFile(filePath)
  const fileName = filePath.split("/").pop() || "unknown"

  return await pathways.write("document/uploaded", {
    data: {
      fileId: `doc-${Date.now()}`,
      fileName,
      fileContent,
      documentType,
      department,
      tags: ["automated-upload"],
      metadata: {
        uploadedAt: new Date().toISOString(),
        source: "api",
      },
    },
  })
}

// Usage
await uploadDocument("./invoice.pdf", "invoice", "finance")
```

## API Reference

For a complete API reference, please see the [API documentation](https://jsr.io/@flowcore/pathways).
