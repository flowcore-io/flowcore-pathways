# Flowcore Pathways

A TypeScript Library for creating Flowcore Pathways, simplifying the integration with the Flowcore platform. Flowcore
Pathways helps you build event-driven applications with type-safe pathways for processing and producing events.

## Table of Contents

- [Installation](#installation)
- [Getting Started](#getting-started)
- [Core Concepts](#core-concepts)
- [Usage](#usage)
  - [Creating a Pathways Builder](#creating-a-pathways-builder)
  - [Registering Pathways](#registering-pathways)
  - [Handling Events](#handling-events)
  - [Writing Events](#writing-events)
  - [Error Handling](#error-handling)
  - [Event Observability](#event-observability)
  - [Setting up a Router](#setting-up-a-router)
  - [HTTP Server Integration](#http-server-integration)
  - [Persistence Options](#persistence-options)
- [Advanced Usage](#advanced-usage)
  - [Auditing](#auditing)
  - [Custom Loggers](#custom-loggers)
  - [Retry Mechanisms](#retry-mechanisms)
  - [Session Pathways](#session-pathways)
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

// Fire-and-forget mode (doesn't wait for processing)
const eventId3 = await pathways.write("order/placed", {
  data: orderData,
  options: {
    fireAndForget: true,
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

## Advanced Usage

### Auditing

Enable auditing to track events:

```typescript
// Set up auditing
pathways.withAudit(
  // Audit handler
  (path, event) => {
    console.log(`Audit: ${path} event ${event.eventId}`)
    logToAuditSystem(path, event)
  },
  // User ID resolver
  async () => {
    // Get the current user ID from context
    return getCurrentUserId()
  },
)
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

## API Reference

For a complete API reference, please see the [API documentation](https://jsr.io/@flowcore/pathways).
