# `metadataWebhook` Function Summary

This code defines a higher-order function called `metadataWebhook` that enhances webhook calls with predefined metadata.
Here's a breakdown of what it does and how it works:

## Conceptual Overview

The `metadataWebhook` function creates a wrapper around webhook functions that automatically injects predefined metadata
into every webhook call, while still allowing for additional metadata to be provided on a per-call basis.

## Type Parameters and Function Structure

1. **Type Parameters**:
   - `TMetadata`: The type of metadata (defaults to `Record<string, unknown>`)
   - `TData`: The type of the payload data (inferred from usage)
   - `TReturnType`: The return type of the webhook function (either a single string ID or an array of string IDs)

2. **Function Structure**:
   - It's a curried function that takes metadata first, then returns a function that takes a webhook and its parameters
   - This allows for creating reusable webhook wrappers with specific metadata presets

## Function Parameters

1. **First call** - `metadataWebhook(metadata)`:
   - `metadata`: A record of key-value pairs to be included in all webhook calls

2. **Second call** - `metadataWebhook(metadata)(webhook, payload, additionalMetadata)`:
   - `webhook`: The original webhook function to be called
   - `payload`: The data to be sent via the webhook
   - `additionalMetadata`: Optional extra metadata for this specific call

## Behavior

1. The function merges the predefined metadata with any additional metadata provided for the specific call
2. Priority is given to the predefined metadata (it will override any duplicate keys in additionalMetadata)
3. It calls the original webhook function with the payload and merged metadata
4. It preserves the return type of the original webhook function

## Usage Example (Conceptual)

```typescript
// Create a webhook with predefined metadata
const loggedWebhook = metadataWebhook({
  source: "backend-service",
  version: "1.0.0",
})

// Use the enhanced webhook
await loggedWebhook(
  userWebhook.send,
  { userId: "123", action: "login" },
  { correlationId: "abc-123" },
)
// This would call userWebhook.send with the payload and merged metadata:
// { correlationId: "abc-123", source: "backend-service", version: "1.0.0" }
```

This pattern is useful for ensuring consistent metadata across multiple webhook calls, such as adding tracking
information, source identifiers, or version data to all outgoing events.
