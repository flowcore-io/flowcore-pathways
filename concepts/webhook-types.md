# WebhookBuilder Types Summary

This code defines the type system and schema validation for the WebhookBuilder library. It uses the `@sinclair/typebox` package to create runtime type definitions that can be used for validation.

## Core Type Definitions

### Response Schemas

These schemas define the expected structure of API responses:

1. **Success Response Schemas**:
   - `WebhookSuccessResponseSchema`: Defines a successful single event response with an `eventId` field.
   - `WebhookBatchSuccessResponseSchema`: Defines a successful batch event response with an array of `eventIds`.
   - `WebhookFileSuccessResponseSchema`: Defines a successful file upload response with `checksum`, `hashType`, and `eventIds`.

2. **Error Response Schemas**:
   - `WebhookErrorResponseSchema`: Defines the standard error response with `error` and `message` fields, plus an optional `__localError` for client-side errors.
   - `WebhookError500ResponseSchema`: Defines a specific server error response with status code 500.

3. **Union Response Schemas**:
   - `WebhookResponseSchema`: Combines success and error schemas for single events.
   - `WebhookBatchResponseSchema`: Combines success and error schemas for batch events.
   - `WebhookFileResponseSchema`: Combines success and error schemas for file uploads.

### Configuration Interfaces

1. **`WebhookBuilderOptions`**: Core configuration for the webhook client:
   - `baseUrl`: Optional API base URL (defaults to "https://webhook.api.flowcore.io")
   - `tenant`: Tenant identifier
   - `dataCore`: Data core identifier
   - `apiKey`: API authentication key

2. **`WebhookRetryOptions`**: Configuration for retry behavior:
   - `maxAttempts`: Maximum number of retry attempts
   - `attemptDelayMs`: Delay between attempts (can be a fixed number or a function)

3. **`WebhookLocalTransformOptions`**: Configuration for local transformation:
   - `baseUrl`: URL for the local transformer
   - `secret`: Secret key for authenticating with the transformer

### Request Options Interfaces

1. **`WebhookSendOptions`**: Options for sending single events:
   - `eventTime`: Optional timestamp for when the event occurred
   - `validTime`: Optional timestamp for when the event becomes valid

2. **`WebhookSendBatchOptions`**: Options for sending batch events:
   - `eventTimeKey`: Optional key to extract event time from payload
   - `validTimeKey`: Optional key to extract valid time from payload

3. **`WebhookHeaderOptions`**: Combined options for HTTP headers:
   - Extends both `WebhookSendOptions` and `WebhookSendBatchOptions`
   - Adds `contentType` option for specifying the request content type

### Data and Function Types

1. **`WebhookFileData`**: Structure for file upload data:
   - `fileId`: Identifier for the file
   - `fileName`: Name of the file
   - `fileType`: MIME type or format of the file
   - `fileContent`: The file content as a Blob
   - `metadata`: Optional additional metadata

2. **`WebhookPredicate`**: Function type for event verification:
   - Takes an `eventId` and returns a boolean (or Promise of boolean)
   - Used to check if an event was successfully processed

3. **Client Interface Types**:
   - `Webhook<EventPayload, EventMetadata>`: Interface for regular event webhooks with `send` and `sendBatch` methods
   - `WebhookFile<EventMetadata>`: Interface for file upload webhooks with a `send` method

## Constants

- `RETRYABLE_STATUS_CODES`: HTTP status codes that should trigger a retry (408, 429, 500, 502, 503, 504)

This type system provides a strongly-typed foundation for the WebhookBuilder implementation, ensuring type safety and enabling schema validation of API responses.

