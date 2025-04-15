# WebhookBuilder Code Summary

This code implements a webhook client library for interacting with a webhook API service, specifically designed for the
Flowcore platform. Here's a conceptual overview of what this code does:

## Core Concepts

1. **Webhook Builder Pattern**: Uses a builder pattern to configure and create webhook clients.
2. **Event Handling**: Manages sending events (single or batch) and file uploads to webhook endpoints.
3. **Retry Mechanisms**: Implements configurable retry logic for both API requests and predicate checks.
4. **Validation**: Validates responses against schema definitions.
5. **Error Handling**: Custom error types for different failure scenarios.

## Key Classes and Methods

### `WebhookBuilder` Class

The main class that configures and creates webhook clients with these key methods:

#### Configuration Methods

- `constructor(options)`: Initializes with base URL, tenant, dataCore, and API key.
- `withRetry(retryOptions)`: Configures retry behavior for API requests.
- `withPredicate({predicate, options})`: Sets up a predicate function to check event processing status.
- `withLocalTransform(options)`: Configures local transformation of events.
- `factory()`: Creates a factory function that produces new instances with the same configuration.

#### Webhook Creation Methods

- `buildWebhook(flowType, eventType)`: Creates a webhook client for regular events.
- `buildFileWebhook(flowType, eventType)`: Creates a webhook client for file uploads.

#### Helper Methods

- `fetchWithRetry(url, options)`: Handles HTTP requests with configurable retry logic.
- `doPredicateCheck(eventIds)`: Verifies event processing through the predicate function.
- `doLocalTransform(flowType, eventType, payload, eventId)`: Sends events to a local transformer.
- `validateWebhookResponse(response, responseSchema)`: Validates API responses against schemas.
- `getHeaders(metadata, options)`: Constructs HTTP headers for requests.
- `getUrl(flowType, eventType, type)`: Builds API endpoint URLs.

### Utility Function

- `sleep(ms, attempt)`: Implements delay between retry attempts, with support for dynamic timing.

## Key Workflows

1. **Sending an Event**:
   - Construct the request with proper headers
   - Send data to the API endpoint
   - Validate the response
   - Optionally transform the event locally
   - Verify event processing via predicate checks

2. **Batch Event Processing**:
   - Similar to single events but handles arrays of payloads
   - Validates that response contains the correct number of event IDs

3. **File Upload**:
   - Constructs a FormData object with file content and metadata
   - Sends to the file-specific endpoint
   - Validates response and runs predicate checks

4. **Error Handling**:
   - Custom error types for different failure scenarios
   - Structured error information for debugging

This library abstracts away the complexity of interacting with webhook APIs, providing a clean interface for sending
events while handling common concerns like retries, validation, and error management.
