/**
 * Compatibility layer for the Flowcore Transformer Core SDK
 * 
 * This module re-exports components from the @flowcore/sdk-transformer-core package
 * to provide backwards compatibility with existing transformer implementations.
 */
import transformerCore from "npm:@flowcore/sdk-transformer-core"

/**
 * WebhookBuilder from the transformer core SDK for sending webhook events
 */
export const { WebhookBuilder } = transformerCore

/**
 * Options for configuring webhook send operations
 */
export type { WebhookSendOptions } from "npm:@flowcore/sdk-transformer-core"
