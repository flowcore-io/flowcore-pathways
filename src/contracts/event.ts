export interface FlowcoreEvent<Payload = unknown, Metadata = Record<string, unknown>> {
  eventId: string
  timeBucket: string
  tenant: string
  dataCoreId: string
  flowType: string
  eventType: string
  metadata: Metadata
  payload: Payload
  validTime: string
}
