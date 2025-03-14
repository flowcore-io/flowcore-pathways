export const validEvent = {
  id: "test-id-1",
  organizationId: "org-1",
  dataCoreId: "dc-1",
  flowTypeId: "ft-1",
  name: "Test Event 1"
};

export const invalidEvent = {
  id: "test-id-2",
  organizationId: "org-2",
  // missing dataCoreId
  flowTypeId: "ft-2",
  name: "Test Event 2"
};

export const mockFlowcoreLegacyEvent = {
  eventId: "legacy-event-1",
  tenant: "default",
  dataCoreId: "default",
  flowType: "event-type.1",
  eventType: "event-type.created.0",
  payload: validEvent,
  metadata: {},
  timeBucket: "202403201200",
  validTime: new Date().toISOString()
}; 