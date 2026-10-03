import { describe, expect, it } from "vitest";
import {
  InventoryReservationResultSchema,
  OrderEventEnvelopeSchema,
  TraceEventSchema,
} from "../src/contracts.js";

const result = {
  schemaVersion: 1,
  resultId: "01ARZ3NDEKTSV4RRFFQ69G5FAD",
  commandId: "01ARZ3NDEKTSV4RRFFQ69G5FAA",
  traceId: "01ARZ3NDEKTSV4RRFFQ69G5FAB",
  orderId: "01ARZ3NDEKTSV4RRFFQ69G5FAC",
  runId: "contract-test",
  sku: "DROP-CAP-LIME",
  quantity: 2,
  status: "reserved",
  remainingStock: 58,
  processedAt: "2026-08-30T10:00:01.000Z",
};

describe("event-hub wire contracts", () => {
  it.each(["reserved", "sold_out", "failed"])("accepts a v1 %s inventory result", (status) => {
    const value = { ...result, status };
    expect(InventoryReservationResultSchema.parse(value)).toEqual(value);
  });

  it.each([
    { schemaVersion: 2 },
    { quantity: "2" },
    { remainingStock: -1 },
    { extra: true },
  ])("rejects incompatible inventory results: %j", (override) => {
    expect(InventoryReservationResultSchema.safeParse({ ...result, ...override }).success).toBe(false);
  });

  it.each([
    ["order.created", "pending"],
    ["order.confirmed", "confirmed"],
    ["order.sold_out", "sold_out"],
    ["order.failed", "failed"],
  ])("preserves the %s order envelope", (eventType, status) => {
    const event = {
      schemaVersion: 1,
      eventId: "01ARZ3NDEKTSV4RRFFQ69G5FAE",
      eventType,
      occurredAt: result.processedAt,
      traceId: result.traceId,
      orderId: result.orderId,
      runId: result.runId,
      aggregateVersion: 1,
      data: { sku: result.sku, quantity: 2, currency: "USD", totalCents: 5000, status },
    };
    expect(OrderEventEnvelopeSchema.parse(event)).toEqual(event);
    expect(OrderEventEnvelopeSchema.safeParse({ ...event, schemaVersion: 2 }).success).toBe(false);
  });

  it("keeps optional correlation and payload fields in incoming traces", () => {
    const trace = {
      id: "01ARZ3NDEKTSV4RRFFQ69G5FAE",
      traceId: result.traceId,
      orderId: result.orderId,
      runId: result.runId,
      correlationId: result.commandId,
      causationId: result.resultId,
      timestamp: result.processedAt,
      source: "inventory-worker",
      target: "mysql",
      stage: "inventory.reserve",
      status: "succeeded",
      transport: "mysql",
      summary: "Inventory reserved",
      payload: { quantity: result.quantity },
    };
    expect(TraceEventSchema.parse(trace)).toEqual(trace);
    expect(TraceEventSchema.safeParse({ ...trace, transport: "unknown" }).success).toBe(false);
  });
});
