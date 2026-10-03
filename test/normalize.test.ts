import { describe, expect, it, vi } from "vitest";
import {
  normalizeKafkaValue,
  observeKafkaOrder,
  observeRabbitResult,
} from "../src/normalize.js";

const traceId = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
const orderId = "01ARZ3NDEKTSV4RRFFQ69G5FAW";

describe("event normalization", () => {
  it("preserves normalized trace events", () => {
    const value = {
      id: "01ARZ3NDEKTSV4RRFFQ69G5FAA",
      traceId,
      timestamp: "2026-08-28T10:00:00.000Z",
      source: "analytics",
      target: "dashboard",
      transport: "kafka",
      stage: "analytics.aggregate",
      status: "succeeded",
      summary: "Analytics updated",
      payload: { count: 1 },
    };
    expect(normalizeKafkaValue("flashdrop.traces.v1", value)).toEqual({
      kind: "trace",
      event: value,
    });
  });

  it("rejects retired storage trace records through the ingestion validation path", () => {
    const historical = {
      id: "01ARZ3NDEKTSV4RRFFQ69G5FAA",
      traceId,
      timestamp: "2026-08-28T10:00:00.000Z",
      source: "analytics",
      target: "mongodb",
      transport: "mongodb",
      stage: "analytics.persist",
      status: "succeeded",
      summary: "Historical analytics projection",
    };
    expect(() => normalizeKafkaValue("flashdrop.traces.v1", historical)).toThrow();
    expect(normalizeKafkaValue("flashdrop.traces.v1", {
      ...historical, target: "postgresql", transport: "postgresql",
    })).toMatchObject({ kind: "trace", event: { target: "postgresql" } });
  });

  it("preserves normalized order events", () => {
    const value = {
      schemaVersion: 1,
      eventId: "01ARZ3NDEKTSV4RRFFQ69G5FAX",
      eventType: "order.created",
      occurredAt: "2026-08-28T10:00:00.000Z",
      traceId,
      orderId,
      runId: "flashdrop-run-42",
      aggregateVersion: 1,
      data: {
        sku: "DROP-SNEAKER-RED",
        quantity: 2,
        currency: "USD",
        totalCents: 29_800,
        status: "pending",
      },
    } as const;

    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-08-30T12:00:00.000Z"));
    try {
      expect(normalizeKafkaValue("flashdrop.orders.v1", value)).toEqual({
        kind: "order",
        event: value,
      });
      expect(observeKafkaOrder(value)).toEqual({
        id: expect.stringMatching(/^[0-9A-HJKMNP-TV-Z]{26}$/),
        traceId,
        orderId,
        runId: "flashdrop-run-42",
        timestamp: "2026-08-30T12:00:00.000Z",
        source: "kafka",
        target: "event-hub",
        transport: "kafka",
        stage: "order.event.observed",
        status: "succeeded",
        summary: "Event Hub observed order.created",
        payload: {
          eventId: value.eventId,
          eventType: "order.created",
          aggregateVersion: 1,
          status: "pending",
          eventOccurredAt: "2026-08-28T10:00:00.000Z",
        },
      });
    } finally {
      vi.useRealTimers();
    }
  });

  it("records Event Hub's RabbitMQ inventory-result observation hop", () => {
    const result = {
      schemaVersion: 1,
      resultId: "01ARZ3NDEKTSV4RRFFQ69G5FAA",
      commandId: "01ARZ3NDEKTSV4RRFFQ69G5FAB",
      traceId,
      orderId,
      runId: "flashdrop-run-42",
      sku: "DROP-SNEAKER-RED",
      quantity: 2,
      status: "reserved",
      remainingStock: 98,
      processedAt: "2026-08-28T10:00:00.000Z",
    } as const;
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-08-30T12:00:00.000Z"));

    try {
      expect(observeRabbitResult(result)).toEqual({
        id: expect.stringMatching(/^[0-9A-HJKMNP-TV-Z]{26}$/),
        traceId,
        orderId,
        runId: "flashdrop-run-42",
        timestamp: "2026-08-30T12:00:00.000Z",
        source: "rabbitmq",
        target: "event-hub",
        transport: "rabbitmq",
        stage: "inventory.result.observe",
        status: "succeeded",
        summary: "Event Hub consumed a durable inventory result",
        payload: {
          resultId: result.resultId,
          commandId: result.commandId,
          resultStatus: "reserved",
        },
      });
    } finally {
      vi.useRealTimers();
    }
  });
});
