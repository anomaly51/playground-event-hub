import {
  InventoryReservationResultSchema,
  OrderEventEnvelopeSchema,
  TraceEventSchema,
  type OrderEventEnvelope,
  type TraceEvent,
} from "./contracts.js";
import { ulid } from "ulid";

export type NormalizedKafkaRecord =
  | { kind: "trace"; event: TraceEvent }
  | { kind: "order"; event: OrderEventEnvelope };

export function normalizeKafkaValue(
  topic: string,
  value: unknown,
  topics: { traces: string; orders: string } = {
    traces: "flashdrop.traces.v1",
    orders: "flashdrop.orders.v1",
  },
): NormalizedKafkaRecord {
  if (topic === topics.traces) {
    return { kind: "trace", event: TraceEventSchema.parse(value) };
  }
  if (topic === topics.orders) {
    return { kind: "order", event: OrderEventEnvelopeSchema.parse(value) };
  }
  throw new Error(`Unsupported Kafka topic: ${topic}`);
}

export function observeRabbitResult(value: unknown): TraceEvent {
  const result = InventoryReservationResultSchema.parse(value);
  return TraceEventSchema.parse({
    id: ulid(),
    traceId: result.traceId,
    orderId: result.orderId,
    ...(result.runId ? { runId: result.runId } : {}),
    timestamp: new Date().toISOString(),
    source: "rabbitmq",
    target: "event-hub",
    transport: "rabbitmq",
    stage: "inventory.result.observe",
    status: "succeeded",
    summary: "Event Hub consumed a durable inventory result",
    payload: {
      resultId: result.resultId,
      commandId: result.commandId,
      resultStatus: result.status,
    },
  });
}

export function observeKafkaOrder(event: OrderEventEnvelope): TraceEvent {
  return TraceEventSchema.parse({
    id: ulid(),
    traceId: event.traceId,
    orderId: event.orderId,
    ...(event.runId ? { runId: event.runId } : {}),
    timestamp: new Date().toISOString(),
    source: "kafka",
    target: "event-hub",
    transport: "kafka",
    stage: "order.event.observed",
    // The observation itself succeeded even when the business order outcome
    // carried by the event is failed.
    status: "succeeded",
    summary: `Event Hub observed ${event.eventType}`,
    payload: {
      eventId: event.eventId,
      eventType: event.eventType,
      aggregateVersion: event.aggregateVersion,
      status: event.data.status,
      eventOccurredAt: event.occurredAt,
    },
  });
}
