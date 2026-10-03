import { z } from "zod";

// Service-owned wire schemas; preserve the versioned payload format.

export const FLASHDROP_SKUS = [
  "DROP-SNEAKER-RED",
  "DROP-HOODIE-BLACK",
  "DROP-CAP-LIME",
] as const;

export const ORDER_STATUSES = [
  "pending",
  "confirmed",
  "sold_out",
  "failed",
] as const;

export const ORDER_EVENT_TYPES = [
  "order.created",
  "order.confirmed",
  "order.sold_out",
  "order.failed",
] as const;

export const TRACE_STATUSES = ["started", "succeeded", "failed", "retrying"] as const;

export const TRACE_TRANSPORTS = [
  "http",
  "kafka",
  "rabbitmq",
  "redis",
  "postgresql",
  "mysql",
  "airflow",
  "sse",
] as const;

export const RABBITMQ = {
  exchanges: {
    commands: "flashdrop.inventory.commands.v1",
    retry: "flashdrop.inventory.retry.v1",
    deadLetter: "flashdrop.inventory.dlx.v1",
    results: "flashdrop.inventory.results.v1",
  },
  queues: {
    commands: "flashdrop.inventory-worker.commands.v1",
    retry: "flashdrop.inventory-worker.retry.v1",
    deadLetter: "flashdrop.inventory-worker.dlq.v1",
    orderResults: "flashdrop.order-service.inventory-results.v1",
  },
  routingKeys: {
    process: "inventory.reserve",
    retry: "inventory.retry",
    deadLetter: "inventory.dead",
  },
} as const;

export const TraceEventSchema = z
  .object({
    id: z.string().ulid(),
    traceId: z.string().ulid(),
    runId: z.string().min(1).max(128).optional(),
    orderId: z.string().ulid().optional(),
    correlationId: z.string().min(1).max(128).optional(),
    causationId: z.string().min(1).max(128).optional(),
    timestamp: z.string().datetime({ offset: true }),
    source: z.string().min(1).max(128),
    target: z.string().min(1).max(128).optional(),
    stage: z.string().min(1).max(128),
    status: z.enum(TRACE_STATUSES),
    transport: z.enum(TRACE_TRANSPORTS),
    summary: z.string().min(1).max(2_048),
    payload: z.record(z.unknown()).optional(),
  })
  .strict();

export const OrderEventEnvelopeSchema = z
  .object({
    schemaVersion: z.literal(1),
    eventId: z.string().ulid(),
    eventType: z.enum(ORDER_EVENT_TYPES),
    occurredAt: z.string().datetime({ offset: true }),
    traceId: z.string().ulid(),
    orderId: z.string().ulid(),
    runId: z.string().min(1).max(128).optional(),
    aggregateVersion: z.number().int().positive(),
    data: z
      .object({
        sku: z.enum(FLASHDROP_SKUS),
        quantity: z.number().int().min(1).max(5),
        currency: z.string().length(3),
        totalCents: z.number().int().nonnegative(),
        status: z.enum(ORDER_STATUSES),
        reason: z.string().max(512).optional(),
      })
      .strict(),
  })
  .strict();

export const InventoryReservationResultSchema = z
  .object({
    schemaVersion: z.literal(1),
    resultId: z.string().ulid(),
    commandId: z.string().ulid(),
    traceId: z.string().ulid(),
    orderId: z.string().ulid(),
    runId: z.string().min(1).max(128).optional(),
    sku: z.enum(FLASHDROP_SKUS),
    quantity: z.number().int().min(1).max(5),
    status: z.enum(["reserved", "sold_out", "failed"]),
    remainingStock: z.number().int().nonnegative().optional(),
    reason: z.string().max(512).optional(),
    processedAt: z.string().datetime({ offset: true }),
  })
  .strict();

export type TraceEvent = z.infer<typeof TraceEventSchema>;

export type OrderEventEnvelope = z.infer<typeof OrderEventEnvelopeSchema>;
