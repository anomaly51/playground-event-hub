import { RABBITMQ } from "../src/contracts.js";
import type { FastifyBaseLogger } from "fastify";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const kafkaAdmin = vi.hoisted(() => ({
  connect: vi.fn(),
  disconnect: vi.fn(),
  fetchTopicOffsets: vi.fn(),
  fetchOffsets: vi.fn(),
}));

vi.mock("kafkajs", () => ({
  Kafka: class Kafka {
    admin() {
      return kafkaAdmin;
    }
  },
  logLevel: { NOTHING: 0 },
}));

import { EventHubMetrics } from "../src/metrics.js";
import { EVENT_HUB_RESULTS_QUEUE } from "../src/ingestion.js";
import { OperationsSampler } from "../src/operations.js";
import { eventHubConfig } from "./fixtures.js";

const sampledAt = "2026-08-30T12:00:00.000Z";

const serviceDocuments: Record<string, unknown> = {
  "http://gateway.test/operations/snapshot": {
    nodes: {
      "traffic-mfe": { inFlight: null, total: null, healthy: true },
      gateway: { inFlight: 1, total: 10, healthy: true },
      redis: { inFlight: 0, total: 100, healthy: true },
      postgres: { inFlight: 0, total: 10, healthy: true },
      relay: { inFlight: 2, total: 8, healthy: true },
      "order-finalizer": { inFlight: 1, total: 3, healthy: true },
    },
    postgres: { pendingOutbox: 4 },
  },
  "http://relay.test/operations/snapshot": {
    nodes: {
      "outbox-relay": { inFlight: 3, total: 12, healthy: true },
      "order-finalizer": { inFlight: 2, total: 5, healthy: true },
    },
  },
  "http://pricing.test/operations/snapshot": {
    nodes: { processor: { inFlight: 0, total: 9, healthy: true } },
  },
  "http://analytics.test/operations/snapshot": {
    nodes: {
      analytics: { inFlight: 1, total: 7, healthy: true },
    },
  },
  "http://inventory.test/operations/snapshot": {
    nodes: {
      "rabbit-worker": { inFlight: 2, total: 6, healthy: true },
      mysql: { inFlight: 0, total: 6, healthy: true },
    },
  },
};

const queueDocuments: Record<string, unknown> = {
  [RABBITMQ.queues.commands]: {
    messages_ready: 3,
    messages_unacknowledged: 1,
    consumers: 2,
    message_stats: { publish: 10 },
  },
  [RABBITMQ.queues.retry]: {
    messages_ready: 2,
    messages_unacknowledged: 1,
    consumers: 1,
  },
  [RABBITMQ.queues.deadLetter]: {
    messages_ready: 4,
    messages_unacknowledged: 3,
    consumers: 0,
    message_stats: { publish: 7 },
  },
  [RABBITMQ.queues.orderResults]: {
    messages_ready: 6,
    messages_unacknowledged: 2,
    consumers: 4,
    message_stats: { publish: 11 },
  },
  [EVENT_HUB_RESULTS_QUEUE]: {
    messages_ready: 8,
    messages_unacknowledged: 3,
    consumers: 5,
    message_stats: { publish: 13 },
  },
};

function jsonResponse(document: unknown): Response {
  return new Response(JSON.stringify(document), {
    status: 200,
    headers: { "content-type": "application/json" },
  });
}

function operationsFetch(
  airflow: unknown,
  unavailableUrls: ReadonlySet<string> = new Set(),
): typeof fetch {
  return vi.fn<typeof fetch>(async (input) => {
    const url = String(input);
    if (unavailableUrls.has(url)) return new Response(null, { status: 503 });
    const service = serviceDocuments[url];
    if (service !== undefined) return jsonResponse(service);
    if (url === "http://airflow.test/api/v2/monitor/health") {
      return jsonResponse(airflow);
    }
    const parsed = new URL(url);
    if (parsed.pathname.startsWith("/api/queues/%2F/")) {
      const queue = decodeURIComponent(parsed.pathname.split("/").at(-1) ?? "");
      const document = queueDocuments[queue];
      if (document !== undefined) return jsonResponse(document);
    }
    return new Response(null, { status: 404 });
  });
}

function createSampler(
  airflow: unknown,
  unavailableUrls: ReadonlySet<string> = new Set(),
) {
  const metrics = new EventHubMetrics();
  metrics.connectSseClient();
  return new OperationsSampler(
    eventHubConfig(),
    { error: vi.fn(), warn: vi.fn() } as unknown as FastifyBaseLogger,
    metrics,
    () => ({ inFlight: 3, total: 40, healthy: true }),
    operationsFetch(airflow, unavailableUrls),
  );
}

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date(sampledAt));
  kafkaAdmin.connect.mockResolvedValue(undefined);
  kafkaAdmin.disconnect.mockResolvedValue(undefined);
  kafkaAdmin.fetchTopicOffsets.mockResolvedValue([
    { partition: 1, low: "0", high: "25" },
    { partition: 0, low: "0", high: "10" },
  ]);
  kafkaAdmin.fetchOffsets.mockResolvedValue([
    {
      topic: "flashdrop.orders.v1",
      partitions: [
        { partition: 0, offset: "7" },
        { partition: 1, offset: "20" },
      ],
    },
  ]);
});

afterEach(() => {
  vi.useRealTimers();
  vi.clearAllMocks();
});

describe("OperationsSampler", () => {
  it("publishes the exact FlashDrop operations snapshot", async () => {
    const sampler = createSampler({
      metadatabase: { status: "healthy" },
      scheduler: { status: "healthy", latest_scheduler_heartbeat: sampledAt },
      triggerer: { status: null, latest_triggerer_heartbeat: null },
      dag_processor: { status: "healthy", latest_dag_processor_heartbeat: sampledAt },
    });

    await sampler.sample();

    expect(sampler.current()).toEqual({
      nodes: {
        "traffic-mfe": { inFlight: null, total: null, healthy: true },
        airflow: { inFlight: null, total: null, healthy: true },
        "order-api": { inFlight: 1, total: 10, healthy: true },
        redis: { inFlight: 0, total: 100, healthy: true },
        pricing: { inFlight: 0, total: 9, healthy: true },
        postgresql: { inFlight: 4, total: 10, healthy: true },
        "outbox-relay": { inFlight: 3, total: 12, healthy: true },
        kafka: { inFlight: 8, total: 35, healthy: true },
        analytics: { inFlight: 1, total: 7, healthy: true },
        rabbitmq: { inFlight: 33, total: 41, healthy: true },
        "inventory-worker": { inFlight: 2, total: 6, healthy: true },
        mysql: { inFlight: 0, total: 6, healthy: true },
        "order-finalizer": { inFlight: 2, total: 5, healthy: true },
        "event-hub": { inFlight: 3, total: 40, healthy: true },
      },
      kafka: {
        lag: 8,
        partitions: [
          { partition: 0, currentOffset: "7", endOffset: "10", lag: 3 },
          { partition: 1, currentOffset: "20", endOffset: "25", lag: 5 },
        ],
      },
      rabbit: { ready: 19, unacked: 7, dlq: 7, consumers: 12 },
      postgres: { pendingOutbox: 4 },
      sseClients: 1,
      sampledAt,
      fresh: true,
      errors: [],
    });
    expect(kafkaAdmin.fetchOffsets).toHaveBeenCalledWith({
      groupId: "flashdrop-analytics-v1",
      topics: ["flashdrop.orders.v1"],
      resolveOffsets: false,
    });

    await sampler.stop();
  });

  it("keeps order PostgreSQL metrics when analytics returns shared-storage counters", async () => {
    const analyticsUrl = "http://analytics.test/operations/snapshot";
    const original = serviceDocuments[analyticsUrl];
    serviceDocuments[analyticsUrl] = {
      nodes: {
        analytics: { inFlight: 1, total: 7, healthy: true },
        postgresql: { inFlight: 99, total: 700, healthy: false },
      },
      postgres: { pendingOutbox: 99 },
    };
    const sampler = createSampler({
      metadatabase: { status: "healthy" },
      scheduler: { status: "healthy" },
      dag_processor: { status: "healthy" },
    });
    try {
      await sampler.sample();
      expect(sampler.current().nodes.analytics).toEqual({
        inFlight: 1, total: 7, healthy: true,
      });
      expect(sampler.current().nodes.postgresql).toEqual({
        inFlight: 4, total: 10, healthy: true,
      });
      expect(sampler.current().postgres.pendingOutbox).toBe(4);
    } finally {
      serviceDocuments[analyticsUrl] = original;
      await sampler.stop();
    }
  });

  it("uses the low watermark when a consumer group has not committed an offset", async () => {
    kafkaAdmin.fetchTopicOffsets.mockResolvedValue([
      { partition: 1, low: "5", high: "12" },
      { partition: 0, low: "0", high: "0" },
    ]);
    kafkaAdmin.fetchOffsets.mockResolvedValue([{
      topic: "flashdrop.orders.v1",
      partitions: [
        { partition: 0, offset: "-1" },
        { partition: 1, offset: "-1" },
      ],
    }]);
    const sampler = createSampler({
      metadatabase: { status: "healthy" },
      scheduler: { status: "healthy" },
      dag_processor: { status: "healthy" },
    });

    await sampler.sample();

    expect(sampler.current().kafka).toEqual({
      lag: 7,
      partitions: [
        { partition: 0, currentOffset: "0", endOffset: "0", lag: 0 },
        { partition: 1, currentOffset: "5", endOffset: "12", lag: 7 },
      ],
    });
    expect(sampler.current().nodes.kafka).toEqual({
      inFlight: 7,
      total: 12,
      healthy: true,
    });
    await sampler.stop();
  });

  it("uses null only when committed offset metadata is unavailable", async () => {
    kafkaAdmin.fetchTopicOffsets.mockResolvedValue([
      { partition: 0, low: "3", high: "9" },
      { partition: 1, low: "4", high: "10" },
    ]);
    kafkaAdmin.fetchOffsets.mockResolvedValue([{
      topic: "flashdrop.orders.v1",
      partitions: [{ partition: 0, offset: "-1" }],
    }]);
    const sampler = createSampler({
      metadatabase: { status: "healthy" },
      scheduler: { status: "healthy" },
      dag_processor: { status: "healthy" },
    });

    await sampler.sample();

    expect(sampler.current().kafka).toEqual({
      lag: null,
      partitions: [
        { partition: 0, currentOffset: "3", endOffset: "9", lag: 6 },
        { partition: 1, currentOffset: null, endOffset: "10", lag: null },
      ],
    });
    await sampler.stop();
  });

  it("requires every deployed Airflow control-plane component to be healthy", async () => {
    const sampler = createSampler({
      metadatabase: { status: "healthy" },
      scheduler: { status: "unhealthy" },
      dag_processor: { status: "healthy" },
    });

    await sampler.sample();

    expect(sampler.current().nodes.airflow).toEqual({
      inFlight: null,
      total: null,
      healthy: false,
    });
    await sampler.stop();
  });

  it("marks both relay-owned nodes unhealthy when the relay endpoint fails", async () => {
    const sampler = createSampler(
      {
        metadatabase: { status: "healthy" },
        scheduler: { status: "healthy" },
        dag_processor: { status: "healthy" },
      },
      new Set(["http://relay.test/operations/snapshot"]),
    );

    await sampler.sample();

    const snapshot = sampler.current();
    expect({
      outboxRelay: snapshot.nodes["outbox-relay"],
      orderFinalizer: snapshot.nodes["order-finalizer"],
      errors: snapshot.errors,
    }).toEqual({
      outboxRelay: { inFlight: null, total: null, healthy: false },
      orderFinalizer: { inFlight: null, total: null, healthy: false },
      errors: [{
        source: "services",
        message: "relay: /operations/snapshot returned HTTP 503",
      }],
    });
    await sampler.stop();
  });
});
