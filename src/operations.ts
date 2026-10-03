import { RABBITMQ } from "./contracts.js";
import type { FastifyBaseLogger } from "fastify";
import {
  Kafka,
  logLevel as kafkaLogLevel,
  type Admin,
} from "kafkajs";
import type { EventHubConfig } from "./config.js";
import { EVENT_HUB_RESULTS_QUEUE } from "./ingestion.js";
import type { EventHubMetrics } from "./metrics.js";

export interface NodeOperationalMetric {
  inFlight: number | null;
  total: number | null;
  healthy: boolean | null;
}

export interface OperationsSnapshot {
  nodes: Record<string, NodeOperationalMetric>;
  kafka: {
    lag: number | null;
    partitions: Array<{
      partition: number;
      currentOffset: string | null;
      endOffset: string | null;
      lag: number | null;
    }>;
  };
  rabbit: {
    ready: number | null;
    unacked: number | null;
    dlq: number | null;
    consumers: number | null;
  };
  postgres: { pendingOutbox: number | null };
  sseClients: number;
  sampledAt: string;
  fresh: boolean;
  errors: Array<{
    source: "kafka" | "rabbitmq" | "postgresql" | "services";
    message: string;
  }>;
}

type SnapshotError = OperationsSnapshot["errors"][number];

const NODE_IDS = [
  "traffic-mfe",
  "airflow",
  "order-api",
  "redis",
  "pricing",
  "postgresql",
  "outbox-relay",
  "kafka",
  "analytics",
  "rabbitmq",
  "inventory-worker",
  "mysql",
  "order-finalizer",
  "event-hub",
] as const;

const NODE_ALIASES: Record<string, string> = {
  gateway: "order-api",
  "api-gateway": "order-api",
  "order-service": "order-api",
  processor: "pricing",
  "pricing-service": "pricing",
  postgres: "postgresql",
  relay: "outbox-relay",
  "gateway-relay": "outbox-relay",
  "rabbit-worker": "inventory-worker",
  "node-rabbit-worker": "inventory-worker",
  eventhub: "event-hub",
};

interface QueueState {
  ready: number;
  unacked: number;
  consumers: number;
  published: number | null;
}

interface ServiceContribution {
  nodes: Record<string, NodeOperationalMetric>;
  pendingOutbox: number | null;
  errors: SnapshotError[];
  successfulSamples: number;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function nullableMetric(value: unknown): number | null | undefined {
  if (value === null) return null;
  return typeof value === "number" && Number.isFinite(value) && value >= 0
    ? value
    : undefined;
}

function parseNodeMetric(value: unknown): NodeOperationalMetric | undefined {
  if (!isRecord(value)) return undefined;
  const inFlight = nullableMetric(value.inFlight);
  const total = nullableMetric(value.total);
  const healthy = value.healthy;
  if (
    inFlight === undefined
    || total === undefined
    || !(healthy === null || typeof healthy === "boolean")
  ) return undefined;
  return { inFlight, total, healthy };
}

function emptyNodes(): Record<string, NodeOperationalMetric> {
  return Object.fromEntries(
    NODE_IDS.map((id) => [id, { inFlight: null, total: null, healthy: null }]),
  );
}

function safeInteger(value: bigint): number | null {
  return value <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(value) : null;
}

function errorMessage(error: unknown): string {
  if (error instanceof Error) return error.message.slice(0, 512);
  return String(error).slice(0, 512);
}

export class OperationsSampler {
  private readonly admin: Admin;
  private adminConnected = false;
  private timer?: NodeJS.Timeout;
  private sampling?: Promise<void>;
  private snapshot: OperationsSnapshot;

  constructor(
    private readonly config: EventHubConfig,
    private readonly logger: FastifyBaseLogger,
    private readonly metrics: EventHubMetrics,
    private readonly localRuntime: () => {
      inFlight: number;
      total: number;
      healthy: boolean;
    },
    private readonly fetchImpl: typeof fetch = fetch,
  ) {
    const kafka = new Kafka({
      clientId: `${config.kafkaClientId}-operations`,
      brokers: config.kafkaBrokers,
      connectionTimeout: config.operationsRequestTimeoutMs,
      requestTimeout: config.operationsRequestTimeoutMs,
      retry: { retries: 1, initialRetryTime: 100, maxRetryTime: 500 },
      logLevel: kafkaLogLevel.NOTHING,
    });
    this.admin = kafka.admin();
    this.snapshot = {
      nodes: emptyNodes(),
      kafka: { lag: null, partitions: [] },
      rabbit: { ready: null, unacked: null, dlq: null, consumers: null },
      postgres: { pendingOutbox: null },
      sseClients: 0,
      sampledAt: new Date(0).toISOString(),
      fresh: false,
      errors: [],
    };
  }

  async start(): Promise<void> {
    await this.sample();
    this.timer = setInterval(() => void this.sample(), this.config.operationsSampleIntervalMs);
    this.timer.unref();
  }

  async stop(): Promise<void> {
    if (this.timer) clearInterval(this.timer);
    await this.sampling?.catch(() => undefined);
    if (this.adminConnected) await this.admin.disconnect().catch(() => undefined);
    this.adminConnected = false;
  }

  current(): OperationsSnapshot {
    const sampledAt = Date.parse(this.snapshot.sampledAt);
    const withinWindow = Number.isFinite(sampledAt)
      && Date.now() - sampledAt <= this.config.operationsStaleAfterMs;
    return structuredClone({
      ...this.snapshot,
      fresh: this.snapshot.fresh && withinWindow,
      sseClients: this.metrics.sseClientCount,
    });
  }

  async sample(): Promise<void> {
    if (this.sampling) return this.sampling;
    const operation = this.collect().finally(() => {
      if (this.sampling === operation) this.sampling = undefined;
    });
    this.sampling = operation;
    return operation;
  }

  private async collect(): Promise<void> {
    const nodes = emptyNodes();
    const errors: SnapshotError[] = [];
    let successfulSamples = 0;
    const local = this.localRuntime();
    nodes["event-hub"] = {
      inFlight: local.inFlight,
      total: local.total,
      healthy: local.healthy,
    };

    const [kafkaResult, rabbitResult, serviceResult] = await Promise.all([
      this.sampleKafka(),
      this.sampleRabbit(),
      this.sampleServices(),
    ]);

    if (kafkaResult.error) {
      errors.push(kafkaResult.error);
      nodes.kafka = { inFlight: null, total: null, healthy: false };
    }
    else {
      successfulSamples += 1;
      nodes.kafka = {
        inFlight: kafkaResult.value.lag,
        total: kafkaResult.value.total,
        healthy: true,
      };
    }
    if (rabbitResult.error) {
      errors.push(rabbitResult.error);
      nodes.rabbitmq = { inFlight: null, total: null, healthy: false };
    }
    else {
      successfulSamples += 1;
      const active = rabbitResult.value.snapshot.ready === null
        || rabbitResult.value.snapshot.unacked === null
        || rabbitResult.value.snapshot.dlq === null
        ? null
        : rabbitResult.value.snapshot.ready
          + rabbitResult.value.snapshot.unacked
          + rabbitResult.value.snapshot.dlq;
      nodes.rabbitmq = {
        inFlight: active,
        total: rabbitResult.value.total,
        healthy: true,
      };
    }
    successfulSamples += serviceResult.successfulSamples;
    errors.push(...serviceResult.errors);
    Object.assign(nodes, serviceResult.nodes);
    if (serviceResult.pendingOutbox !== null) {
      const postgresql = nodes.postgresql
        ?? { inFlight: null, total: null, healthy: null };
      nodes.postgresql = {
        ...postgresql,
        inFlight: serviceResult.pendingOutbox,
      };
    }

    const sampledAt = new Date().toISOString();
    this.snapshot = {
      nodes,
      kafka: kafkaResult.error
        ? { lag: null, partitions: [] }
        : {
            lag: kafkaResult.value.lag,
            partitions: kafkaResult.value.partitions,
          },
      rabbit: rabbitResult.error
        ? { ready: null, unacked: null, dlq: null, consumers: null }
        : rabbitResult.value.snapshot,
      postgres: { pendingOutbox: serviceResult.pendingOutbox },
      sseClients: this.metrics.sseClientCount,
      sampledAt,
      fresh: successfulSamples > 0,
      errors,
    };
    if (this.snapshot.kafka.lag !== null) {
      this.metrics.kafkaLag.set(this.snapshot.kafka.lag);
    }
    for (const [state, value] of Object.entries(this.snapshot.rabbit)) {
      if (value !== null) this.metrics.rabbitMessages.set({ state }, value);
    }
    this.metrics.operationsFresh.set(this.snapshot.fresh ? 1 : 0);
  }

  private async sampleKafka(): Promise<{
    value: OperationsSnapshot["kafka"] & { total: number | null };
    error?: undefined;
  } | { value?: undefined; error: SnapshotError }> {
    try {
      if (!this.adminConnected) {
        await this.admin.connect();
        this.adminConnected = true;
      }
      const [endOffsets, committedTopics] = await Promise.all([
        this.admin.fetchTopicOffsets(this.config.kafkaOrdersTopic),
        this.admin.fetchOffsets({
          groupId: this.config.kafkaAnalyticsGroupId,
          topics: [this.config.kafkaOrdersTopic],
          resolveOffsets: false,
        }),
      ]);
      const committed = new Map(
        (committedTopics.find((entry) => entry.topic === this.config.kafkaOrdersTopic)
          ?.partitions ?? [])
          .map((entry) => [entry.partition, entry.offset]),
      );
      let totalLag = 0;
      let totalEnd = 0;
      let completeLag = true;
      let completeEnd = true;
      const partitions = endOffsets
        .sort((left, right) => left.partition - right.partition)
        .map((entry) => {
          const currentRaw = committed.get(entry.partition);
          const lowOffset = entry.low || null;
          const currentOffset = currentRaw === undefined
            ? null
            : BigInt(currentRaw) === -1n
              ? lowOffset
              : BigInt(currentRaw) >= 0n
                ? currentRaw
                : null;
          const endOffset = entry.high || null;
          let lag: number | null = null;
          if (currentOffset !== null && endOffset !== null) {
            lag = safeInteger(
              BigInt(endOffset) > BigInt(currentOffset)
                ? BigInt(endOffset) - BigInt(currentOffset)
                : 0n,
            );
          }
          if (lag === null) completeLag = false;
          else totalLag += lag;
          const end = endOffset === null ? null : safeInteger(BigInt(endOffset));
          if (end === null) completeEnd = false;
          else totalEnd += end;
          return { partition: entry.partition, currentOffset, endOffset, lag };
        });
      return {
        value: {
          lag: completeLag ? totalLag : null,
          total: completeEnd ? totalEnd : null,
          partitions,
        },
      };
    } catch (error) {
      this.adminConnected = false;
      await this.admin.disconnect().catch(() => undefined);
      return {
        error: { source: "kafka", message: errorMessage(error) },
      };
    }
  }

  private async sampleRabbit(): Promise<{
    value: { snapshot: OperationsSnapshot["rabbit"]; total: number | null };
    error?: undefined;
  } | { value?: undefined; error: SnapshotError }> {
    try {
      const [commands, retry, deadLetter, orderResults, eventHubResults] = await Promise.all([
        this.fetchQueue(RABBITMQ.queues.commands),
        this.fetchQueue(RABBITMQ.queues.retry),
        this.fetchQueue(RABBITMQ.queues.deadLetter),
        this.fetchQueue(RABBITMQ.queues.orderResults),
        this.fetchQueue(EVENT_HUB_RESULTS_QUEUE),
      ]);
      return {
        value: {
          snapshot: {
            // These are physical queued deliveries. The fanout result appears
            // once in each durable consumer queue by design.
            ready: commands.ready + retry.ready + orderResults.ready + eventHubResults.ready,
            unacked: commands.unacked
              + retry.unacked
              + orderResults.unacked
              + eventHubResults.unacked,
            dlq: deadLetter.ready + deadLetter.unacked,
            consumers: commands.consumers
              + retry.consumers
              + orderResults.consumers
              + eventHubResults.consumers,
          },
          total: commands.published === null
            || retry.published === null
            || deadLetter.published === null
            || orderResults.published === null
            || eventHubResults.published === null
            ? null
            : commands.published
              + retry.published
              + deadLetter.published
              + orderResults.published
              + eventHubResults.published,
        },
      };
    } catch (error) {
      return {
        error: { source: "rabbitmq", message: errorMessage(error) },
      };
    }
  }

  private async fetchQueue(queue: string): Promise<QueueState> {
    const authorization = Buffer.from(
      `${this.config.rabbitManagementUser}:${this.config.rabbitManagementPassword}`,
    ).toString("base64");
    const response = await this.fetchImpl(
      `${this.config.rabbitManagementUrl}/api/queues/%2F/${encodeURIComponent(queue)}`,
      {
        headers: { authorization: `Basic ${authorization}`, accept: "application/json" },
        signal: AbortSignal.timeout(this.config.operationsRequestTimeoutMs),
      },
    );
    if (!response.ok) throw new Error(`RabbitMQ ${queue} returned HTTP ${response.status}`);
    const document = await response.json() as unknown;
    if (!isRecord(document)) throw new Error(`RabbitMQ ${queue} returned invalid JSON`);
    const ready = nullableMetric(document.messages_ready);
    const unacked = nullableMetric(document.messages_unacknowledged);
    const consumers = nullableMetric(document.consumers);
    if (ready == null || unacked == null || consumers == null) {
      throw new Error(`RabbitMQ ${queue} omitted queue counters`);
    }
    const messageStats = isRecord(document.message_stats)
      ? document.message_stats
      : undefined;
    const published = messageStats
      ? nullableMetric(messageStats.publish) ?? 0
      : 0;
    return { ready, unacked, consumers, published };
  }

  private async sampleServices(): Promise<ServiceContribution> {
    const nodes: Record<string, NodeOperationalMetric> = {};
    const errors: SnapshotError[] = [];
    let pendingOutbox: number | null = null;
    let successfulSamples = 0;
    const endpoints = [
      { name: "gateway", url: this.config.gatewayOperationsUrl, nodeIds: ["order-api"] },
      { name: "relay", url: this.config.relayOperationsUrl, nodeIds: ["outbox-relay", "order-finalizer"] },
      { name: "pricing", url: this.config.pricingOperationsUrl, nodeIds: ["pricing"] },
      { name: "analytics", url: this.config.analyticsOperationsUrl, nodeIds: ["analytics"] },
      { name: "inventory", url: this.config.inventoryOperationsUrl, nodeIds: ["inventory-worker"] },
    ];
    const responses = await Promise.all(endpoints.map(async (endpoint) => {
      try {
        const document = endpoint.name === "gateway"
          ? await this.fetchGatewayRuntime(endpoint.url)
          : await this.fetchJson(endpoint.url);
        return { ...endpoint, document };
      } catch (error) {
        return { ...endpoint, error };
      }
    }));

    for (const response of responses) {
      if ("error" in response) {
        response.nodeIds.forEach((nodeId) => {
          nodes[nodeId] = {
            inFlight: null,
            total: null,
            healthy: false,
          };
        });
        errors.push({
          source: "services",
          message: `${response.name}: ${errorMessage(response.error)}`,
        });
        continue;
      }
      successfulSamples += 1;
      // Analytics shares PostgreSQL with orders but owns only its worker metric.
      this.mergeNodes(
        nodes,
        response.document,
        response.name === "analytics" ? ["analytics"] : undefined,
      );
      if (response.name !== "analytics") {
        const pending = this.pendingOutbox(response.document);
        if (pending !== null) pendingOutbox = pending;
      }
      if (response.name === "gateway") this.mergeLegacyGateway(nodes, response.document);
    }

    if (this.config.airflowHealthUrl) {
      try {
        const health = await this.fetchJson(this.config.airflowHealthUrl);
        nodes.airflow = { inFlight: null, total: null, healthy: this.airflowHealthy(health) };
        successfulSamples += 1;
      } catch (error) {
        nodes.airflow = { inFlight: null, total: null, healthy: false };
        errors.push({ source: "services", message: `airflow: ${errorMessage(error)}` });
      }
    }
    return { nodes, pendingOutbox, errors, successfulSamples };
  }

  private async fetchGatewayRuntime(url: string): Promise<unknown> {
    try {
      return await this.fetchJson(url);
    } catch (operationsError) {
      const fallback = new URL("/api/v1/runtime", url).toString();
      try {
        return await this.fetchJson(fallback);
      } catch {
        throw operationsError;
      }
    }
  }

  private async fetchJson(url: string): Promise<unknown> {
    const response = await this.fetchImpl(url, {
      headers: { accept: "application/json" },
      signal: AbortSignal.timeout(this.config.operationsRequestTimeoutMs),
    });
    if (!response.ok) throw new Error(`${new URL(url).pathname} returned HTTP ${response.status}`);
    return response.json();
  }

  private mergeNodes(
    target: Record<string, NodeOperationalMetric>,
    document: unknown,
    allowedIds?: readonly string[],
  ): void {
    if (!isRecord(document) || !isRecord(document.nodes)) return;
    for (const [rawId, value] of Object.entries(document.nodes)) {
      const id = NODE_ALIASES[rawId] ?? rawId;
      if (!NODE_IDS.includes(id as (typeof NODE_IDS)[number])) continue;
      if (allowedIds && !allowedIds.includes(id)) continue;
      const metric = parseNodeMetric(value);
      if (metric) target[id] = metric;
    }
  }

  private pendingOutbox(document: unknown): number | null {
    if (!isRecord(document)) return null;
    if (isRecord(document.postgres)) {
      return nullableMetric(document.postgres.pendingOutbox) ?? null;
    }
    const storage = isRecord(document.storage) ? document.storage : undefined;
    const outbox = storage && isRecord(storage.outbox) ? storage.outbox : undefined;
    return outbox ? nullableMetric(outbox.pending) ?? null : null;
  }

  private mergeLegacyGateway(
    target: Record<string, NodeOperationalMetric>,
    document: unknown,
  ): void {
    if (!isRecord(document)) return;
    const storage = isRecord(document.storage) ? document.storage : undefined;
    const orders = storage && isRecord(storage.orders) ? storage.orders : undefined;
    if (!orders) return;
    const counts = ["pending", "confirmed", "sold_out", "failed"]
      .map((status) => nullableMetric(orders[status]));
    const total = counts.every((value) => typeof value === "number")
      ? (counts as number[]).reduce((sum, value) => sum + value, 0)
      : null;
    const dependencies = isRecord(document.dependencies) ? document.dependencies : undefined;
    target["order-api"] = {
      inFlight: null,
      total,
      healthy: dependencies && typeof dependencies.postgres === "boolean"
        ? dependencies.postgres
        : null,
    };
    target.postgresql = {
      inFlight: this.pendingOutbox(document),
      total,
      healthy: dependencies && typeof dependencies.postgres === "boolean"
        ? dependencies.postgres
        : null,
    };
  }

  private airflowHealthy(document: unknown): boolean {
    if (!isRecord(document)) return false;
    const required = ["metadatabase", "scheduler", "dag_processor"];
    return required.every((name) => {
      const component = document[name];
      return isRecord(component) && component.status === "healthy";
    });
  }
}
