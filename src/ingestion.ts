import amqp, {
  type Channel,
  type ChannelModel,
  type ConsumeMessage,
  type Options,
} from "amqplib";
import { RABBITMQ, type TraceEvent } from "./contracts.js";
import type { FastifyBaseLogger } from "fastify";
import {
  Consumer,
  Kafka,
  logLevel as kafkaLogLevel,
  type EachMessagePayload,
} from "kafkajs";
import type { EventBuffer } from "./buffer.js";
import type { EventHubConfig } from "./config.js";
import type { EventHubMetrics } from "./metrics.js";
import {
  normalizeKafkaValue,
  observeKafkaOrder,
  observeRabbitResult,
} from "./normalize.js";

export const EVENT_HUB_RESULTS_QUEUE = "flashdrop.event-hub.inventory-results.v1";

function rabbitConnectionOptions(connectionUrl: string): Options.Connect {
  const url = new URL(connectionUrl);
  if (url.protocol !== "amqp:" && url.protocol !== "amqps:") {
    throw new Error("RABBITMQ_URL must use amqp:// or amqps://");
  }
  return {
    protocol: url.protocol.slice(0, -1),
    hostname: url.hostname,
    ...(url.port ? { port: Number(url.port) } : {}),
    ...(url.username || url.password
      ? {
          username: decodeURIComponent(url.username),
          password: decodeURIComponent(url.password),
        }
      : {}),
    vhost: url.pathname.length > 1
      ? decodeURIComponent(url.pathname.slice(1))
      : "/",
    frameMax: 131_072,
    heartbeat: 15,
  };
}

export class EventIngestion {
  private readonly consumer: Consumer;
  private rabbitConnection?: ChannelModel;
  private rabbitChannel?: Channel;
  private rabbitConsumerTag?: string;
  private kafkaReady = false;
  private rabbitReady = false;
  private closing = false;
  private runPromise?: Promise<void>;
  private kafkaConnectPromise?: Promise<void>;
  private kafkaReconnectTimer?: NodeJS.Timeout;
  private kafkaReconnectDelayMs = 1_000;
  private rabbitConnectPromise?: Promise<void>;
  private rabbitReconnectTimer?: NodeJS.Timeout;
  private rabbitReconnectDelayMs = 1_000;
  private inFlight = 0;
  private total = 0;

  constructor(
    private readonly config: EventHubConfig,
    private readonly buffer: EventBuffer,
    private readonly logger: FastifyBaseLogger,
    private readonly metrics: EventHubMetrics,
  ) {
    const kafka = new Kafka({
      clientId: config.kafkaClientId,
      brokers: config.kafkaBrokers,
      connectionTimeout: 5_000,
      requestTimeout: 10_000,
      retry: { retries: 8, initialRetryTime: 250, maxRetryTime: 5_000 },
      logLevel: kafkaLogLevel.NOTHING,
    });
    this.consumer = kafka.consumer({
      groupId: config.kafkaGroupId,
      sessionTimeout: 30_000,
      heartbeatInterval: 3_000,
      rebalanceTimeout: 60_000,
      allowAutoTopicCreation: false,
    });
    this.consumer.on(this.consumer.events.GROUP_JOIN, () => {
      this.kafkaReady = true;
      this.kafkaReconnectDelayMs = 1_000;
    });
    this.consumer.on(this.consumer.events.DISCONNECT, () => {
      this.kafkaReady = false;
      // KafkaJS also emits this during its own restart and our deliberate retry.
    });
    this.consumer.on(this.consumer.events.CRASH, ({ payload }) => {
      this.kafkaReady = false;
      this.logger.error({ err: payload.error }, "Kafka consumer crashed");
      if (!this.closing && !payload.restart) this.scheduleKafkaReconnect();
    });
  }

  async start(): Promise<void> {
    void this.connectKafka().catch((error) => {
      this.logger.error({ err: error }, "Initial Kafka connection failed");
      this.scheduleKafkaReconnect();
    });
    void this.connectRabbit().catch((error) => {
      this.logger.error({ err: error }, "Initial RabbitMQ connection failed");
      this.scheduleRabbitReconnect();
    });
  }

  private connectKafka(): Promise<void> {
    if (this.kafkaConnectPromise) return this.kafkaConnectPromise;
    const operation = this.openKafka().finally(() => {
      if (this.kafkaConnectPromise === operation) this.kafkaConnectPromise = undefined;
    });
    this.kafkaConnectPromise = operation;
    return operation;
  }

  private async openKafka(): Promise<void> {
    await this.consumer.connect();
    await this.consumer.subscribe({
      topics: [this.config.kafkaTraceTopic, this.config.kafkaOrdersTopic],
      fromBeginning: false,
    });
    this.runPromise = this.consumer.run({
      partitionsConsumedConcurrently: 3,
      eachMessage: (payload) => this.handleKafka(payload),
    });
    this.runPromise.catch((error) => {
      this.kafkaReady = false;
      if (!this.closing) {
        this.logger.error({ err: error }, "Kafka consumer stopped");
        this.scheduleKafkaReconnect();
      }
    });
  }

  private scheduleKafkaReconnect(): void {
    if (this.closing || this.kafkaReconnectTimer || this.kafkaConnectPromise) return;
    const delay = this.kafkaReconnectDelayMs;
    this.kafkaReconnectDelayMs = Math.min(this.kafkaReconnectDelayMs * 2, 30_000);
    this.kafkaReconnectTimer = setTimeout(() => {
      this.kafkaReconnectTimer = undefined;
      void this.consumer.disconnect().catch(() => undefined).finally(() => {
        void this.connectKafka().catch((error) => {
          this.logger.error(
            { err: error, retryInMs: this.kafkaReconnectDelayMs },
            "Kafka reconnect failed",
          );
          this.scheduleKafkaReconnect();
        });
      });
    }, delay);
    this.kafkaReconnectTimer.unref();
  }

  private connectRabbit(): Promise<void> {
    if (this.rabbitConnectPromise) return this.rabbitConnectPromise;
    const operation = this.openRabbit().finally(() => {
      if (this.rabbitConnectPromise === operation) this.rabbitConnectPromise = undefined;
    });
    this.rabbitConnectPromise = operation;
    return operation;
  }

  private async openRabbit(): Promise<void> {
    const connection = await amqp.connect(rabbitConnectionOptions(this.config.rabbitUrl), {
      clientProperties: { connection_name: "flashdrop-event-hub" },
      keepAlive: true,
      keepAliveDelay: 5_000,
    });
    const channel = await connection.createChannel();
    await channel.assertExchange(RABBITMQ.exchanges.results, "fanout", { durable: true });
    const queue = await channel.assertQueue(EVENT_HUB_RESULTS_QUEUE, {
      durable: true,
      arguments: { "x-queue-type": "classic" },
    });
    await channel.bindQueue(EVENT_HUB_RESULTS_QUEUE, RABBITMQ.exchanges.results, "");
    await channel.prefetch(50);
    this.rabbitConnection = connection;
    this.rabbitChannel = channel;
    const consumer = await channel.consume(
      EVENT_HUB_RESULTS_QUEUE,
      (message) => this.handleRabbit(message),
      { noAck: false },
    );
    this.rabbitConsumerTag = consumer.consumerTag;
    this.rabbitReady = true;
    this.rabbitReconnectDelayMs = 1_000;
    connection.on("error", (error) => {
      if (this.rabbitConnection === connection) this.rabbitReady = false;
      this.logger.error({ err: error }, "RabbitMQ connection error");
    });
    connection.on("close", () => {
      if (this.rabbitConnection !== connection) return;
      this.rabbitReady = false;
      this.rabbitConnection = undefined;
      this.rabbitChannel = undefined;
      this.rabbitConsumerTag = undefined;
      if (!this.closing) {
        this.logger.warn("RabbitMQ connection closed; reconnect scheduled");
        this.scheduleRabbitReconnect();
      }
    });
  }

  private scheduleRabbitReconnect(): void {
    if (this.closing || this.rabbitReconnectTimer) return;
    const delay = this.rabbitReconnectDelayMs;
    this.rabbitReconnectDelayMs = Math.min(this.rabbitReconnectDelayMs * 2, 30_000);
    this.rabbitReconnectTimer = setTimeout(() => {
      this.rabbitReconnectTimer = undefined;
      void this.connectRabbit().catch((error) => {
        this.logger.error({ err: error, retryInMs: this.rabbitReconnectDelayMs }, "RabbitMQ reconnect failed");
        this.scheduleRabbitReconnect();
      });
    }, delay);
    this.rabbitReconnectTimer.unref();
  }

  private async handleKafka({ topic, message }: EachMessagePayload): Promise<void> {
    this.inFlight += 1;
    try {
      if (!message.value) throw new Error("Kafka record has no value");
      const value = JSON.parse(message.value.toString("utf8")) as unknown;
      const normalized = normalizeKafkaValue(topic, value, {
        traces: this.config.kafkaTraceTopic,
        orders: this.config.kafkaOrdersTopic,
      });
      this.ingest(
        normalized.kind === "trace"
          ? normalized.event
          : observeKafkaOrder(normalized.event),
      );
      this.total += 1;
      this.metrics.ingested.inc({ source: "kafka", outcome: "accepted" });
    } catch (error) {
      this.metrics.ingested.inc({ source: "kafka", outcome: "rejected" });
      this.logger.warn(
        { err: error, topic, partition: message.attributes, offset: message.offset },
        "invalid Kafka event discarded",
      );
    } finally {
      this.inFlight = Math.max(0, this.inFlight - 1);
    }
  }

  private handleRabbit(message: ConsumeMessage | null): void {
    if (!message) {
      this.rabbitReady = false;
      void this.rabbitConnection?.close().catch(() => undefined);
      return;
    }
    const channel = this.rabbitChannel;
    if (!channel) return;
    this.inFlight += 1;
    try {
      const value = JSON.parse(message.content.toString("utf8")) as unknown;
      this.ingest(observeRabbitResult(value));
      this.total += 1;
      this.metrics.ingested.inc({ source: "rabbitmq", outcome: "accepted" });
    } catch (error) {
      this.metrics.ingested.inc({ source: "rabbitmq", outcome: "rejected" });
      this.logger.warn({ err: error }, "invalid RabbitMQ result discarded");
    } finally {
      this.inFlight = Math.max(0, this.inFlight - 1);
      channel.ack(message);
    }
  }

  private ingest(event: TraceEvent): void {
    if (this.buffer.push(event)) {
      this.metrics.bufferSize.set(this.buffer.size);
    }
  }

  isReady(): boolean {
    return this.kafkaReady || this.rabbitReady;
  }

  readiness(): { kafka: boolean; rabbitmq: boolean } {
    return { kafka: this.kafkaReady, rabbitmq: this.rabbitReady };
  }

  runtime(): { inFlight: number; total: number; healthy: boolean } {
    return { inFlight: this.inFlight, total: this.total, healthy: this.isReady() };
  }

  async stop(): Promise<void> {
    this.closing = true;
    this.kafkaReady = false;
    this.rabbitReady = false;
    if (this.kafkaReconnectTimer) clearTimeout(this.kafkaReconnectTimer);
    if (this.rabbitReconnectTimer) clearTimeout(this.rabbitReconnectTimer);
    if (this.rabbitChannel && this.rabbitConsumerTag) {
      await this.rabbitChannel.cancel(this.rabbitConsumerTag).catch(() => undefined);
    }
    await Promise.allSettled([
      this.consumer.stop(),
      this.rabbitChannel?.close(),
      this.rabbitConnection?.close(),
    ]);
    await this.consumer.disconnect().catch(() => undefined);
    await this.runPromise?.catch(() => undefined);
  }
}
