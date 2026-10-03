import { z } from "zod";

const schema = z.object({
  LOG_LEVEL: z.string().default("info"),
  HOST: z.string().default("0.0.0.0"),
  PORT: z.coerce.number().int().min(1).max(65_535).default(3_003),
  CORS_ORIGINS: z.string().default("http://localhost:5173,http://localhost:4173"),
  KAFKA_BROKERS: z.string().default("kafka:29092"),
  KAFKA_CLIENT_ID: z.string().default("flashdrop-event-hub"),
  KAFKA_GROUP_ID: z.string().default("flashdrop-event-hub-v1"),
  KAFKA_ORDERS_TOPIC: z.string().default("flashdrop.orders.v1"),
  KAFKA_TRACE_TOPIC: z.string().default("flashdrop.traces.v1"),
  KAFKA_ANALYTICS_GROUP_ID: z.string().default("flashdrop-analytics-v1"),
  RABBITMQ_URL: z.string().default("amqp://guest:guest@rabbitmq:5672"),
  RABBITMQ_MANAGEMENT_URL: z.string().default("http://rabbitmq:15672"),
  RABBITMQ_MANAGEMENT_USER: z.string().optional(),
  RABBITMQ_MANAGEMENT_PASSWORD: z.string().optional(),
  GATEWAY_OPERATIONS_URL: z.string().default("http://gateway:3000/operations/snapshot"),
  RELAY_OPERATIONS_URL: z.string().default("http://outbox-relay:3005/operations/snapshot"),
  PRICING_OPERATIONS_URL: z.string().default("http://processor:8001/operations/snapshot"),
  ANALYTICS_OPERATIONS_URL: z.string().default("http://analytics:8002/operations/snapshot"),
  INVENTORY_OPERATIONS_URL: z.string().default("http://rabbit-worker:3004/operations/snapshot"),
  AIRFLOW_HEALTH_URL: z.string().default(""),
  OPERATIONS_SAMPLE_INTERVAL_MS: z.coerce.number().int().min(500).max(60_000).default(2_000),
  OPERATIONS_REQUEST_TIMEOUT_MS: z.coerce.number().int().min(100).max(10_000).default(1_500),
  OPERATIONS_STALE_AFTER_MS: z.coerce.number().int().min(1_000).max(300_000).default(6_000),
  EVENT_BUFFER_SIZE: z.coerce.number().int().min(10).max(100_000).default(2_000),
  RECENT_DEFAULT_LIMIT: z.coerce.number().int().min(1).max(1_000).default(200),
  SSE_HEARTBEAT_MS: z.coerce.number().int().min(1_000).max(60_000).default(15_000),
  SSE_MAX_BUFFER_BYTES: z.coerce.number().int().min(16_384).default(1_048_576),
});

export interface EventHubConfig {
  logLevel: string;
  host: string;
  port: number;
  corsOrigins: string[];
  kafkaBrokers: string[];
  kafkaClientId: string;
  kafkaGroupId: string;
  kafkaOrdersTopic: string;
  kafkaTraceTopic: string;
  kafkaAnalyticsGroupId: string;
  rabbitUrl: string;
  rabbitManagementUrl: string;
  rabbitManagementUser: string;
  rabbitManagementPassword: string;
  gatewayOperationsUrl: string;
  relayOperationsUrl: string;
  pricingOperationsUrl: string;
  analyticsOperationsUrl: string;
  inventoryOperationsUrl: string;
  airflowHealthUrl?: string;
  operationsSampleIntervalMs: number;
  operationsRequestTimeoutMs: number;
  operationsStaleAfterMs: number;
  eventBufferSize: number;
  recentDefaultLimit: number;
  sseHeartbeatMs: number;
  sseMaxBufferBytes: number;
}

export function loadConfig(environment: NodeJS.ProcessEnv = process.env): EventHubConfig {
  const parsed = schema.parse(environment);
  const rabbit = new URL(parsed.RABBITMQ_URL);
  return {
    logLevel: parsed.LOG_LEVEL,
    host: parsed.HOST,
    port: parsed.PORT,
    corsOrigins: parsed.CORS_ORIGINS.split(",").map((entry) => entry.trim()).filter(Boolean),
    kafkaBrokers: parsed.KAFKA_BROKERS.split(",").map((entry) => entry.trim()).filter(Boolean),
    kafkaClientId: parsed.KAFKA_CLIENT_ID,
    kafkaGroupId: parsed.KAFKA_GROUP_ID,
    kafkaOrdersTopic: parsed.KAFKA_ORDERS_TOPIC,
    kafkaTraceTopic: parsed.KAFKA_TRACE_TOPIC,
    kafkaAnalyticsGroupId: parsed.KAFKA_ANALYTICS_GROUP_ID,
    rabbitUrl: parsed.RABBITMQ_URL,
    rabbitManagementUrl: parsed.RABBITMQ_MANAGEMENT_URL.replace(/\/$/, ""),
    rabbitManagementUser: parsed.RABBITMQ_MANAGEMENT_USER
      ?? decodeURIComponent(rabbit.username),
    rabbitManagementPassword: parsed.RABBITMQ_MANAGEMENT_PASSWORD
      ?? decodeURIComponent(rabbit.password),
    gatewayOperationsUrl: parsed.GATEWAY_OPERATIONS_URL,
    relayOperationsUrl: parsed.RELAY_OPERATIONS_URL,
    pricingOperationsUrl: parsed.PRICING_OPERATIONS_URL,
    analyticsOperationsUrl: parsed.ANALYTICS_OPERATIONS_URL,
    inventoryOperationsUrl: parsed.INVENTORY_OPERATIONS_URL,
    ...(parsed.AIRFLOW_HEALTH_URL ? { airflowHealthUrl: parsed.AIRFLOW_HEALTH_URL } : {}),
    operationsSampleIntervalMs: parsed.OPERATIONS_SAMPLE_INTERVAL_MS,
    operationsRequestTimeoutMs: parsed.OPERATIONS_REQUEST_TIMEOUT_MS,
    operationsStaleAfterMs: parsed.OPERATIONS_STALE_AFTER_MS,
    eventBufferSize: parsed.EVENT_BUFFER_SIZE,
    recentDefaultLimit: parsed.RECENT_DEFAULT_LIMIT,
    sseHeartbeatMs: parsed.SSE_HEARTBEAT_MS,
    sseMaxBufferBytes: parsed.SSE_MAX_BUFFER_BYTES,
  };
}
