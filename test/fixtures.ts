import type { EventHubConfig } from "../src/config.js";

export function eventHubConfig(
  overrides: Partial<EventHubConfig> = {},
): EventHubConfig {
  return {
    logLevel: "silent",
    host: "127.0.0.1",
    port: 3_003,
    corsOrigins: ["http://localhost:5173"],
    kafkaBrokers: ["unused:9092"],
    kafkaClientId: "event-hub-test",
    kafkaGroupId: "event-hub-test-v1",
    kafkaOrdersTopic: "flashdrop.orders.v1",
    kafkaTraceTopic: "flashdrop.traces.v1",
    kafkaAnalyticsGroupId: "flashdrop-analytics-v1",
    rabbitUrl: "amqp://guest:guest@unused:5672",
    rabbitManagementUrl: "http://rabbit-management.test",
    rabbitManagementUser: "guest",
    rabbitManagementPassword: "guest",
    gatewayOperationsUrl: "http://gateway.test/operations/snapshot",
    relayOperationsUrl: "http://relay.test/operations/snapshot",
    pricingOperationsUrl: "http://pricing.test/operations/snapshot",
    analyticsOperationsUrl: "http://analytics.test/operations/snapshot",
    inventoryOperationsUrl: "http://inventory.test/operations/snapshot",
    airflowHealthUrl: "http://airflow.test/api/v2/monitor/health",
    operationsSampleIntervalMs: 2_000,
    operationsRequestTimeoutMs: 1_500,
    operationsStaleAfterMs: 6_000,
    eventBufferSize: 10,
    recentDefaultLimit: 10,
    sseHeartbeatMs: 60_000,
    sseMaxBufferBytes: 65_536,
    ...overrides,
  };
}
