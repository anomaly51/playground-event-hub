import { Counter, Gauge, Registry, collectDefaultMetrics } from "@prometheus-io/client";

export class EventHubMetrics {
  private connectedSseClients = 0;
  readonly registry = new Registry();
  readonly ingested = new Counter({
    name: "lab_event_hub_ingested_total",
    help: "Events ingested by source and outcome",
    labelNames: ["source", "outcome"] as const,
    registers: [this.registry],
  });
  readonly sseClients = new Gauge({
    name: "lab_event_hub_sse_clients",
    help: "Currently connected SSE clients",
    registers: [this.registry],
  });
  readonly droppedSseClients = new Counter({
    name: "lab_event_hub_sse_clients_dropped_total",
    help: "Slow SSE clients closed to bound server memory",
    registers: [this.registry],
  });
  readonly bufferSize = new Gauge({
    name: "lab_event_hub_buffer_events",
    help: "Trace events currently retained in memory",
    registers: [this.registry],
  });
  readonly kafkaLag = new Gauge({
    name: "flashdrop_event_hub_kafka_lag",
    help: "Latest sampled analytics consumer lag, when known",
    registers: [this.registry],
  });
  readonly rabbitMessages = new Gauge({
    name: "flashdrop_event_hub_rabbit_messages",
    help: "Latest sampled RabbitMQ messages by state",
    labelNames: ["state"] as const,
    registers: [this.registry],
  });
  readonly operationsFresh = new Gauge({
    name: "flashdrop_event_hub_operations_snapshot_fresh",
    help: "Whether the operational snapshot is within its freshness window",
    registers: [this.registry],
  });

  constructor() {
    collectDefaultMetrics({ register: this.registry, prefix: "lab_event_hub_process_" });
  }

  connectSseClient(): void {
    this.connectedSseClients += 1;
    this.sseClients.set(this.connectedSseClients);
  }

  disconnectSseClient(): void {
    this.connectedSseClients = Math.max(0, this.connectedSseClients - 1);
    this.sseClients.set(this.connectedSseClients);
  }

  get sseClientCount(): number {
    return this.connectedSseClients;
  }
}
