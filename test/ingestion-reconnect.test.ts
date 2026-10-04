import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const kafka = vi.hoisted(() => ({
  listeners: new Map<string, (event: unknown) => void>(),
  consumer: {
    events: { GROUP_JOIN: "GROUP_JOIN", DISCONNECT: "DISCONNECT", CRASH: "CRASH" },
    on: vi.fn(),
    connect: vi.fn(),
    subscribe: vi.fn(),
    run: vi.fn(),
    disconnect: vi.fn(),
    stop: vi.fn(),
  },
}));

vi.mock("kafkajs", () => ({
  Kafka: class Kafka {
    consumer() { return kafka.consumer; }
  },
  logLevel: { NOTHING: 0 },
}));

import { EventBuffer } from "../src/buffer.js";
import { EventIngestion } from "../src/ingestion.js";
import { EventHubMetrics } from "../src/metrics.js";
import { eventHubConfig } from "./fixtures.js";

function emit(event: string, payload = {}): void {
  kafka.listeners.get(event)?.({ payload });
}

describe("Kafka ingestion reconnect ownership", () => {
  let ingestion: EventIngestion;

  beforeEach(() => {
    vi.useFakeTimers();
    vi.resetAllMocks();
    kafka.listeners.clear();
    kafka.consumer.on.mockImplementation((event: string, listener: (event: unknown) => void) => {
      kafka.listeners.set(event, listener);
    });
    kafka.consumer.connect.mockResolvedValue(undefined);
    kafka.consumer.subscribe.mockResolvedValue(undefined);
    kafka.consumer.run.mockImplementation(async () => { emit("GROUP_JOIN"); });
    kafka.consumer.disconnect.mockImplementation(async () => { emit("DISCONNECT"); });
    kafka.consumer.stop.mockResolvedValue(undefined);
    ingestion = new EventIngestion(
      eventHubConfig(),
      new EventBuffer(10),
      { error: vi.fn(), warn: vi.fn(), info: vi.fn() } as never,
      new EventHubMetrics(),
    );
    vi.spyOn(ingestion as unknown as { connectRabbit: () => Promise<void> }, "connectRabbit")
      .mockResolvedValue(undefined);
  });

  afterEach(async () => {
    await ingestion.stop();
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  async function start(): Promise<void> {
    await ingestion.start();
    await vi.advanceTimersByTimeAsync(0);
  }

  it("recovers from bootstrap failure without disconnecting again after joining", async () => {
    kafka.consumer.connect.mockRejectedValueOnce(new Error("GroupCoordinatorNotFound"));
    await start();
    expect(ingestion.readiness().kafka).toBe(false);

    await vi.advanceTimersByTimeAsync(1_000);
    expect(ingestion.readiness().kafka).toBe(true);
    expect(kafka.consumer.connect).toHaveBeenCalledTimes(2);
    expect(kafka.consumer.disconnect).toHaveBeenCalledOnce();
    await vi.advanceTimersByTimeAsync(60_000);
    expect(kafka.consumer.connect).toHaveBeenCalledTimes(2);
    expect(kafka.consumer.disconnect).toHaveBeenCalledOnce();
  });

  it("lets KafkaJS own a retriable crash and its native restart", async () => {
    await start();
    // KafkaJS onCrash disconnects before it emits CRASH and restarts internally.
    await kafka.consumer.disconnect();
    emit("CRASH", { error: new Error("transient broker failure"), restart: true });
    expect(ingestion.readiness().kafka).toBe(false);
    await vi.advanceTimersByTimeAsync(30_000);
    expect(kafka.consumer.connect).toHaveBeenCalledOnce();
    expect(kafka.consumer.run).toHaveBeenCalledOnce();
    expect(kafka.consumer.disconnect).toHaveBeenCalledOnce();

    emit("GROUP_JOIN");
    expect(ingestion.readiness().kafka).toBe(true);
    await vi.advanceTimersByTimeAsync(60_000);
    expect(kafka.consumer.disconnect).toHaveBeenCalledOnce();
  });

  it("retries once when KafkaJS declines to restart a crashed consumer", async () => {
    await start();
    await kafka.consumer.disconnect();
    emit("CRASH", { error: new Error("consumer stopped"), restart: false });
    await vi.advanceTimersByTimeAsync(999);
    expect(kafka.consumer.connect).toHaveBeenCalledOnce();
    await vi.advanceTimersByTimeAsync(1);
    expect(kafka.consumer.connect).toHaveBeenCalledTimes(2);
    expect(kafka.consumer.disconnect).toHaveBeenCalledTimes(2);
    expect(ingestion.readiness().kafka).toBe(true);
    await vi.advanceTimersByTimeAsync(60_000);
    expect(kafka.consumer.connect).toHaveBeenCalledTimes(2);
    expect(kafka.consumer.disconnect).toHaveBeenCalledTimes(2);
  });

  it("backs off across repeated bootstrap failures and stays connected after recovery", async () => {
    kafka.consumer.connect
      .mockRejectedValueOnce(new Error("broker unavailable"))
      .mockRejectedValueOnce(new Error("broker unavailable"))
      .mockRejectedValueOnce(new Error("broker unavailable"));
    await start();
    let attempts = 1;
    for (const delay of [1_000, 2_000, 4_000]) {
      await vi.advanceTimersByTimeAsync(delay - 1);
      expect(kafka.consumer.connect).toHaveBeenCalledTimes(attempts);
      await vi.advanceTimersByTimeAsync(1);
      expect(kafka.consumer.connect).toHaveBeenCalledTimes(++attempts);
    }
    expect(ingestion.readiness().kafka).toBe(true);
    await vi.advanceTimersByTimeAsync(60_000);
    expect(kafka.consumer.connect).toHaveBeenCalledTimes(4);
    expect(kafka.consumer.disconnect).toHaveBeenCalledTimes(3);
  });

  it("does not retry after shutdown during failed startup", async () => {
    kafka.consumer.connect.mockRejectedValueOnce(new Error("broker unavailable"));
    await start();
    await ingestion.stop();
    emit("CRASH", { error: new Error("stopping"), restart: false });
    await vi.advanceTimersByTimeAsync(60_000);
    expect(kafka.consumer.connect).toHaveBeenCalledOnce();
    expect(kafka.consumer.disconnect).toHaveBeenCalledOnce();
    expect(ingestion.readiness().kafka).toBe(false);
  });

  it("preserves readiness when only the RabbitMQ transport is available", () => {
    (ingestion as unknown as { rabbitReady: boolean }).rabbitReady = true;
    expect(ingestion.readiness()).toEqual({ kafka: false, rabbitmq: true });
    expect(ingestion.isReady()).toBe(true);
  });
});
