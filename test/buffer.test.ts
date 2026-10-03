import type { TraceEvent } from "../src/contracts.js";
import { describe, expect, it, vi } from "vitest";
import { EventBuffer } from "../src/buffer.js";

function event(id: string, traceId = "01ARZ3NDEKTSV4RRFFQ69G5FAV"): TraceEvent {
  return {
    id,
    traceId,
    timestamp: "2026-08-28T10:00:00.000Z",
    source: "test",
    target: "event-hub",
    transport: "kafka",
    stage: "test",
    status: "succeeded",
    summary: "Test event",
  };
}

describe("EventBuffer", () => {
  it("stays bounded and replays from a known event", () => {
    const buffer = new EventBuffer(2);
    buffer.push(event("01ARZ3NDEKTSV4RRFFQ69G5FAA"));
    buffer.push(event("01ARZ3NDEKTSV4RRFFQ69G5FAB"));
    buffer.push(event("01ARZ3NDEKTSV4RRFFQ69G5FAC"));
    expect(buffer.recent(10).map(({ id }) => id)).toEqual([
      "01ARZ3NDEKTSV4RRFFQ69G5FAB",
      "01ARZ3NDEKTSV4RRFFQ69G5FAC",
    ]);
    expect(buffer.after("01ARZ3NDEKTSV4RRFFQ69G5FAB")).toHaveLength(1);
  });

  it("deduplicates ids and notifies subscribers", () => {
    const buffer = new EventBuffer(10);
    const listener = vi.fn();
    const unsubscribe = buffer.subscribe(listener);
    const item = event("01ARZ3NDEKTSV4RRFFQ69G5FAA");
    expect(buffer.push(item)).toBe(true);
    expect(buffer.push(item)).toBe(false);
    unsubscribe();
    expect(listener).toHaveBeenCalledOnce();
  });
});
