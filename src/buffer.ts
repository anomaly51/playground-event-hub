import { EventEmitter } from "node:events";
import type { TraceEvent } from "./contracts.js";

export type EventListener = (event: TraceEvent) => void;

export class EventBuffer {
  private events: TraceEvent[] = [];
  private readonly ids = new Set<string>();
  private readonly emitter = new EventEmitter();

  constructor(private readonly capacity: number) {
    if (!Number.isSafeInteger(capacity) || capacity < 1) {
      throw new Error("Event buffer capacity must be a positive integer");
    }
    this.emitter.setMaxListeners(0);
  }

  push(event: TraceEvent): boolean {
    if (this.ids.has(event.id)) return false;
    this.events.push(event);
    this.ids.add(event.id);
    while (this.events.length > this.capacity) {
      const removed = this.events.shift();
      if (removed) this.ids.delete(removed.id);
    }
    this.emitter.emit("event", event);
    return true;
  }

  recent(limit: number, traceId?: string): TraceEvent[] {
    const filtered = traceId
      ? this.events.filter((event) => event.traceId === traceId)
      : this.events;
    return filtered.slice(-Math.max(0, limit));
  }

  after(eventId: string, limit = this.capacity): TraceEvent[] {
    const index = this.events.findIndex((event) => event.id === eventId);
    return index < 0 ? [] : this.events.slice(index + 1, index + 1 + limit);
  }

  subscribe(listener: EventListener): () => void {
    this.emitter.on("event", listener);
    return () => this.emitter.off("event", listener);
  }

  get size(): number {
    return this.events.length;
  }
}
