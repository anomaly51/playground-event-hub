import cors from "@fastify/cors";
import Fastify, { type FastifyInstance } from "fastify";
import { z } from "zod";
import type { EventBuffer } from "./buffer.js";
import type { EventHubConfig } from "./config.js";
import type { EventIngestion } from "./ingestion.js";
import type { EventHubMetrics } from "./metrics.js";
import type { OperationsSampler } from "./operations.js";

const recentQuerySchema = z.object({
  limit: z.coerce.number().int().min(1).max(1_000).optional(),
  traceId: z.string().ulid().optional(),
});

const streamQuerySchema = z.object({
  lastEventId: z.string().ulid().optional(),
  traceId: z.string().ulid().optional(),
});

export function sseCorsHeaders(
  origin: string | undefined,
  allowedOrigins: readonly string[],
): Record<string, string> {
  const headers: Record<string, string> = { vary: "Origin" };
  if (allowedOrigins.includes("*")) {
    headers["access-control-allow-origin"] = "*";
  } else if (origin && allowedOrigins.includes(origin)) {
    headers["access-control-allow-origin"] = origin;
  }
  return headers;
}

export async function createApp(options: {
  config: EventHubConfig;
  buffer: EventBuffer;
  ingestion: EventIngestion;
  metrics: EventHubMetrics;
  operations: OperationsSampler;
}): Promise<FastifyInstance> {
  const { config, buffer, ingestion, metrics, operations } = options;
  const app = Fastify({
    logger: { level: config.logLevel },
    requestTimeout: 10_000,
    connectionTimeout: 10_000,
    keepAliveTimeout: 72_000,
    trustProxy: true,
  });
  await app.register(cors, {
    origin: config.corsOrigins.includes("*") ? true : config.corsOrigins,
    methods: ["GET", "OPTIONS"],
    allowedHeaders: ["last-event-id", "cache-control"],
    exposedHeaders: ["content-type"],
    maxAge: 86_400,
  });

  app.get("/healthz", async () => ({ status: "ok" }));
  app.get("/readyz", async (_request, reply) =>
    reply.code(ingestion.isReady() ? 200 : 503).send({
      status: ingestion.isReady() ? "ready" : "not-ready",
      dependencies: ingestion.readiness(),
    }),
  );
  app.get("/metrics", async (_request, reply) => {
    reply.header("content-type", metrics.registry.contentType);
    return metrics.registry.metrics();
  });
  app.get("/operations/snapshot", async (_request, reply) => {
    reply.header("cache-control", "no-store");
    return operations.current();
  });
  app.get("/events/recent", async (request, reply) => {
    const query = recentQuerySchema.safeParse(request.query);
    if (!query.success) {
      return reply.code(400).send({ error: "invalid_query", details: query.error.flatten() });
    }
    const events = buffer.recent(
      query.data.limit ?? config.recentDefaultLimit,
      query.data.traceId,
    );
    reply.header("cache-control", "no-store");
    return { events, count: events.length };
  });

  app.get("/events/stream", async (request, reply) => {
    const query = streamQuerySchema.safeParse(request.query);
    if (!query.success) return reply.code(400).send({ error: "invalid_query" });
    const headerId = request.headers["last-event-id"];
    const lastEventId = query.data.lastEventId
      ?? (typeof headerId === "string" ? headerId : undefined);
    const traceId = query.data.traceId;

    request.raw.socket.setTimeout(0);
    reply.hijack();
    reply.raw.writeHead(200, {
      "content-type": "text/event-stream; charset=utf-8",
      "cache-control": "no-cache, no-transform",
      connection: "keep-alive",
      "x-accel-buffering": "no",
      ...sseCorsHeaders(request.headers.origin, config.corsOrigins),
    });
    reply.raw.write("retry: 3000\n\n");
    metrics.connectSseClient();
    let closed = false;

    const writeEvent = (event: ReturnType<EventBuffer["recent"]>[number]) => {
      if (closed || (traceId && event.traceId !== traceId)) return;
      if (reply.raw.writableLength > config.sseMaxBufferBytes) {
        metrics.droppedSseClients.inc();
        reply.raw.destroy();
        return;
      }
      reply.raw.write(`id: ${event.id}\nevent: trace\ndata: ${JSON.stringify(event)}\n\n`);
    };
    const unsubscribe = buffer.subscribe(writeEvent);
    if (lastEventId) buffer.after(lastEventId).forEach(writeEvent);

    const heartbeat = setInterval(() => {
      if (!closed) reply.raw.write(`: heartbeat ${Date.now()}\n\n`);
    }, config.sseHeartbeatMs);
    heartbeat.unref();

    const cleanup = () => {
      if (closed) return;
      closed = true;
      clearInterval(heartbeat);
      unsubscribe();
      metrics.disconnectSseClient();
    };
    request.raw.once("close", cleanup);
    reply.raw.once("error", cleanup);
  });

  return app;
}
