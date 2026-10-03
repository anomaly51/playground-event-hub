import http from "node:http";
import { describe, expect, it } from "vitest";
import { createApp, sseCorsHeaders } from "../src/app.js";
import { EventBuffer } from "../src/buffer.js";
import type { EventIngestion } from "../src/ingestion.js";
import { EventHubMetrics } from "../src/metrics.js";
import type { OperationsSampler } from "../src/operations.js";
import { eventHubConfig } from "./fixtures.js";

const config = eventHubConfig();

describe("SSE CORS headers", () => {
  it("echoes an explicitly allowed local-development origin", () => {
    expect(
      sseCorsHeaders("http://localhost:5173", [
        "http://localhost:5173",
        "http://localhost:4173",
      ]),
    ).toEqual({
      "access-control-allow-origin": "http://localhost:5173",
      vary: "Origin",
    });
  });

  it("does not authorize an unknown origin", () => {
    expect(
      sseCorsHeaders("https://attacker.example", ["http://localhost:5173"]),
    ).toEqual({ vary: "Origin" });
  });

  it("supports the explicit wildcard policy", () => {
    expect(sseCorsHeaders("https://example.test", ["*"])).toEqual({
      "access-control-allow-origin": "*",
      vary: "Origin",
    });
  });

  it("writes the allowed origin on the real hijacked SSE response", async () => {
    const ingestion = {
      isReady: () => true,
      readiness: () => ({ kafka: true, rabbitmq: true }),
    } as unknown as EventIngestion;
    const operations = {
      current: () => ({ fresh: true }),
    } as unknown as OperationsSampler;
    const app = await createApp({
      config,
      buffer: new EventBuffer(10),
      ingestion,
      metrics: new EventHubMetrics(),
      operations,
    });
    const address = await app.listen({ host: "127.0.0.1", port: 0 });
    try {
      const headers = await new Promise<http.IncomingHttpHeaders>((resolve, reject) => {
        let received = false;
        const request = http.get(
          `${address}/events/stream`,
          { headers: { origin: "http://localhost:5173" } },
          (response) => {
            received = true;
            resolve(response.headers);
            response.destroy();
          },
        );
        request.setTimeout(2_000, () => request.destroy(new Error("SSE test timed out")));
        request.on("error", (error) => {
          if (!received) reject(error);
        });
      });
      expect(headers["access-control-allow-origin"]).toBe("http://localhost:5173");
      expect(headers.vary).toContain("Origin");
    } finally {
      await app.close();
    }
  });
});
