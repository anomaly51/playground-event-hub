import { createApp } from "./app.js";
import { EventBuffer } from "./buffer.js";
import { loadConfig } from "./config.js";
import { EventIngestion } from "./ingestion.js";
import { EventHubMetrics } from "./metrics.js";
import { OperationsSampler } from "./operations.js";
import pino from "pino";

const config = loadConfig();
const buffer = new EventBuffer(config.eventBufferSize);
const metrics = new EventHubMetrics();
const bootstrap = pino({ name: "event-hub", level: config.logLevel });
const ingestion = new EventIngestion(config, buffer, bootstrap, metrics);
await ingestion.start();
const operations = new OperationsSampler(
  config,
  bootstrap,
  metrics,
  () => ingestion.runtime(),
);
await operations.start();
const app = await createApp({ config, buffer, ingestion, metrics, operations });
let shuttingDown = false;

async function shutdown(signal: string): Promise<void> {
  if (shuttingDown) return;
  shuttingDown = true;
  app.log.info({ signal }, "graceful shutdown started");
  const forceExit = setTimeout(() => process.exit(1), 15_000).unref();
  await app.close();
  await operations.stop();
  await ingestion.stop();
  clearTimeout(forceExit);
  process.exit(0);
}

process.once("SIGTERM", () => void shutdown("SIGTERM"));
process.once("SIGINT", () => void shutdown("SIGINT"));
await app.listen({ host: config.host, port: config.port });
