import pino from "pino";
import { pool } from "./db.js";
import { publishClaimedOutboxEvents } from "./outbox.js";

const pollIntervalMs = Number(process.env.OUTBOX_POLL_INTERVAL_MS || 3000);
const batchSize = Number(process.env.OUTBOX_BATCH_SIZE || 10);
const logger = pino({
  base: {
    service: "todo-backend-outbox-worker"
  }
});

function logEvent(event, fields = {}) {
  logger.info({
    event,
    timestamp: new Date().toISOString(),
    ...fields
  });
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

let shouldStop = false;

process.on("SIGTERM", () => {
  shouldStop = true;
});

process.on("SIGINT", () => {
  shouldStop = true;
});

logEvent("outbox_worker_started", {
  pollIntervalMs,
  batchSize
});

while (!shouldStop) {
  try {
    const count = await publishClaimedOutboxEvents({
      logEvent,
      batchSize
    });

    if (count === 0) {
      await sleep(pollIntervalMs);
    }
  } catch (error) {
    logEvent("outbox_worker_loop_failed", {
      error: error.message
    });
    await sleep(pollIntervalMs);
  }
}

logEvent("outbox_worker_stopped");
await pool.end();
