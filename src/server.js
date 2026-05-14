import cors from "cors";
import express from "express";
import pino from "pino";
import { redis, getRedisStatus } from "./cache.js";
import { pool } from "./db.js";

const app = express();
const port = Number(process.env.PORT || 3000);
const serviceBaseUrl = process.env.SERVICE_BASE_URL || `http://127.0.0.1:${port}`;
const notificationServiceUrl = process.env.NOTIFICATION_SERVICE_URL || "http://notification:3001";
const logger = pino({
  base: {
    service: "todo-backend"
  }
});

app.use(cors());
app.use(express.json());

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

app.use((req, res, next) => {
  const startedAt = Date.now();

  res.on("finish", () => {
    logEvent("http_request", {
      method: req.method,
      path: req.originalUrl,
      statusCode: res.statusCode,
      durationMs: Date.now() - startedAt
    });
  });

  next();
});

app.get("/api/health", async (req, res) => {
  const redisStatus = await getRedisStatus();

  res.json({
    status: "ok",
    redis: redisStatus
  });
});

app.get("/api/todos", async (req, res) => {
  const result = await pool.query(
    "select id, title, completed, created_at, updated_at from todos order by id"
  );

  res.json({
    todos: result.rows
  });
});

app.post("/api/todos", async (req, res) => {
  const title = String(req.body?.title || "").trim();

  if (!title) {
    return res.status(400).json({
      error: "title is required"
    });
  }

  const result = await pool.query(
    "insert into todos (title) values ($1) returning id, title, completed, created_at, updated_at",
    [title]
  );

  if (redis) {
    await redis.del("todos:count").catch(() => {});
  }

  return res.status(201).json({
    todo: result.rows[0]
  });
});

app.patch("/api/todos/:id", async (req, res) => {
  const id = Number(req.params.id);
  const completed = Boolean(req.body?.completed);

  const result = await pool.query(
    `update todos
     set completed = $1, updated_at = now()
     where id = $2
     returning id, title, completed, created_at, updated_at`,
    [completed, id]
  );

  if (result.rowCount === 0) {
    return res.status(404).json({
      error: "todo not found"
    });
  }

  return res.json({
    todo: result.rows[0]
  });
});

app.delete("/api/todos/:id", async (req, res) => {
  const id = Number(req.params.id);
  const result = await pool.query("delete from todos where id = $1", [id]);

  if (result.rowCount === 0) {
    return res.status(404).json({
      error: "todo not found"
    });
  }

  return res.status(204).send();
});

app.get("/api/stats", async (req, res) => {
  const result = await pool.query("select count(*)::int as total from todos");
  const total = result.rows[0].total;

  if (redis) {
    await redis.set("todos:count", total).catch(() => {});
  }

  res.json({
    total
  });
});

app.post("/api/trace-demo", async (req, res, next) => {
  const startedAt = Date.now();
  const title = String(req.body?.title || `trace-demo-${Date.now()}`).trim();

  try {
    logEvent("trace_demo_started", {
      titleLength: title.length
    });

    await sleep(80);

    const created = await pool.query(
      "insert into todos (title) values ($1) returning id, title, completed, created_at, updated_at",
      [title]
    );

    if (redis) {
      await redis.set(`trace-demo:todo:${created.rows[0].id}`, title, "EX", 300).catch((error) => {
        logEvent("trace_demo_redis_error", {
          todoId: created.rows[0].id,
          error: error.message
        });
      });
    }

    const statsResponse = await fetch(`${serviceBaseUrl}/api/stats`);
    const stats = await statsResponse.json();
    const notificationResponse = await fetch(`${notificationServiceUrl}/api/notify`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        todoId: created.rows[0].id,
        title,
        channel: "datadog-trace-demo"
      })
    });
    const notification = await notificationResponse.json();

    logEvent("trace_demo_completed", {
      todoId: created.rows[0].id,
      total: stats.total,
      notificationId: notification.notificationId,
      durationMs: Date.now() - startedAt
    });

    res.status(201).json({
      todo: created.rows[0],
      stats,
      notification,
      demo: {
        purpose: "Generate Datadog APM spans and searchable structured logs",
        expectedSignals: ["express request", "postgres query", "redis command", "backend to notification HTTP call"]
      }
    });
  } catch (error) {
    logEvent("trace_demo_failed", {
      error: error.message,
      durationMs: Date.now() - startedAt
    });

    next(error);
  }
});

app.use((error, req, res, next) => {
  logEvent("http_error", {
    method: req.method,
    path: req.originalUrl,
    error: error.message
  });

  res.status(500).json({
    error: "internal server error"
  });
});

app.listen(port, "0.0.0.0", () => {
  logEvent("service_started", {
    port
  });
});
