import { pool } from "./db.js";

function createAggregateId() {
  return `todo-${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

export function replayTodo(events) {
  let todo = null;

  for (const event of events) {
    if (event.event_type === "TodoCreated") {
      todo = {
        id: event.aggregate_id,
        title: event.payload.title,
        completed: false,
        createdAt: event.created_at,
        updatedAt: event.created_at,
        version: event.event_version
      };
    }

    if (event.event_type === "TodoCompleted" && todo) {
      todo.completed = true;
      todo.updatedAt = event.created_at;
      todo.version = event.event_version;
    }

    if (event.event_type === "TodoReopened" && todo) {
      todo.completed = false;
      todo.updatedAt = event.created_at;
      todo.version = event.event_version;
    }
  }

  return todo;
}

export async function appendTodoEvent({ aggregateId, eventType, payload }) {
  const client = await pool.connect();

  try {
    await client.query("begin");
    await client.query("select pg_advisory_xact_lock(hashtext($1))", [aggregateId]);

    const versionResult = await client.query(
      `select coalesce(max(event_version), 0) + 1 as next_version
       from todo_event_store
       where aggregate_id = $1`,
      [aggregateId]
    );
    const nextVersion = versionResult.rows[0].next_version;

    const insertResult = await client.query(
      `insert into todo_event_store (
         aggregate_id,
         aggregate_type,
         event_type,
         event_version,
         payload
       )
       values ($1, 'Todo', $2, $3, $4)
       returning id, aggregate_id, aggregate_type, event_type, event_version, payload, created_at`,
      [aggregateId, eventType, nextVersion, payload]
    );

    await client.query("commit");
    return insertResult.rows[0];
  } catch (error) {
    await client.query("rollback");
    throw error;
  } finally {
    client.release();
  }
}

export async function createEventSourcedTodo(title) {
  const aggregateId = createAggregateId();

  const event = await appendTodoEvent({
    aggregateId,
    eventType: "TodoCreated",
    payload: {
      title
    }
  });

  return {
    event,
    todo: replayTodo([event])
  };
}

export async function getTodoEvents(aggregateId) {
  const result = await pool.query(
    `select id, aggregate_id, aggregate_type, event_type, event_version, payload, created_at
     from todo_event_store
     where aggregate_id = $1
     order by event_version`,
    [aggregateId]
  );

  return result.rows;
}

export async function getEventSourcedTodo(aggregateId) {
  const events = await getTodoEvents(aggregateId);

  return {
    events,
    todo: replayTodo(events)
  };
}

export async function listEventSourcedTodos() {
  const result = await pool.query(
    `select id, aggregate_id, aggregate_type, event_type, event_version, payload, created_at
     from todo_event_store
     order by aggregate_id, event_version`
  );
  const grouped = new Map();

  for (const event of result.rows) {
    const events = grouped.get(event.aggregate_id) || [];
    events.push(event);
    grouped.set(event.aggregate_id, events);
  }

  return Array.from(grouped.values())
    .map((events) => replayTodo(events))
    .filter(Boolean);
}
