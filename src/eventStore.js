import { pool } from "./db.js";

function createAggregateId() {
  return `todo-${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

function toIntegrationEvent({ event, payload }) {
  const integrationEventTypeByDomainEvent = {
    TodoCreated: "todo.created",
    TodoCompleted: "todo.completed",
    TodoReopened: "todo.reopened"
  };
  const eventType = integrationEventTypeByDomainEvent[event.event_type];

  if (!eventType) {
    throw new Error(`No integration event mapping for ${event.event_type}`);
  }

  return {
    eventId: `${event.aggregate_id}-${event.event_version}`,
    aggregateId: event.aggregate_id,
    eventType,
    routingKey: eventType,
    payload: {
      eventId: `${event.aggregate_id}-${event.event_version}`,
      eventType,
      version: event.event_version,
      occurredAt: event.created_at,
      source: "todo-backend",
      data: {
        todoId: event.aggregate_id,
        ...payload
      }
    }
  };
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
    const event = insertResult.rows[0];
    const integrationEvent = toIntegrationEvent({
      event,
      payload
    });

    await client.query(
      `insert into outbox_events (
         event_id,
         aggregate_id,
         event_type,
         routing_key,
         payload
       )
       values ($1, $2, $3, $4, $5)
       on conflict (event_id) do nothing`,
      [
        integrationEvent.eventId,
        integrationEvent.aggregateId,
        integrationEvent.eventType,
        integrationEvent.routingKey,
        integrationEvent.payload
      ]
    );

    await client.query("commit");
    return event;
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
