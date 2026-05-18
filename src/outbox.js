import { pool } from "./db.js";
import { publishIntegrationEvent } from "./events.js";

export async function listOutboxEvents(limit = 50) {
  const result = await pool.query(
    `select id, event_id, aggregate_id, event_type, routing_key, payload,
            attempts, locked_at, published_at, last_error, created_at
     from outbox_events
     order by id desc
     limit $1`,
    [limit]
  );

  return result.rows;
}

export async function claimOutboxEvents(limit = 10) {
  const result = await pool.query(
    `with claimed as (
       select id
       from outbox_events
       where published_at is null
         and (
           locked_at is null
           or locked_at < now() - interval '2 minutes'
         )
       order by id
       limit $1
       for update skip locked
     )
     update outbox_events
     set locked_at = now(),
         attempts = attempts + 1
     where id in (select id from claimed)
     returning id, event_id, aggregate_id, event_type, routing_key, payload, attempts`,
    [limit]
  );

  return result.rows;
}

export async function markOutboxEventPublished(id) {
  await pool.query(
    `update outbox_events
     set published_at = now(),
         locked_at = null,
         last_error = null
     where id = $1`,
    [id]
  );
}

export async function markOutboxEventFailed(id, error) {
  await pool.query(
    `update outbox_events
     set locked_at = null,
         last_error = $2
     where id = $1`,
    [id, error.message]
  );
}

export async function publishClaimedOutboxEvents({ logEvent = () => {}, batchSize = 10 } = {}) {
  const events = await claimOutboxEvents(batchSize);

  for (const event of events) {
    try {
      const publishResult = await publishIntegrationEvent({
        eventId: event.event_id,
        eventType: event.event_type,
        routingKey: event.routing_key,
        payload: event.payload
      });

      if (!publishResult.published) {
        throw new Error(publishResult.reason || "publish_returned_false");
      }

      await markOutboxEventPublished(event.id);

      logEvent("outbox_event_published", {
        outboxId: event.id,
        eventId: event.event_id,
        eventType: event.event_type,
        routingKey: event.routing_key,
        attempts: event.attempts
      });
    } catch (error) {
      await markOutboxEventFailed(event.id, error);

      logEvent("outbox_event_publish_failed", {
        outboxId: event.id,
        eventId: event.event_id,
        eventType: event.event_type,
        routingKey: event.routing_key,
        attempts: event.attempts,
        error: error.message
      });
    }
  }

  return events.length;
}
