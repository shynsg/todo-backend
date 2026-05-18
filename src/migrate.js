import { pool } from "./db.js";

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function waitForDatabase() {
  for (let attempt = 1; attempt <= 30; attempt += 1) {
    try {
      await pool.query("select 1");
      return;
    } catch (error) {
      console.log(`Waiting for database (${attempt}/30): ${error.message}`);
      await sleep(2000);
    }
  }

  throw new Error("Database did not become ready in time");
}

async function migrate() {
  await waitForDatabase();

  await pool.query(`
    create table if not exists todos (
      id serial primary key,
      title text not null,
      completed boolean not null default false,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now()
    );
  `);

  await pool.query(`
    create table if not exists todo_event_store (
      id bigserial primary key,
      aggregate_id text not null,
      aggregate_type text not null,
      event_type text not null,
      event_version integer not null,
      payload jsonb not null,
      created_at timestamptz not null default now(),
      unique (aggregate_id, event_version)
    );
  `);

  await pool.query(`
    create index if not exists todo_event_store_aggregate_idx
    on todo_event_store (aggregate_id, event_version);
  `);

  await pool.query(`
    create table if not exists outbox_events (
      id bigserial primary key,
      event_id text not null unique,
      aggregate_id text not null,
      event_type text not null,
      routing_key text not null,
      payload jsonb not null,
      attempts integer not null default 0,
      locked_at timestamptz,
      published_at timestamptz,
      last_error text,
      created_at timestamptz not null default now()
    );
  `);

  await pool.query(`
    create index if not exists outbox_events_unpublished_idx
    on outbox_events (published_at, locked_at, created_at);
  `);

  await pool.query(`
    insert into todos (title)
    select 'Ship the Kubernetes capstone'
    where not exists (
      select 1 from todos where title = 'Ship the Kubernetes capstone'
    );
  `);

  console.log("Todo migration completed");
}

try {
  await migrate();
} finally {
  await pool.end();
}
