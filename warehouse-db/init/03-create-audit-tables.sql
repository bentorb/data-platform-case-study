-- Creates the platform's audit tables. Runs once, after 02-create-schemas.sql.
-- Unlike raw tables, which ingestion creates per feed, these have a fixed structure
-- and are defined here.

-- One row per load attempt, written by ingestion (lib/ingestion/load_log.py).
CREATE TABLE audit.load_log (
  load_id       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  target_table  text        NOT NULL,
  source_file   text        NOT NULL,
  status        text        NOT NULL CHECK (status IN ('running', 'success', 'failed')),
  rows_loaded   bigint,
  rows_replaced bigint,
  started_at    timestamptz NOT NULL,
  finished_at   timestamptz,
  error_message text,
  -- The Airflow task attempt that ran the load; null when run outside Airflow.
  dag_id        text,
  run_id        text,
  task_id       text,
  try_number    integer
);

-- Finds the loads of a file, e.g. the latest one.
CREATE INDEX load_log_target_source_idx ON audit.load_log (target_table, source_file, started_at);

-- Created by the admin user, so the default privileges in 02 do not apply.
-- UPDATE moves an entry from running to success or failed; no DELETE, so entries
-- are never removed.
GRANT SELECT, INSERT, UPDATE ON audit.load_log TO ingestion;
GRANT SELECT ON audit.load_log TO transformation;
