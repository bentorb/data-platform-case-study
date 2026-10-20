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
  -- Columns in the file that the load did not expect, so did not load.
  schema_drift  text[],
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

-- One row per model or test result of each dbt invocation, written by dbt's
-- on-run-end hook (dbt/macros/log_run_results.sql).
CREATE TABLE audit.dbt_run_results (
  result_id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  invocation_id  text        NOT NULL,
  command        text        NOT NULL,
  run_started_at timestamptz NOT NULL,
  node_id        text        NOT NULL,
  resource_type  text        NOT NULL,
  status         text        NOT NULL,
  failures       bigint,
  rows_affected  bigint,
  execution_time numeric(10, 3),
  message        text,
  -- The Airflow task attempt that ran dbt; null when run outside Airflow.
  dag_id         text,
  run_id         text,
  task_id        text,
  try_number     integer,
  recorded_at    timestamptz NOT NULL DEFAULT now(),
  UNIQUE (invocation_id, node_id)
);

-- Finds the history of a model or test.
CREATE INDEX dbt_run_results_node_idx ON audit.dbt_run_results (node_id, run_started_at);

-- Insert only: results are never changed or removed.
GRANT SELECT, INSERT ON audit.dbt_run_results TO transformation;
GRANT SELECT ON audit.dbt_run_results TO ingestion;

-- Data-quality history: written by each staging model, for its own loads, right after
-- it is built (dbt/macros/record_dq_issues.sql). Kept for every load, including loads
-- whose rows were later replaced by a new delivery of the same file.

-- One row per load and staging model: how many rows had data-quality issues.
CREATE TABLE audit.dq_load_summary (
  load_id            bigint       NOT NULL REFERENCES audit.load_log (load_id),
  model              text         NOT NULL,
  total_rows         bigint       NOT NULL,
  -- Rows with at least one dq_errors code, i.e. excluded from core.
  rows_with_errors   bigint       NOT NULL,
  -- Rows with at least one dq_warnings code, i.e. repaired.
  rows_with_warnings bigint       NOT NULL,
  error_rate         numeric(5, 4) NOT NULL CHECK (error_rate BETWEEN 0 AND 1),
  measured_at        timestamptz  NOT NULL DEFAULT now(),
  PRIMARY KEY (load_id, model)
);

-- One row per load, staging model and issue code: how many rows had the issue.
CREATE TABLE audit.dq_issue_counts (
  load_id       bigint NOT NULL REFERENCES audit.load_log (load_id),
  model         text   NOT NULL,
  -- error: value unusable, row excluded from core; warning: value repaired.
  kind          text   NOT NULL CHECK (kind IN ('error', 'warning')),
  code          text   NOT NULL,
  affected_rows bigint NOT NULL,
  PRIMARY KEY (load_id, model, kind, code)
);

-- Insert only. Counts only, no raw values, so data consumers may read them.
GRANT SELECT, INSERT ON audit.dq_load_summary, audit.dq_issue_counts TO transformation;
GRANT SELECT ON audit.dq_load_summary, audit.dq_issue_counts TO analytics;

-- One row per model and publish to live, written by dbt/macros/publish_models.sql in
-- the publish's own transaction: a row exists if and only if the publish committed.
-- Answers which version of a live table consumers see, and since when.
CREATE TABLE audit.publish_log (
  invocation_id  text        NOT NULL,
  model          text        NOT NULL,
  target_table   text        NOT NULL,
  -- Whether this publish created the live table.
  created        boolean     NOT NULL,
  rows_deleted   bigint      NOT NULL,
  rows_inserted  bigint,
  published_at   timestamptz NOT NULL DEFAULT now(),
  -- The Airflow task attempt that published; null when run outside Airflow.
  dag_id         text,
  run_id         text,
  task_id        text,
  try_number     integer,
  PRIMARY KEY (invocation_id, model)
);

-- Finds the publishes of a model, e.g. the latest one.
CREATE INDEX publish_log_model_idx ON audit.publish_log (model, published_at);

-- UPDATE fills in rows_inserted, within the same transaction. Consumers may read it.
GRANT SELECT, INSERT, UPDATE ON audit.publish_log TO transformation;
GRANT SELECT ON audit.publish_log TO analytics;
