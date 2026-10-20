"""Records load attempts in audit.load_log.

The start and the failure of a load are written on their own autocommitted
connection, so they survive the load's transaction being rolled back. Success is
written inside the load's transaction, so the data and its log entry commit together.
"""

from __future__ import annotations

import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg import Cursor

from lib.orchestration.context import airflow_task_attempt

log = logging.getLogger(__name__)


def start(hook: PostgresHook, *, target_table: str, source_file: str) -> int:
    """Logs a running load and returns its load_id."""
    with _autocommit_connection(hook) as conn:
        row = conn.execute(
            "insert into audit.load_log"
            " (target_table, source_file, status, started_at, dag_id, run_id, task_id, try_number)"
            " values (%s, %s, 'running', clock_timestamp(), %s, %s, %s, %s)"
            " returning load_id",
            (target_table, source_file, *airflow_task_attempt()),
        ).fetchone()
    return row[0]


def succeed(
    cur: Cursor, load_id: int, *, rows_loaded: int, rows_replaced: int, schema_drift: list[str]
) -> None:
    """Marks a load successful, with the file's extra columns that were not loaded (if
    any). Runs on the load's own cursor, inside its transaction."""
    cur.execute(
        "update audit.load_log"
        " set status = 'success', rows_loaded = %s, rows_replaced = %s, schema_drift = %s,"
        " finished_at = clock_timestamp()"
        " where load_id = %s",
        (rows_loaded, rows_replaced, schema_drift or None, load_id),
    )


def fail(hook: PostgresHook, load_id: int, error: BaseException) -> None:
    """Marks a load failed. Never raises, so the load's own error is the one reported."""
    try:
        with _autocommit_connection(hook) as conn:
            conn.execute(
                "update audit.load_log"
                " set status = 'failed', error_message = %s, finished_at = clock_timestamp()"
                " where load_id = %s",
                (f"{type(error).__name__}: {error}", load_id),
            )
    except Exception:
        log.exception("Could not mark load %s as failed in audit.load_log", load_id)


def _autocommit_connection(hook: PostgresHook):
    conn = hook.get_conn()
    conn.autocommit = True
    return conn
