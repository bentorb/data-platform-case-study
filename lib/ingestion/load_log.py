"""Records load attempts in audit.load_log.

The start and the failure of a load are written on their own autocommitted
connection, so they survive the load's transaction being rolled back. Success is
written inside the load's transaction, so the data and its log entry commit together.
"""

from __future__ import annotations

import logging
from typing import NamedTuple

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
    cur: Cursor,
    load_id: int,
    *,
    rows_loaded: int,
    rows_replaced: int,
    schema_drift: list[str],
    checksum: str,
) -> None:
    """Marks a load successful, with the file's extra columns that were not loaded (if
    any) and its checksum. Runs on the load's own cursor, inside its transaction."""
    cur.execute(
        "update audit.load_log"
        " set status = 'success', rows_loaded = %s, rows_replaced = %s, schema_drift = %s,"
        " checksum = %s, finished_at = clock_timestamp()"
        " where load_id = %s",
        (rows_loaded, rows_replaced, schema_drift or None, checksum, load_id),
    )


def skip(cur: Cursor, load_id: int, *, schema_drift: list[str], checksum: str) -> None:
    """Marks a load skipped: the file was identical to the one already loaded. Runs on
    the load's own cursor, inside its transaction."""
    cur.execute(
        "update audit.load_log"
        " set status = 'skipped', rows_loaded = 0, rows_replaced = 0, schema_drift = %s,"
        " checksum = %s, finished_at = clock_timestamp()"
        " where load_id = %s",
        (schema_drift or None, checksum, load_id),
    )


class Load(NamedTuple):
    load_id: int
    checksum: str | None


def latest_success(cur: Cursor, *, target_table: str, source_file: str) -> Load | None:
    """The latest successful load of a file into a table, or None if it was never loaded."""
    cur.execute(
        "select load_id, checksum from audit.load_log"
        " where target_table = %s and source_file = %s and status = 'success'"
        " order by load_id desc limit 1",
        (target_table, source_file),
    )
    row = cur.fetchone()
    return Load(*row) if row else None


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
