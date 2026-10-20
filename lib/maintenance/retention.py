"""Expires old data: entries of the platform's audit tables, files such as task logs,
and the rows of raw tables older than their retention.

Each table is kept for a number of days, and older entries are deleted in one
transaction, each deletion recorded in audit.retention_log. Load log entries are
lineage: rows in raw and core refer to their load through _load_id, and since loads
replace whole files, current data only refers to the latest successful load of each
file. Those entries are never expired; failed attempts and superseded loads are,
once their data-quality history has been.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg import Cursor, sql

from lib.ingestion import partitioning
from lib.orchestration.context import airflow_task_attempt
from lib.warehouse import TableName

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class AuditRetention:
    """Days each audit table is kept (set by the platform_maintenance DAG)."""

    dbt_run_results_days: int
    publish_log_days: int
    # Data-quality history and the load log entries it refers to.
    data_quality_days: int


# The expired rows of each table, with %(cutoff)s the oldest time kept, and the
# retention setting giving its days. Children before parents: the data-quality tables
# reference load_log.
_EXPIRY = {
    "audit.dbt_run_results": ("dbt_run_results_days", "entry.recorded_at < %(cutoff)s"),
    "audit.publish_log": ("publish_log_days", "entry.published_at < %(cutoff)s"),
    "audit.dq_issue_counts": (
        "data_quality_days",
        "entry.load_id in (select load_id from audit.load_log where started_at < %(cutoff)s)",
    ),
    "audit.dq_load_summary": (
        "data_quality_days",
        "entry.load_id in (select load_id from audit.load_log where started_at < %(cutoff)s)",
    ),
    "audit.load_log": (
        "data_quality_days",
        """
        entry.started_at < %(cutoff)s
          -- The latest successful load of each file: current data refers to it.
          and entry.load_id not in (
              select max(load_id) from audit.load_log where status = 'success'
              group by target_table, source_file
          )
          and not exists (select 1 from audit.dq_load_summary as s where s.load_id = entry.load_id)
          and not exists (select 1 from audit.dq_issue_counts as c where c.load_id = entry.load_id)
        """,
    ),
}


def expire_audit_entries(*, postgres_conn_id: str, retention: AuditRetention) -> dict[str, int]:
    """Deletes the audit entries older than their retention, in one transaction, and
    records each table's expiry in audit.retention_log. Returns the rows deleted per table."""
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    deleted = {}
    # The connection block commits on success and rolls back on error.
    with hook.get_conn() as conn, conn.cursor() as cur:
        for table, (setting, condition) in _EXPIRY.items():
            cutoff = _cutoff(cur, getattr(retention, setting))
            statement = sql.SQL(
                "with deleted as (delete from {} as entry where {} returning 1) select count(*) from deleted"
            ).format(TableName.parse(table).identifier, sql.SQL(condition))
            cur.execute(statement, {"cutoff": cutoff})
            rows = cur.fetchone()[0]
            _record_expiry(cur, table, cutoff, rows)
            deleted[table] = rows
            log.info("Expired %s rows from %s older than %s", rows, table, cutoff)
    return deleted


def delete_old_files(folder: str, days: int) -> int:
    """Deletes the files under `folder` last modified more than `days` days ago, then
    the folders left empty. Returns the number of files deleted."""
    cutoff = time.time() - days * 86400
    root = Path(folder)
    deleted = 0
    for path in root.rglob("*"):
        if path.is_file() and path.stat().st_mtime < cutoff:
            path.unlink()
            deleted += 1
    # Deepest folders first, so a folder emptied by its children is removed too. Only
    # folders unchanged for a day: a new, still empty one may be about to receive a
    # running task's log. (Deleting a file updates its folder, so the folders emptied
    # above are removed by the next day's run.)
    folder_cutoff = time.time() - 86400
    for path in sorted((p for p in root.rglob("*") if p.is_dir()), key=lambda p: len(p.parts), reverse=True):
        if path.stat().st_mtime < folder_cutoff and not any(path.iterdir()):
            path.rmdir()
    log.info("Deleted %s files older than %s days from %s", deleted, days, folder)
    return deleted


def expire_raw_files(*, postgres_conn_id: str, table: str, days: int) -> int:
    """Expires from a raw table the rows of every file whose latest successful load is
    older than `days` days, recording the expiry in audit.retention_log. Returns the
    number of rows expired.

    A partitioned table drops its partitions whose whole period is older: a partition
    only holds files whose latest load was in its period, since loading a file again
    moves its rows to the current one. Whole periods are kept, so raw may be kept up to
    a period longer. A table that is not partitioned deletes those files' rows.
    """
    target = TableName.parse(table)
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    with hook.get_conn() as conn, conn.cursor() as cur:
        cutoff = _cutoff(cur, days)
        granularity, _ = partitioning.partitioning_of(cur, target.schema, target.name)
        if granularity in ("day", "month", "mixed", "empty"):
            rows = 0
            for partition in partitioning.expired_partitions(cur, table, cutoff):
                partition_id = target.in_schema(partition)
                cur.execute(sql.SQL("select count(*) from {}").format(partition_id))
                rows += cur.fetchone()[0]
                cur.execute(sql.SQL("drop table {}").format(partition_id))
        else:
            cur.execute(
                sql.SQL("""
                    with expired_files as (
                        select source_file
                        from audit.load_log
                        where target_table = %(table)s and status = 'success'
                        group by source_file
                        having max(started_at) < %(cutoff)s
                    ),
                    deleted as (
                        delete from {table}
                        where _source_file in (select source_file from expired_files)
                        returning 1
                    )
                    select count(*) from deleted
                """).format(table=target.identifier),
                {"table": table, "cutoff": cutoff},
            )
            rows = cur.fetchone()[0]
        _record_expiry(cur, table, cutoff, rows)
    log.info("Expired %s rows from %s, of files last loaded before %s", rows, table, cutoff)
    return rows


def _cutoff(cur: Cursor, days: int) -> datetime:
    """The oldest time kept with `days` days of retention, from the database's clock."""
    cur.execute("select now() - make_interval(days => %s)", (days,))
    return cur.fetchone()[0]


def _record_expiry(cur: Cursor, table: str, cutoff: datetime, rows: int) -> None:
    """Records an expiry in audit.retention_log, in the expiry's own transaction."""
    cur.execute(
        "insert into audit.retention_log"
        " (target_table, cutoff, rows_deleted, dag_id, run_id, task_id, try_number)"
        " values (%s, %s, %s, %s, %s, %s, %s)",
        (table, cutoff, rows, *airflow_task_attempt()),
    )
