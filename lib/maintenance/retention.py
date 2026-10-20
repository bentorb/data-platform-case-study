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
from pathlib import Path

from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg import sql

from lib.ingestion import partitioning
from lib.orchestration.context import airflow_task_attempt

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class AuditRetention:
    """Days each audit table is kept."""

    dbt_run_results_days: int = 90
    publish_log_days: int = 90
    # Data-quality history and the load log entries it refers to.
    data_quality_days: int = 730


# Each statement deletes the expired rows of one table, with %(days)s days kept, and
# returns the cutoff and the number of rows deleted. Children before parents: the
# data-quality tables reference load_log.
_EXPIRY = {
    "audit.dbt_run_results": (
        "dbt_run_results_days",
        """
        with deleted as (
            delete from audit.dbt_run_results
            where recorded_at < now() - make_interval(days => %(days)s)
            returning 1
        )
        select now() - make_interval(days => %(days)s), count(*) from deleted
    """,
    ),
    "audit.publish_log": (
        "publish_log_days",
        """
        with deleted as (
            delete from audit.publish_log
            where published_at < now() - make_interval(days => %(days)s)
            returning 1
        )
        select now() - make_interval(days => %(days)s), count(*) from deleted
    """,
    ),
    "audit.dq_issue_counts": (
        "data_quality_days",
        """
        with deleted as (
            delete from audit.dq_issue_counts
            where load_id in (
                select load_id from audit.load_log
                where started_at < now() - make_interval(days => %(days)s)
            )
            returning 1
        )
        select now() - make_interval(days => %(days)s), count(*) from deleted
    """,
    ),
    "audit.dq_load_summary": (
        "data_quality_days",
        """
        with deleted as (
            delete from audit.dq_load_summary
            where load_id in (
                select load_id from audit.load_log
                where started_at < now() - make_interval(days => %(days)s)
            )
            returning 1
        )
        select now() - make_interval(days => %(days)s), count(*) from deleted
    """,
    ),
    "audit.load_log": (
        "data_quality_days",
        """
        with latest_success as (
            select max(load_id) as load_id
            from audit.load_log
            where status = 'success'
            group by target_table, source_file
        ),
        deleted as (
            delete from audit.load_log as entry
            where entry.started_at < now() - make_interval(days => %(days)s)
              and entry.load_id not in (select load_id from latest_success)
              and not exists (select 1 from audit.dq_load_summary as s where s.load_id = entry.load_id)
              and not exists (select 1 from audit.dq_issue_counts as c where c.load_id = entry.load_id)
            returning 1
        )
        select now() - make_interval(days => %(days)s), count(*) from deleted
    """,
    ),
}


def expire_audit_entries(*, postgres_conn_id: str, retention: AuditRetention | None = None) -> dict[str, int]:
    """Deletes the audit entries older than their retention, in one transaction, and
    records each table's expiry in audit.retention_log. Returns the rows deleted per table."""
    retention = retention or AuditRetention()
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    attempt = airflow_task_attempt()
    deleted = {}
    # The connection block commits on success and rolls back on error.
    with hook.get_conn() as conn, conn.cursor() as cur:
        for table, (setting, statement) in _EXPIRY.items():
            cur.execute(statement, {"days": getattr(retention, setting)})
            cutoff, rows = cur.fetchone()
            cur.execute(
                "insert into audit.retention_log"
                " (target_table, cutoff, rows_deleted, dag_id, run_id, task_id, try_number)"
                " values (%s, %s, %s, %s, %s, %s, %s)",
                (table, cutoff, rows, *attempt),
            )
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
    schema, name = table.split(".")
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute("select now() - make_interval(days => %s)", (days,))
        cutoff = cur.fetchone()[0]
        granularity, _ = partitioning.partitioning_of(cur, schema, name)
        if granularity in ("day", "month", "mixed", "empty"):
            rows = 0
            for partition in partitioning.expired_partitions(cur, table, cutoff):
                partition_id = sql.Identifier(schema, partition)
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
                """).format(table=sql.Identifier(schema, name)),
                {"table": table, "cutoff": cutoff},
            )
            rows = cur.fetchone()[0]
        cur.execute(
            "insert into audit.retention_log"
            " (target_table, cutoff, rows_deleted, dag_id, run_id, task_id, try_number)"
            " values (%s, %s, %s, %s, %s, %s, %s)",
            (table, cutoff, rows, *airflow_task_attempt()),
        )
    log.info("Expired %s rows from %s, of files last loaded before %s", rows, table, cutoff)
    return rows
