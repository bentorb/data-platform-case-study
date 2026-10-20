"""Expires old entries from the platform's audit tables, and old files such as task logs.

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
    "audit.dbt_run_results": ("dbt_run_results_days", """
        with deleted as (
            delete from audit.dbt_run_results
            where recorded_at < now() - make_interval(days => %(days)s)
            returning 1
        )
        select now() - make_interval(days => %(days)s), count(*) from deleted
    """),
    "audit.publish_log": ("publish_log_days", """
        with deleted as (
            delete from audit.publish_log
            where published_at < now() - make_interval(days => %(days)s)
            returning 1
        )
        select now() - make_interval(days => %(days)s), count(*) from deleted
    """),
    "audit.dq_issue_counts": ("data_quality_days", """
        with deleted as (
            delete from audit.dq_issue_counts
            where load_id in (
                select load_id from audit.load_log
                where started_at < now() - make_interval(days => %(days)s)
            )
            returning 1
        )
        select now() - make_interval(days => %(days)s), count(*) from deleted
    """),
    "audit.dq_load_summary": ("data_quality_days", """
        with deleted as (
            delete from audit.dq_load_summary
            where load_id in (
                select load_id from audit.load_log
                where started_at < now() - make_interval(days => %(days)s)
            )
            returning 1
        )
        select now() - make_interval(days => %(days)s), count(*) from deleted
    """),
    "audit.load_log": ("data_quality_days", """
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
    """),
}


def expire_audit_entries(*, postgres_conn_id: str, retention: AuditRetention = AuditRetention()) -> dict[str, int]:
    """Deletes the audit entries older than their retention, in one transaction, and
    records each table's expiry in audit.retention_log. Returns the rows deleted per table."""
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
