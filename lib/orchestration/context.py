"""The Airflow task attempt that code is running in, for audit records."""

from __future__ import annotations

from airflow.sdk import get_current_context


def airflow_task_attempt() -> tuple[str | None, str | None, str | None, int | None]:
    """The current task attempt's dag_id, run_id, task_id and try_number, or nulls
    when not running inside an Airflow task."""
    try:
        ti = get_current_context()["ti"]
    except RuntimeError:
        return None, None, None, None
    return ti.dag_id, ti.run_id, ti.task_id, ti.try_number
