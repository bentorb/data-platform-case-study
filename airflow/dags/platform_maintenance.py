"""Platform housekeeping, daily: expires old audit entries and task logs.

Business data (raw, core, live) is not expired here: each pipeline expires its own.
Airflow's own metadata is cleaned by the airflow-db-clean job (docker-compose.yml):
Airflow 3 tasks cannot access the metadata database.
"""

import pendulum
from airflow.configuration import conf
from airflow.sdk import dag, task

from lib.maintenance.retention import AuditRetention, delete_old_files, expire_audit_entries
from lib.orchestration.defaults import DEFAULT_ARGS

AUDIT_RETENTION = AuditRetention(dbt_run_results_days=90, publish_log_days=90, data_quality_days=730)
TASK_LOG_DAYS = 90


@dag(
    dag_id="platform_maintenance",
    description="Expires old audit entries and task logs",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "owner": "data-platform"},
    tags=["platform", "maintenance"],
)
def platform_maintenance():
    @task
    def expire_audit() -> dict[str, int]:
        return expire_audit_entries(postgres_conn_id="warehouse_maintenance", retention=AUDIT_RETENTION)

    @task
    def delete_old_task_logs() -> int:
        return delete_old_files(conf.get("logging", "base_log_folder"), TASK_LOG_DAYS)

    expire_audit()
    delete_old_task_logs()


platform_maintenance()
