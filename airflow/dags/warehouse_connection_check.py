"""Smoke test: checks that Airflow can reach the warehouse database with the ingestion credentials."""

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import DAG

with DAG(
    dag_id="warehouse_connection_check",
    description="Checks that Airflow can connect to the warehouse database",
    schedule=None,
    catchup=False,
    tags=["smoke-test"],
):
    SQLExecuteQueryOperator(
        task_id="check_connection",
        conn_id="warehouse_ingestion",
        sql="select current_user, current_database(), version()",
        show_return_value_in_logs=True,
    )
