"""Customer transactions pipeline: loads the landed CSV into the warehouse raw layer,
then runs the dbt models built on it."""

import pendulum
from airflow.sdk import dag, task

from lib.ingestion.object_storage_to_postgres import load_csv
from lib.orchestration.defaults import DEFAULT_ARGS
from lib.transformation.dbt import build_dbt_task_group


@dag(
    dag_id="customer_transactions_pipeline",
    description="Loads customer transactions from the landing zone into the warehouse",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,  # dbt cannot rebuild the same model concurrently
    default_args=DEFAULT_ARGS,
    tags=["customer_transactions", "ingestion", "dbt"],
)
def customer_transactions_pipeline():
    @task
    def load_raw_customer_transactions() -> int:
        return load_csv(
            source_uri="s3://landing/customer_transactions.csv",
            source_conn_id="landing_zone_s3",
            postgres_conn_id="warehouse_ingestion",
            table="raw.customer_transactions",
            columns=[
                "transaction_id",
                "customer_id",
                "transaction_date",
                "product_id",
                "product_name",
                "quantity",
                "price",
                "tax",
            ],
        )

    load_raw_customer_transactions() >> build_dbt_task_group()


customer_transactions_pipeline()
