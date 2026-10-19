"""Builds a pipeline's Airflow DAG from its config (pipelines/<name>.yml): a task that
loads the source file into the target table, followed by the pipeline's dbt models.

Each pipeline has its own DAG file calling build_pipeline, so a broken config only
breaks that pipeline's DAG.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import DAG, dag, task

from lib.ingestion.object_storage_to_postgres import load_csv
from lib.orchestration.defaults import DEFAULT_ARGS
from lib.pipelines.config import config_path, load_config
from lib.transformation.dbt import build_dbt_task_group


def build_pipeline(name: str) -> DAG:
    """Returns the DAG of the pipeline `name`, with dag_id <name>_pipeline."""
    config = load_config(name)
    ingestion = config.ingestion
    # By default, the models built from the target table: its dbt source is named
    # after its schema (e.g. source('raw', 'customer_transactions')).
    select = config.transformation.select or [f"source:{ingestion.target.table}+"]

    @dag(
        dag_id=f"{name}_pipeline",
        description=config.description,
        # The config, shown on the DAG's page in the UI.
        doc_md=f"Built from `pipelines/{name}.yml`:\n\n```yaml\n{config_path(name).read_text()}```",
        schedule=config.schedule,
        start_date=pendulum.datetime(
            config.start_date.year, config.start_date.month, config.start_date.day, tz="UTC"
        ),
        catchup=False,
        max_active_runs=1,  # dbt cannot rebuild the same model concurrently
        default_args={**DEFAULT_ARGS, "owner": config.owner},
        tags=[name, "ingestion", "dbt"],
    )
    def pipeline():
        @task(task_id=f"load_{ingestion.target.table.replace('.', '_')}")
        def load() -> int:
            return load_csv(
                source_uri=ingestion.source.uri,
                source_conn_id=ingestion.source.conn_id,
                postgres_conn_id=ingestion.target.conn_id,
                table=ingestion.target.table,
                columns=ingestion.columns,
            )

        load() >> build_dbt_task_group(select=select)

    return pipeline()
