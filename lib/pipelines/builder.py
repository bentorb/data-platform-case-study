"""Builds a pipeline's Airflow DAG from its config (pipelines/<name>.yml): a task that
loads the source file into the target table, the pipeline's dbt models, built and
tested in core, and a task that publishes them to live.

Each pipeline has its own DAG file calling build_pipeline, so a broken config only
breaks that pipeline's DAG.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import DAG, dag, task
from airflow.timetables.interval import CronDataIntervalTimetable

from lib.ingestion.object_storage_to_postgres import load_csv
from lib.orchestration.defaults import DEFAULT_ARGS
from lib.pipelines.config import config_path, load_config
from lib.transformation.dbt import build_dbt_publish_task, build_dbt_task_group, selected_models


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
        # Each run covers one whole interval, from data_interval_start to
        # data_interval_end, and starts once it has ended: the run at midnight
        # processes the previous day. In Airflow 3 a plain cron schedule gives
        # zero-length intervals at the run time instead.
        schedule=CronDataIntervalTimetable(config.schedule, timezone="UTC"),
        start_date=pendulum.datetime(
            config.start_date.year, config.start_date.month, config.start_date.day, tz="UTC"
        ),
        catchup=False,
        max_active_runs=1,  # dbt cannot rebuild the same model concurrently
        default_args={
            **DEFAULT_ARGS,
            "owner": config.owner,
            # Runs are processed one after another in date order, backfills included:
            # each task waits for the same task, and the ones right after it, in the
            # previous run. Incremental models rely on loads arriving in order, so a
            # failed day blocks later days until it is fixed or marked as success.
            "depends_on_past": True,
            "wait_for_downstream": True,
        },
        tags=[name, "ingestion", "dbt"],
    )
    def pipeline():
        @task(task_id=f"load_{ingestion.target.table.replace('.', '_')}")
        def load_raw_table(source_uri: str) -> int:
            return load_csv(
                source_uri=source_uri,
                source_conn_id=ingestion.source.conn_id,
                postgres_conn_id=ingestion.target.conn_id,
                table=ingestion.target.table,
                columns=ingestion.columns,
            )

        # Build and test in core, then publish to live once every test has passed.
        # The source URI is templated, e.g. with the day the run covers.
        load = load_raw_table(ingestion.source.uri)
        transform = build_dbt_task_group(select=select)
        models = selected_models(transform)
        publish = build_dbt_publish_task(models)

        load >> transform >> publish

    return pipeline()
