"""Runs the dbt unit tests (unit_tests: in dbt/models/**/_*unit_tests.yml) on the warehouse.

dbt unit tests run each model's SQL on given input rows, but read the inputs' column types
from the warehouse, so the raw tables and the models they read must exist, even empty.
Missing ones are created first: raw tables from the pipeline configs, with the loader's
DDL, and models with `dbt run`, as a pipeline would build them (empty on an empty
warehouse). Existing ones are left as they are, so the tests never replace data.
(`dbt run --empty` cannot be used: it breaks models that alias a ref, e.g.
`{{ ref('stg_customer_transactions') }} as stg`.)

Needs the warehouse: skipped without one, unless REQUIRE_WAREHOUSE is set (e.g. in CI).
"""

import json
import os

import psycopg
import pytest
from airflow.providers.postgres.hooks.postgres import PostgresHook

from lib.ingestion.object_storage_to_postgres import create_raw_table
from lib.pipelines.config import PIPELINES_PATH, load_config


def warehouse(conn_id):
    hook = PostgresHook(postgres_conn_id=conn_id)
    try:
        hook.get_conn().close()
    except Exception as error:
        if os.environ.get("REQUIRE_WAREHOUSE"):
            raise
        pytest.skip(f"the warehouse is not reachable ({type(error).__name__}): start the stack first")
    return hook


def create_missing_raw_tables():
    """The raw table of every pipeline, from its config, if it does not exist yet."""
    for path in sorted(PIPELINES_PATH.glob("*.yml")):
        ingestion = load_config(path.stem).ingestion
        with warehouse(ingestion.target.conn_id).get_conn() as conn, conn.cursor() as cur:
            create_raw_table(cur, ingestion.target.table, ingestion.columns, ingestion.target.partition_by)


def missing_models(dbt):
    """The models whose relation does not exist in the warehouse."""
    listed = dbt(
        "ls", "--resource-type", "model", "--output", "json", "--output-keys", "name", "config", "--quiet"
    )
    assert listed.returncode == 0, listed.stdout + listed.stderr
    models = [json.loads(line) for line in listed.stdout.splitlines() if line.startswith("{")]
    # As dbt's user, with dbt's settings: information_schema only lists the tables a
    # user has privileges on.
    with psycopg.connect(
        host=os.environ["DBT_WAREHOUSE_HOST"],
        port=os.environ["DBT_WAREHOUSE_PORT"],
        dbname=os.environ["DBT_WAREHOUSE_DBNAME"],
        user=os.environ["DBT_WAREHOUSE_USER"],
        password=os.environ["DBT_WAREHOUSE_PASSWORD"],
    ) as conn:
        existing = set(
            conn.execute("select table_schema, table_name from information_schema.tables").fetchall()
        )
    return [
        m["name"]
        for m in models
        if (m["config"]["schema"], m["config"].get("alias") or m["name"]) not in existing
    ]


def test_dbt_unit_tests(dbt_project):
    create_missing_raw_tables()
    missing = missing_models(dbt_project)
    if missing:
        built = dbt_project("run", "--select", *missing)
        assert built.returncode == 0, "dbt run of the missing models failed:\n" + built.stdout[-4000:]

    result = dbt_project("test", "--select", "test_type:unit")
    assert result.returncode == 0, "dbt unit tests failed:\n" + result.stdout[-8000:]
