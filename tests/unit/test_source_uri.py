"""Rendering a pipeline's source URI for a run (lib/pipelines/builder.py): the day the run
covers, templates that need no date, and failures that are clear and not retried."""

import pendulum
import pytest
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG
from airflow.sdk.exceptions import AirflowFailException

from lib.pipelines.builder import render_source_uri

TEMPLATED = "s3://landing/orders/{{ data_interval_start | ds }}.csv"


@pytest.fixture
def task():
    with DAG("orders_pipeline", schedule=None, start_date=pendulum.datetime(2026, 1, 1, tz="UTC")):
        return EmptyOperator(task_id="load_raw_orders")


def test_a_templated_uri_gets_the_day_the_run_covers(task):
    context = {"task": task, "data_interval_start": pendulum.datetime(2026, 10, 19, tz="UTC")}
    assert render_source_uri(TEMPLATED, context) == "s3://landing/orders/2026-10-19.csv"


def test_a_run_without_a_logical_date_fails_with_what_to_do(task):
    with pytest.raises(AirflowFailException, match="Cannot render.*no logical date.*--logical-date"):
        render_source_uri(TEMPLATED, {"task": task, "data_interval_start": None})


def test_a_typo_in_a_run_without_a_logical_date_shows_the_template_error(task):
    with pytest.raises(AirflowFailException, match="Cannot render.*'paramz' is undefined.*no logical date"):
        render_source_uri("s3://landing/{{ paramz.feed }}.csv", {"task": task, "data_interval_start": None})


def test_a_fixed_uri_needs_no_logical_date(task):
    uri = "s3://landing/orders/latest.csv"
    assert render_source_uri(uri, {"task": task, "data_interval_start": None}) == uri


def test_a_template_without_dates_renders_in_a_run_without_a_logical_date(task):
    uri = "s3://landing/{{ params.feed }}/latest.csv"
    context = {"task": task, "data_interval_start": None, "params": {"feed": "orders"}}
    assert render_source_uri(uri, context) == "s3://landing/orders/latest.csv"


def test_a_broken_template_fails_without_retrying(task):
    context = {"task": task, "data_interval_start": pendulum.datetime(2026, 10, 19, tz="UTC")}
    with pytest.raises(AirflowFailException, match="Cannot render the source URI") as error:
        render_source_uri("s3://landing/{{ params.missing }}.csv", context)
    # The run has a date: no hint about giving it one.
    assert "logical date" not in str(error.value)
