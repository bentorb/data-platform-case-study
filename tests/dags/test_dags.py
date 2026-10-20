"""DAG integrity: every DAG imports and follows the platform's rules, and every pipeline
config gives a pipeline DAG built as the platform expects (lib/pipelines/builder.py).

Parses the DAGs folder like Airflow's DAG processor does, without running anything.
"""

import pytest
from airflow.configuration import conf
from airflow.dag_processing.dagbag import DagBag
from airflow.timetables.interval import CronDataIntervalTimetable

from lib.orchestration.callbacks import notify_failure
from lib.pipelines.config import PIPELINES_PATH, load_config

PIPELINES = sorted(path.stem for path in PIPELINES_PATH.glob("*.yml"))


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=conf.get("core", "dags_folder"))


def test_every_dag_imports(dagbag):
    assert dagbag.import_errors == {}
    assert dagbag.dags


def test_every_pipeline_config_has_a_dag(dagbag):
    assert {f"{name}_pipeline" for name in PIPELINES} <= set(dagbag.dags)


def test_every_dag_follows_the_platform_rules(dagbag):
    problems = []
    for dag_id, dag in sorted(dagbag.dags.items()):
        if not dag.description:
            problems.append(f"{dag_id}: no description")
        if not dag.tags:
            problems.append(f"{dag_id}: no tags")
        if dag.catchup:
            problems.append(f"{dag_id}: catchup is on")
        for task in dag.tasks:
            # Alerts go to the owner's webhook, so every task needs a real owner.
            if task.owner in ("", "airflow"):
                problems.append(f"{dag_id}.{task.task_id}: no owner")
            if task.retries < 1:
                problems.append(f"{dag_id}.{task.task_id}: no retries")
            if notify_failure not in (task.on_failure_callback or []):
                problems.append(f"{dag_id}.{task.task_id}: no failure alert")
    assert problems == []


@pytest.mark.parametrize("name", PIPELINES)
def test_pipeline_dag(dagbag, name):
    dag = dagbag.dags[f"{name}_pipeline"]
    config = load_config(name)
    table = config.ingestion.target.table.replace(".", "_")

    # Each run covers a whole interval, one run at a time.
    assert isinstance(dag.timetable, CronDataIntervalTimetable)
    assert dag.max_active_runs == 1
    assert config.ingestion.source.uri in dag.doc_md

    for task in dag.tasks:
        assert task.owner == config.owner, task.task_id
        # Days are processed in order: incremental models rely on loads arriving in order.
        assert task.depends_on_past and task.wait_for_downstream, task.task_id

    # The load starts every run, and the publish to live ends it, after everything else.
    assert [t.task_id for t in dag.roots] == [f"load_{table}"]
    assert [t.task_id for t in dag.leaves] == ["publish"]
    assert len(dag.get_task("publish").get_flat_relatives(upstream=True)) == len(dag.tasks) - 1

    # Unit tests check code, not data: never part of a pipeline run.
    assert not [t.task_id for t in dag.tasks if "unit_test" in t.task_id]
