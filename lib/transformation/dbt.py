"""Builds the Airflow tasks that run the platform's dbt project with Cosmos: a task
group that builds and tests models in core, and a task that publishes them to live.

All Cosmos configuration lives here, so DAGs only choose which models to run.
dbt currently runs in local mode (installed in the Airflow image); other execution
modes, such as Kubernetes, are added here without changing DAGs.
"""

from __future__ import annotations

import os
from pathlib import Path

from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import DbtResourceType, ExecutionMode, InvocationMode, LoadMode, TestBehavior
from cosmos.operators.local import DbtRunOperationLocalOperator

from lib.orchestration.callbacks import notify_warning

# Set by docker-compose.yml; defaults to the dbt/ folder at the repo root.
DBT_PROJECT_PATH = Path(os.environ.get("DBT_PROJECT_PATH", Path(__file__).resolve().parents[2] / "dbt"))
# dbt is installed in its own virtualenv in the Airflow image.
DBT_EXECUTABLE_PATH = "/opt/dbt-venv/bin/dbt"

PROFILE_CONFIG = ProfileConfig(
    profile_name="data_platform",
    target_name="local",
    profiles_yml_filepath=DBT_PROJECT_PATH / "profiles.yml",
)


def build_dbt_task_group(group_id: str = "dbt", select: list[str] | None = None) -> DbtTaskGroup:
    """Returns a task group with one task per dbt model, each followed by its tests.

    `select` uses dbt's selection syntax (e.g. ["path:models/staging"]); by default
    the whole project runs. A selection without any model raises ValueError, as it is
    almost certainly a mistake (e.g. a misspelt name).
    """
    task_group = DbtTaskGroup(
        group_id=group_id,
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
            # dbt packages are installed in the Airflow image at build time; otherwise
            # Cosmos would download them while parsing and before every task.
            install_dbt_deps=False,
            env_vars={
                # Run a test only when each model it tests is selected or upstream of
                # one that is, rather than when any is (dbt's default): a test of
                # models built by another pipeline is left to that pipeline.
                "DBT_INDIRECT_SELECTION": "buildable",
                # Unit tests check code, not data, so they run in CI and with the tests
                # service, not in pipelines: `dbt test --select <model>` would run them too.
                "DBT_EXCLUDE_RESOURCE_TYPES": "unit_test",
            },
        ),
        profile_config=PROFILE_CONFIG,
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.LOCAL,
            # Run the dbt executable rather than importing dbt: it is not installed
            # in Airflow's Python environment.
            invocation_mode=InvocationMode.SUBPROCESS,
            dbt_executable_path=DBT_EXECUTABLE_PATH,
        ),
        render_config=RenderConfig(
            # Cosmos runs `dbt ls` while parsing the DAG to discover the models;
            # like the tasks, it must run the executable rather than import dbt.
            load_method=LoadMode.DBT_LS,
            invocation_mode=InvocationMode.SUBPROCESS,
            dbt_executable_path=DBT_EXECUTABLE_PATH,
            select=select or [],
            test_behavior=TestBehavior.AFTER_EACH,
            # Tests on several models (e.g. relationships) get their own task, run once
            # all those models are built, instead of with one of them.
            should_detach_multiple_parents_tests=True,
        ),
        # Tests that warn do not fail their task, so they are sent to the owner's alert
        # webhook by this callback instead.
        on_warning_callback=notify_warning,
    )
    if not selected_models(task_group):
        raise ValueError(f"The dbt selection {select} matches no models")
    return task_group


def selected_models(task_group: DbtTaskGroup) -> list[str]:
    """The names of the dbt models a task group builds."""
    nodes = task_group.dbt_graph.filtered_nodes.values()
    return sorted(node.resource_name for node in nodes if node.resource_type == DbtResourceType.MODEL)


def core_retention(task_group: DbtTaskGroup) -> dict[str, int | None]:
    """The retention of each core model a task group builds, in days, from the model's
    config.meta.retention ({column: ..., days: ...}); None keeps it forever, the default.
    An invalid retention raises ValueError."""
    retention = {}
    for node in task_group.dbt_graph.filtered_nodes.values():
        if node.resource_type != DbtResourceType.MODEL or node.config.get("schema") != "core":
            continue
        setting = (node.config.get("meta") or {}).get("retention")
        if setting is None:
            retention[node.resource_name] = None
            continue
        days = setting.get("days") if isinstance(setting, dict) else None
        if not (isinstance(days, int) and days > 0 and isinstance(setting.get("column"), str)):
            raise ValueError(
                f"{node.resource_name}: retention must be {{column: <date column>, days: <positive integer>}},"
                f" not {setting!r}"
            )
        retention[node.resource_name] = days
    return retention


def build_dbt_publish_task(models: list[str], task_id: str = "publish") -> DbtRunOperationLocalOperator:
    """Returns a task that publishes `models` from core to live in one transaction (see
    dbt/macros/publish_models.sql). Models outside core are ignored. Placed after the
    task group that builds and tests them, it only runs once they have all passed."""
    return DbtRunOperationLocalOperator(
        task_id=task_id,
        macro_name="publish_models",
        args={"models": models},
        project_dir=DBT_PROJECT_PATH,
        profile_config=PROFILE_CONFIG,
        install_deps=False,
        invocation_mode=InvocationMode.SUBPROCESS,
        dbt_executable_path=DBT_EXECUTABLE_PATH,
    )
