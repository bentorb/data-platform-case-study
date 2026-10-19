"""Builds Airflow task groups that run the platform's dbt project with Cosmos.

All Cosmos configuration lives here, so DAGs only choose which models to run.
dbt currently runs in local mode (installed in the Airflow image); other execution
modes, such as Kubernetes, are added here without changing DAGs.
"""

from __future__ import annotations

import os
from pathlib import Path

from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import DbtResourceType, ExecutionMode, InvocationMode, LoadMode, TestBehavior

# Set by docker-compose.yml; defaults to the dbt/ folder at the repo root.
DBT_PROJECT_PATH = Path(os.environ.get("DBT_PROJECT_PATH", Path(__file__).resolve().parents[2] / "dbt"))
# dbt is installed in its own virtualenv in the Airflow image.
DBT_EXECUTABLE_PATH = "/opt/dbt-venv/bin/dbt"


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
            },
        ),
        profile_config=ProfileConfig(
            profile_name="data_platform",
            target_name="local",
            profiles_yml_filepath=DBT_PROJECT_PATH / "profiles.yml",
        ),
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
    )
    nodes = task_group.dbt_graph.filtered_nodes.values()
    if not any(node.resource_type == DbtResourceType.MODEL for node in nodes):
        raise ValueError(f"The dbt selection {select} matches no models")
    return task_group
