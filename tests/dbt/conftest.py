"""Fixtures for the dbt tests."""

import shutil
import subprocess

import pytest

from lib.transformation.dbt import DBT_EXECUTABLE_PATH, DBT_PROJECT_PATH


@pytest.fixture(scope="module")
def dbt_project(tmp_path_factory):
    """A dbt runner on a writable copy of the dbt project (it is mounted read-only); the
    copy's path is the runner's `project` attribute."""
    project = tmp_path_factory.mktemp("dbt") / "project"
    shutil.copytree(DBT_PROJECT_PATH, project, ignore=shutil.ignore_patterns("target", "logs"))

    def dbt(*args):
        return subprocess.run(
            [DBT_EXECUTABLE_PATH, *args, "--project-dir", str(project), "--profiles-dir", str(project)],
            capture_output=True,
            text=True,
        )

    dbt.project = project
    return dbt
