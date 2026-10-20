"""Retention rules: how long raw is kept given its core models (lib/pipelines/builder.py),
how core retentions are read (lib/transformation/dbt.py), and old file deletion
(lib/maintenance/retention.py)."""

import os
import time
from types import SimpleNamespace

import pytest
from cosmos.constants import DbtResourceType

from lib.maintenance.retention import delete_old_files
from lib.pipelines.builder import raw_retention_days
from lib.transformation.dbt import core_retention


@pytest.mark.parametrize(
    "configured, core, expected",
    [
        (None, {"fct": 365, "agg": 30}, 365),  # as long as the longest-lived model
        (400, {"fct": 365}, 400),  # longer if configured
        (None, {"fct": 365, "dim": None}, None),  # a model kept forever keeps raw forever
        (None, {}, None),  # no core models: kept forever
    ],
)
def test_raw_retention(configured, core, expected):
    assert raw_retention_days("orders", configured, core) == expected


@pytest.mark.parametrize(
    "configured, core, message",
    [
        (100, {"fct": 365}, "shorter than the retention"),
        (100, {"fct": None}, "are kept forever"),
    ],
)
def test_raw_retention_that_would_break_full_refreshes(configured, core, message):
    with pytest.raises(ValueError, match=message):
        raw_retention_days("orders", configured, core)


def node(name, schema="core", meta=None, resource_type=DbtResourceType.MODEL):
    return SimpleNamespace(
        resource_name=name, resource_type=resource_type, config={"schema": schema, "meta": meta or {}}
    )


def task_group(*nodes):
    return SimpleNamespace(dbt_graph=SimpleNamespace(filtered_nodes={n.resource_name: n for n in nodes}))


def test_core_retention():
    group = task_group(
        node("fct", meta={"retention": {"column": "day", "days": 365}}),
        node("dim"),
        node("stg", schema="staging", meta={"retention": {"column": "day", "days": 1}}),
        node("fct_test", resource_type=DbtResourceType.TEST),
    )
    assert core_retention(group) == {"fct": 365, "dim": None}


@pytest.mark.parametrize(
    "retention",
    [{"column": "day", "days": 0}, {"column": "day", "days": "forever"}, {"days": 365}, "365"],
)
def test_invalid_core_retention(retention):
    with pytest.raises(ValueError, match="retention must be"):
        core_retention(task_group(node("fct", meta={"retention": retention})))


def age(path, days):
    then = time.time() - days * 86400
    os.utime(path, (then, then))


def test_delete_old_files(tmp_path):
    (tmp_path / "old_run").mkdir()
    old_log = tmp_path / "old_run" / "attempt.log"
    old_log.write_text("old")
    age(old_log, 100)
    (tmp_path / "new_run").mkdir()
    new_log = tmp_path / "new_run" / "attempt.log"
    new_log.write_text("new")
    (tmp_path / "emptied").mkdir()
    age(tmp_path / "emptied", 2)
    (tmp_path / "just_created").mkdir()

    assert delete_old_files(str(tmp_path), 90) == 1
    assert not old_log.exists()
    assert new_log.exists()
    # Empty folders unchanged for a day are removed; a new empty one may be about to
    # receive a log, and deleting old_run's file has just updated that folder.
    assert not (tmp_path / "emptied").exists()
    assert (tmp_path / "just_created").exists()
    assert (tmp_path / "old_run").exists()
