"""Project integrity: rules every dbt model must follow, checked on dbt's manifest, the
final configuration of every model once the project-level, folder-level and model
settings are combined. `dbt parse` writes it without connecting to the warehouse.
"""

import json

import pytest


@pytest.fixture(scope="module")
def models(dbt_project):
    parsed = dbt_project("parse")
    assert parsed.returncode == 0, "dbt parse failed:\n" + parsed.stdout[-4000:]
    manifest = json.loads((dbt_project.project / "target" / "manifest.json").read_text())
    return {node["name"]: node for node in manifest["nodes"].values() if node["resource_type"] == "model"}


def test_there_are_models(models):
    assert models


def test_every_model_is_locked_while_it_is_built(models):
    """Every model takes its lock before its build and releases it after, both outside
    the model's transaction (macros/model_lock.sql): inside it, the lock would be
    released at the commit, before dbt drops its backup table."""
    problems = []
    for name, node in sorted(models.items()):
        for hook_type, macro in (("pre-hook", "lock_model(this)"), ("post-hook", "unlock_model(this)")):
            hooks = [h for h in node["config"][hook_type] if macro in h["sql"]]
            if not hooks:
                problems.append(f"{name}: no {macro} {hook_type}")
            elif any(h["transaction"] for h in hooks):
                problems.append(f"{name}: {macro} runs inside the transaction")
    assert problems == []


def core_models(models):
    return {name: node for name, node in models.items() if node["schema"] == "core"}


def column_meta(column):
    return (column.get("config") or {}).get("meta") or column.get("meta") or {}


def test_every_core_model_has_an_enforced_contract(models):
    """dbt creates core tables from their declared columns and types, and the publish
    copies that structure to live."""
    unenforced = [
        name for name, node in core_models(models).items() if not node["config"]["contract"]["enforced"]
    ]
    assert unenforced == []


def test_every_core_model_has_a_primary_key(models):
    """The publish syncs each live table on its model's primary key, and fails without one."""
    without_key = [
        name
        for name, node in core_models(models).items()
        if not any(c["type"] == "primary_key" for c in node["constraints"])
        and not any(
            c["type"] == "primary_key" for col in node["columns"].values() for c in col["constraints"]
        )
    ]
    assert without_key == []


def test_every_classification_is_known(models):
    """The publish grants columns by classification, and fails on an unknown one."""
    unknown = [
        f"{name}.{column}: {column_meta(col)['classification']!r}"
        for name, node in sorted(models.items())
        for column, col in node["columns"].items()
        if column_meta(col).get("classification", "internal") not in ("internal", "restricted")
    ]
    assert unknown == []


def test_every_retention_is_valid(models):
    """A retention is {column: <one of the model's columns>, days: <positive integer>}: the
    builder fails the DAG on an invalid one, and the expiry fails the build on a missing column."""
    invalid = []
    for name, node in sorted(models.items()):
        retention = (node["config"].get("meta") or {}).get("retention")
        if retention is None:
            continue
        days = retention.get("days") if isinstance(retention, dict) else None
        column = retention.get("column") if isinstance(retention, dict) else None
        if not (isinstance(days, int) and not isinstance(days, bool) and days > 0):
            invalid.append(f"{name}: days must be a positive integer, not {days!r}")
        if column not in node["columns"]:
            invalid.append(f"{name}: column {column!r} is not one of its columns")
    assert invalid == []


def test_every_model_and_column_is_described(models):
    """dbt's documentation is the catalogue consumers read."""
    undescribed = [name for name, node in sorted(models.items()) if not node["description"].strip()]
    undescribed += [
        f"{name}.{column}"
        for name, node in sorted(models.items())
        for column, col in node["columns"].items()
        if not col["description"].strip()
    ]
    assert undescribed == []
