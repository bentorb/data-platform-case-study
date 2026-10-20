"""Pipeline configs (lib/pipelines/config.py): what a valid config looks like, and the
errors that invalid ones give."""

import pytest
import yaml

from lib.pipelines import config as pipeline_config
from lib.pipelines.config import InvalidConfigError, load_config

VALID = {
    "description": "Loads orders",
    "owner": "sales-data",
    "schedule": "@daily",
    "start_date": "2026-01-01",
    "ingestion": {
        "source": {"conn_id": "landing", "uri": "s3://landing/orders/{{ ds }}.csv"},
        "target": {"conn_id": "warehouse", "table": "raw.orders"},
        "columns": ["order_id", "amount"],
    },
}


@pytest.fixture
def write_config(tmp_path, monkeypatch):
    """Writes a config as pipelines/orders.yml in a temporary pipelines folder."""
    monkeypatch.setattr(pipeline_config, "PIPELINES_PATH", tmp_path)

    def write(data):
        (tmp_path / "orders.yml").write_text(yaml.safe_dump(data) if isinstance(data, dict) else data)

    return write


def with_changes(**changes):
    """VALID with some values replaced; a dotted key reaches into a section."""
    data = yaml.safe_load(yaml.safe_dump(VALID))
    for key, value in changes.items():
        *parents, last = key.split("__")
        section = data
        for parent in parents:
            section = section[parent]
        if value is None:
            section.pop(last)
        else:
            section[last] = value
    return data


def test_valid_config(write_config):
    write_config(VALID)
    config = load_config("orders")
    assert config.ingestion.target.table == "raw.orders"
    assert config.ingestion.retention_days is None
    # No transformation section: the builder selects the models built from the table.
    assert config.transformation.select is None


@pytest.mark.parametrize(
    "changes, message",
    [
        ({"shedule": "@daily"}, "Extra inputs are not permitted"),
        ({"schedule": None}, "Field required"),
        ({"owner": "Sales Data"}, "must be lowercase letters, digits and hyphens"),
        ({"ingestion__target__table": "orders"}, "must be <schema>.<table>"),
        ({"ingestion__columns": []}, "at least 1 item"),
        ({"ingestion__columns": ["order_id", "order_id"]}, "duplicate columns: order_id"),
        ({"ingestion__columns": ["Order Id"]}, "should match pattern"),
        ({"ingestion__retention_days": 0}, "greater than 0"),
        ({"transformation": {"select": []}}, "at least 1 item"),
    ],
)
def test_invalid_config(write_config, changes, message):
    write_config(with_changes(**changes))
    with pytest.raises(InvalidConfigError, match=message):
        load_config("orders")


def test_missing_config(write_config):
    with pytest.raises(InvalidConfigError, match="orders.yml"):
        load_config("orders")


def test_invalid_yaml(write_config):
    write_config("owner: [unclosed")
    with pytest.raises(InvalidConfigError, match="orders.yml"):
        load_config("orders")
