"""Partitioning of raw tables by load time (lib/ingestion/partitioning.py): periods and
their names, tables that differ from their config, and which partitions expire."""

from datetime import UTC, datetime, timedelta, timezone

import pytest

from lib.ingestion.partitioning import Period, check_partitioning, expired_partitions, period
from lib.ingestion.schema_drift import SchemaDriftError


def at(*args):
    return datetime(*args, tzinfo=UTC)


@pytest.mark.parametrize(
    "when, partition_by, name, start, end",
    [
        (at(2026, 10, 20, 13, 5), "day", "orders_p20261020", at(2026, 10, 20), at(2026, 10, 21)),
        (at(2026, 12, 31, 23, 59), "day", "orders_p20261231", at(2026, 12, 31), at(2027, 1, 1)),
        (at(2026, 10, 20, 13, 5), "month", "orders_p202610", at(2026, 10, 1), at(2026, 11, 1)),
        (at(2026, 12, 31, 23, 59), "month", "orders_p202612", at(2026, 12, 1), at(2027, 1, 1)),
        (at(2027, 1, 31), "month", "orders_p202701", at(2027, 1, 1), at(2027, 2, 1)),
    ],
)
def test_period(when, partition_by, name, start, end):
    assert period("orders", when, partition_by) == Period(name, start, end)


def test_periods_are_in_utc():
    # 01:30 on 21 October in UTC+2 is still 20 October in UTC.
    when = datetime(2026, 10, 21, 1, 30, tzinfo=timezone(timedelta(hours=2)))
    assert period("orders", when, "day").name == "orders_p20261020"


def test_a_partition_name_too_long_for_postgres():
    with pytest.raises(ValueError, match="longer than 63 characters"):
        period("t" * 60, at(2026, 10, 20), "day")


class FakeCatalog:
    """Answers partitioning's catalogue queries: the table's kind, then its partitions."""

    def __init__(self, relkind, partitions=()):
        self.answers = [[(relkind,)] if relkind else [], [(p,) for p in partitions]]

    def execute(self, query, params=None):
        self.result = self.answers.pop(0)

    def fetchone(self):
        return self.result[0] if self.result else None

    def fetchall(self):
        return self.result


@pytest.mark.parametrize(
    "relkind, partitions, partition_by",
    [
        (None, [], "day"),  # does not exist yet
        ("r", [], "none"),
        ("p", [], "day"),  # partitioned, no partitions yet
        ("p", ["orders_p20261020"], "day"),
        ("p", ["orders_p202610"], "month"),
    ],
)
def test_a_table_partitioned_as_configured(relkind, partitions, partition_by):
    check_partitioning(FakeCatalog(relkind, partitions), "raw.orders", partition_by)


@pytest.mark.parametrize(
    "relkind, partitions, partition_by, message",
    [
        ("r", [], "day", "not partitioned, but its pipeline config says partition_by: day"),
        (
            "p",
            ["orders_p202610"],
            "day",
            "partitioned by month, but its pipeline config says partition_by: day",
        ),
        (
            "p",
            ["orders_p20261020"],
            "none",
            "partitioned by day, but its pipeline config says partition_by: none",
        ),
        ("p", ["orders_p20261020", "orders_p202611"], "day", "partitioned by mixed periods"),
    ],
)
def test_a_table_partitioned_otherwise_fails(relkind, partitions, partition_by, message):
    with pytest.raises(SchemaDriftError, match=message):
        check_partitioning(FakeCatalog(relkind, partitions), "raw.orders", partition_by)


def test_only_partitions_whose_whole_period_is_older_expire():
    partitions = ["orders_p20261018", "orders_p20261019", "orders_p20261020", "orders_p20261021"]
    # The cutoff falls inside 20 October: that day's partition still has recent rows.
    assert expired_partitions(FakeCatalog("p", partitions), "raw.orders", at(2026, 10, 20, 6)) == [
        "orders_p20261018",
        "orders_p20261019",
    ]


def test_a_table_that_is_not_partitioned_has_no_partitions_to_expire():
    assert expired_partitions(FakeCatalog("r"), "raw.orders", at(2026, 10, 20)) == []
