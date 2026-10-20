"""Partitioning of raw tables by load time (_loaded_at), per day or per month, or none.

Each partition holds the rows loaded in one period, e.g. raw.orders_p20261020 for
20 October 2026 (UTC) or raw.orders_p202610 for October. Retention then drops whole
partitions, and each partition's indexes stay small. Partitions are created ahead, for
the current period and the next one, so a load that starts just before the end of a
period still has a partition for its rows when it commits.

How a table is partitioned cannot change once it exists: a table that differs from its
pipeline's config fails the load, asking for it to be migrated.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal

from psycopg import Cursor, sql

from lib.ingestion.schema_drift import SchemaDriftError
from lib.warehouse import TableName

PartitionBy = Literal["day", "month", "none"]

# Postgres truncates longer identifiers, which could make two partitions' names collide.
MAX_IDENTIFIER_LENGTH = 63
SUFFIX_FORMATS = {"day": "%Y%m%d", "month": "%Y%m"}


@dataclass(frozen=True)
class Period:
    """A partition's period: rows loaded from `start` (inclusive) to `end` (exclusive)."""

    name: str
    start: datetime
    end: datetime


def period(table_name: str, when: datetime, partition_by: PartitionBy) -> Period:
    """The period, and its partition's name, of a time."""
    when = when.astimezone(UTC)
    if partition_by == "day":
        start = when.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
    elif partition_by == "month":
        start = when.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end = (start + timedelta(days=32)).replace(day=1)
    else:
        raise ValueError(f"{table_name} is not partitioned")
    name = f"{table_name}_p{start.strftime(SUFFIX_FORMATS[partition_by])}"
    if len(name) > MAX_IDENTIFIER_LENGTH:
        raise ValueError(f"the partition name {name} is longer than {MAX_IDENTIFIER_LENGTH} characters")
    return Period(name, start, end)


def partitioning_of(cur: Cursor, schema: str, name: str) -> tuple[str, list[str]]:
    """How an existing table is partitioned, from the database, with its partitions:
    "missing" (no such table), "none" (not partitioned), "day" or "month" (from its
    partitions' names), "empty" (partitioned, no partitions yet) or "mixed"."""
    cur.execute(
        "select c.relkind from pg_class c join pg_namespace n on n.oid = c.relnamespace"
        " where n.nspname = %s and c.relname = %s",
        (schema, name),
    )
    row = cur.fetchone()
    if row is None:
        return "missing", []
    if row[0] != "p":
        return "none", []
    cur.execute(
        "select child.relname from pg_inherits i"
        " join pg_class parent on parent.oid = i.inhparent"
        " join pg_namespace n on n.oid = parent.relnamespace"
        " join pg_class child on child.oid = i.inhrelid"
        " where n.nspname = %s and parent.relname = %s order by child.relname",
        (schema, name),
    )
    partitions = [r[0] for r in cur.fetchall()]
    if not partitions:
        return "empty", []
    # The digits after "<table>_p": 8 for a day, 6 for a month.
    granularities = {{8: "day", 6: "month"}.get(len(p) - len(name) - 2, "mixed") for p in partitions}
    return (granularities.pop() if len(granularities) == 1 else "mixed"), partitions


def check_partitioning(cur: Cursor, table: str, partition_by: PartitionBy) -> None:
    """Fails if an existing table is partitioned otherwise than its config says."""
    target = TableName.parse(table)
    existing, _ = partitioning_of(cur, target.schema, target.name)
    if existing == "missing" or existing == partition_by or (existing == "empty" and partition_by != "none"):
        return
    actual = {"none": "not partitioned", "empty": "partitioned", "mixed": "partitioned by mixed periods"}
    raise SchemaDriftError(
        f"{table} is {actual.get(existing, f'partitioned by {existing}')}, but its pipeline config says"
        f" partition_by: {partition_by}. Migrate the table first."
    )


def create_partitions(cur: Cursor, table: str, partition_by: PartitionBy, now: datetime) -> None:
    """Creates the partitions of the current period and the next one, if missing."""
    target = TableName.parse(table)
    current = period(target.name, now, partition_by)
    for p in (current, period(target.name, current.end, partition_by)):
        # Literals, not query parameters: Postgres does not accept parameters in DDL.
        cur.execute(
            sql.SQL("create table if not exists {} partition of {} for values from ({}) to ({})").format(
                target.in_schema(p.name),
                target.identifier,
                sql.Literal(p.start.isoformat()),
                sql.Literal(p.end.isoformat()),
            )
        )


def expired_partitions(cur: Cursor, table: str, cutoff: datetime) -> list[str]:
    """The partitions of a table whose whole period ended before `cutoff`."""
    target = TableName.parse(table)
    granularity, partitions = partitioning_of(cur, target.schema, target.name)
    if granularity not in SUFFIX_FORMATS:
        return []
    expired = []
    for partition in partitions:
        suffix = partition[len(target.name) + 2 :]
        start = datetime.strptime(suffix, SUFFIX_FORMATS[granularity]).replace(tzinfo=UTC)
        if period(target.name, start, granularity).end <= cutoff:
            expired.append(partition)
    return expired
