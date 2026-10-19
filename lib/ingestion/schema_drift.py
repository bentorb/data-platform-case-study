"""Schema drift checks: compare a file's header and an existing table with the columns
a load expects.

Missing, duplicate or unexpected columns raise SchemaDriftError, which fails the task
without retrying. Extra columns in a file are tolerated: they are reported, and the
loader leaves them out.
"""

from __future__ import annotations

import csv
import logging
from typing import BinaryIO

from airflow.sdk.exceptions import AirflowFailException
from psycopg import Cursor

log = logging.getLogger(__name__)


class SchemaDriftError(AirflowFailException):
    """A file or table does not have the expected columns. Retrying cannot fix it."""


def read_header(stream: BinaryIO) -> list[str]:
    """Reads the header line of a CSV stream, leaving the stream at the first data row."""
    line = stream.readline()
    if not line:
        raise SchemaDriftError("the file is empty: there is no header line")
    # utf-8-sig drops a byte-order mark, which some tools write at the start of a file.
    return [name.strip() for name in next(csv.reader([line.decode("utf-8-sig")]))]


def check_header(header: list[str], expected: list[str], reserved: list[str]) -> list[str]:
    """Checks a file's header against the expected columns, in any order. Returns the
    header's extra columns, which are not loaded. `reserved` are the names of the
    columns the loader adds, which a file cannot use."""
    duplicates = sorted({name for name in header if header.count(name) > 1})
    if duplicates or "" in header:
        raise SchemaDriftError(
            f"the header has duplicate or empty column names: {header}"
        )
    clashing = [name for name in header if name in reserved]
    if clashing:
        raise SchemaDriftError(
            f"the header uses names reserved for lineage columns: {', '.join(clashing)}"
        )
    missing = [name for name in expected if name not in header]
    if missing:
        raise SchemaDriftError(f"the header is missing expected columns: {', '.join(missing)}")
    extra = [name for name in header if name not in expected]
    if extra:
        log.warning("The header has columns that are not loaded: %s", ", ".join(extra))
    return extra


def check_table(cur: Cursor, schema: str, name: str, expected: list[str]) -> None:
    """Checks that an existing table has exactly the expected columns, in any order.
    A table that does not exist yet passes: the loader creates it."""
    cur.execute(
        "select column_name from information_schema.columns"
        " where table_schema = %s and table_name = %s",
        (schema, name),
    )
    existing = {row[0] for row in cur.fetchall()}
    if not existing:
        return
    missing = sorted(set(expected) - existing)
    unexpected = sorted(existing - set(expected))
    if missing or unexpected:
        raise SchemaDriftError(
            f"{schema}.{name} does not have the expected columns"
            f" (missing: {', '.join(missing) or 'none'};"
            f" not expected: {', '.join(unexpected) or 'none'})."
            " Migrate the table to the new columns before loading."
        )
