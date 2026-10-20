"""Loads CSV files from object storage into PostgreSQL tables.

Storage-agnostic, the source URI's scheme selects the backend:
 - s3:// for S3-compatible storage
 - gs:// for Google Cloud Storage
 - file:// for a local folder
Switching storage is a configuration change in the DAG.
"""

from __future__ import annotations

import hashlib
import logging
from typing import BinaryIO

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import ObjectStoragePath
from airflow.sdk.exceptions import AirflowFailException
from psycopg import Cursor, DataError, sql

from lib.ingestion import load_log, schema_drift

log = logging.getLogger(__name__)

CHUNK_SIZE = 1024 * 1024

# Lineage columns added to every loaded table, after the source columns.
METADATA_COLUMNS = {
    "_source_file": "text not null",
    "_source_row": "bigint not null",
    "_loaded_at": "timestamptz not null",
    "_load_id": "bigint not null",  # the audit.load_log entry of the load
}


def load_csv(
    *,
    source_uri: str,
    source_conn_id: str,
    postgres_conn_id: str,
    table: str,
    columns: list[str],
) -> int:
    """Loads the rows of a CSV file into a table, creating the table if it does not exist.

    Idempotent: rows previously loaded from the same file are replaced, so re-running
    a load never duplicates data. A file identical to the one of its latest successful
    load, whose rows are still in the table, is not loaded again: the load is logged as
    skipped, and nothing downstream changes. Concurrent loads of the same file into the same table
    run one after the other.

    Every source column is created as text, so malformed values cannot break the load;
    typing happens downstream. Each row also records its source file, its position in
    the file, the load time and the load's entry in audit.load_log, where every
    attempt is logged. The file is streamed into COPY in chunks, so memory use
    does not grow with file size. Returns the number of rows loaded.

    Columns are matched by name, so their order in the file does not matter. Extra
    columns in the file are left out and recorded in the load's schema_drift; a missing
    expected column, or an existing table with other columns, fails the load (see
    schema_drift.py).

    A file whose data or structure cannot be loaded (e.g. a malformed CSV) raises
    AirflowFailException, so the task fails without retrying; other errors, including
    a file that has not been delivered (FileNotFoundError), are raised as they are and
    retried.
    """
    schema, name = table.split(".")
    table_id = sql.Identifier(schema, name)
    incoming_id = sql.Identifier("_incoming")
    column_list = sql.SQL(", ").join(map(sql.Identifier, columns))
    source = ObjectStoragePath(source_uri, conn_id=source_conn_id)
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    load_id = load_log.start(hook, target_table=table, source_file=source_uri)
    try:
        if not source.exists():
            # Retried, like other errors: the file may only be late.
            raise FileNotFoundError(f"{source_uri} has not been delivered")
        # The connection block commits on success, rolls back on error, then closes the
        # connection: everything below runs in one transaction.
        with hook.get_conn() as conn:
            with conn.cursor() as cur, source.open("rb", block_size=CHUNK_SIZE) as raw_stream:
                # Hashes the bytes as they are read, so the file is read only once.
                stream = HashingStream(raw_stream)
                # Serialises loads of the same file into the same table: without it, two
                # concurrent loads could both delete before either inserts, leaving both
                # copies. Released automatically when the transaction ends.
                cur.execute(
                    "select pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (f"{table}|{source_uri}",),
                )

                # Compare the file and the table with the expected columns before
                # touching any data. Reading the header leaves the stream at the data.
                header = schema_drift.read_header(stream)
                extra_columns = schema_drift.check_header(header, columns, list(METADATA_COLUMNS))
                schema_drift.check_table(cur, schema, name, columns + list(METADATA_COLUMNS))

                create_raw_table(cur, table, columns)

                # COPY into a temporary table with the file's own columns, then insert
                # the expected ones by name, adding the metadata. The identity column
                # numbers the rows in file order; the table is dropped when the
                # transaction ends. The header was already read, so COPY gets data only.
                header_columns = [sql.Identifier(c) for c in header]
                cur.execute(
                    sql.SQL(
                        "create temp table {} (_source_row bigint generated always as identity, {})"
                        " on commit drop"
                    ).format(
                        incoming_id,
                        sql.SQL(", ").join(sql.SQL("{} text").format(c) for c in header_columns),
                    )
                )
                copy_sql = sql.SQL(
                    "copy {} ({}) from stdin with (format csv, header false, encoding 'UTF8')"
                ).format(incoming_id, sql.SQL(", ").join(header_columns))
                with cur.copy(copy_sql) as copy:
                    while chunk := stream.read(CHUNK_SIZE):
                        copy.write(chunk)
                checksum = stream.hexdigest()

                # A file identical to its latest successful load, whose rows are still
                # here, changes nothing: skip it. Within the advisory lock, so two loads
                # of the file cannot both decide it changed.
                identical_to = _identical_load(cur, table, source_uri, checksum)
                if identical_to is not None:
                    load_log.skip(cur, load_id, schema_drift=extra_columns, checksum=checksum)
                    log.info(
                        "Load %s: %s is identical to load %s, already in %s: skipped",
                        load_id,
                        source_uri,
                        identical_to,
                        table,
                    )
                    return 0

                # Replace the rows of any earlier load of this file. Same transaction as the
                # insert, so readers never see the table without them.
                cur.execute(
                    sql.SQL("delete from {} where _source_file = %s").format(table_id),
                    (source_uri,),
                )
                replaced = cur.rowcount

                # now() is the transaction's start time, so all rows of a load share it.
                cur.execute(
                    sql.SQL(
                        "insert into {} ({}, _source_file, _source_row, _loaded_at, _load_id)"
                        " select {}, %s::text, _source_row, now(), %s::bigint"
                        " from {} order by _source_row"
                    ).format(table_id, column_list, column_list, incoming_id),
                    (source_uri, load_id),
                )
                rows = cur.rowcount

                load_log.succeed(
                    cur,
                    load_id,
                    rows_loaded=rows,
                    rows_replaced=replaced,
                    schema_drift=extra_columns,
                    checksum=checksum,
                )
    except Exception as error:
        load_log.fail(hook, load_id, error)
        if isinstance(error, DataError):
            # Bad data (e.g. a malformed CSV) fails the same way on every attempt:
            # fail the task without retrying.
            raise AirflowFailException(f"Invalid data in {source_uri}: {error}") from error
        raise

    log.info(
        "Load %s: loaded %s rows from %s into %s, replacing %s",
        load_id,
        rows,
        source_uri,
        table,
        replaced,
    )
    return rows


def create_raw_table(cur: Cursor, table: str, columns: list[str]) -> None:
    """Creates a raw table, if it does not exist: the source columns as text, then the
    lineage columns, with the indexes that keep loads and incremental reads fast."""
    schema, name = table.split(".")
    table_id = sql.Identifier(schema, name)
    definitions = [sql.SQL("{} text").format(sql.Identifier(c)) for c in columns] + [
        sql.SQL("{} {}").format(sql.Identifier(c), sql.SQL(definition))
        for c, definition in METADATA_COLUMNS.items()
    ]
    cur.execute(
        sql.SQL("create table if not exists {} ({})").format(table_id, sql.SQL(", ").join(definitions))
    )
    # Keep the loader's delete of an earlier load fast as the table grows, and the
    # reads of incremental models downstream, which select the loads since their last run.
    for column in ("_source_file", "_load_id"):
        cur.execute(
            sql.SQL("create index if not exists {} on {} ({})").format(
                sql.Identifier(f"{name}_{column}_idx"), table_id, sql.Identifier(column)
            )
        )


class HashingStream:
    """Wraps a binary stream, computing the SHA-256 of the bytes read through it."""

    def __init__(self, stream: BinaryIO):
        self._stream = stream
        self._hash = hashlib.sha256()

    def read(self, size: int = -1) -> bytes:
        data = self._stream.read(size)
        self._hash.update(data)
        return data

    # Without a size: object storage files (fsspec's) accept none.
    def readline(self) -> bytes:
        data = self._stream.readline()
        self._hash.update(data)
        return data

    def hexdigest(self) -> str:
        return self._hash.hexdigest()


def _identical_load(cur: Cursor, table: str, source_uri: str, checksum: str) -> int | None:
    """The latest successful load of the file into the table, if its checksum is the same
    and its rows are still in the table (retention, for instance, may have expired them)."""
    latest = load_log.latest_success(cur, target_table=table, source_file=source_uri)
    if latest is None or latest[1] != checksum:
        return None
    schema, name = table.split(".")
    cur.execute(
        sql.SQL("select exists (select 1 from {} where _load_id = %s)").format(sql.Identifier(schema, name)),
        (latest[0],),
    )
    return latest[0] if cur.fetchone()[0] else None
