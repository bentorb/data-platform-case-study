"""Loads CSV files from object storage into PostgreSQL tables.

Storage-agnostic, the source URI's scheme selects the backend:
 - s3:// for S3-compatible storage
 - gs:// for Google Cloud Storage
 - file:// for a local folder
Switching storage is a configuration change in the DAG.
"""

from __future__ import annotations

import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import ObjectStoragePath
from psycopg import sql

from lib.ingestion import load_log

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
    a load never duplicates data. Concurrent loads of the same file into the same table
    run one after the other.

    Every source column is created as text, so malformed values cannot break the load;
    typing happens downstream. Each row also records its source file, its position in
    the file, the load time and the load's entry in audit.load_log, where every
    attempt is logged. The file is streamed into COPY in chunks, so memory use
    does not grow with file size. Returns the number of rows loaded.
    """
    schema, name = table.split(".")
    table_id = sql.Identifier(schema, name)
    index_id = sql.Identifier(f"{name}__source_file_idx")
    incoming_id = sql.Identifier("_incoming")
    column_list = sql.SQL(", ").join(map(sql.Identifier, columns))
    text_columns = [sql.SQL("{} text").format(sql.Identifier(c)) for c in columns]
    metadata_columns = [
        sql.SQL("{} {}").format(sql.Identifier(c), sql.SQL(definition))
        for c, definition in METADATA_COLUMNS.items()
    ]
    source = ObjectStoragePath(source_uri, conn_id=source_conn_id)
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    load_id = load_log.start(hook, target_table=table, source_file=source_uri)
    try:
        # The connection block commits on success, rolls back on error, then closes the
        # connection: everything below runs in one transaction.
        with hook.get_conn() as conn:
            with conn.cursor() as cur, source.open("rb", block_size=CHUNK_SIZE) as stream:
                # Serialises loads of the same file into the same table: without it, two
                # concurrent loads could both delete before either inserts, leaving both
                # copies. Released automatically when the transaction ends.
                cur.execute(
                    "select pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (f"{table}|{source_uri}",),
                )

                cur.execute(
                    sql.SQL("create table if not exists {} ({})").format(
                        table_id, sql.SQL(", ").join(text_columns + metadata_columns)
                    )
                )
                # Keeps the delete below fast as the table grows.
                cur.execute(
                    sql.SQL("create index if not exists {} on {} (_source_file)").format(
                        index_id, table_id
                    )
                )

                # COPY into a temporary table first, so the metadata can be added on insert.
                # The identity column numbers the rows in file order; the table is dropped
                # when the transaction ends.
                cur.execute(
                    sql.SQL(
                        "create temp table {} (_source_row bigint generated always as identity, {})"
                        " on commit drop"
                    ).format(incoming_id, sql.SQL(", ").join(text_columns))
                )
                copy_sql = sql.SQL(
                    "copy {} ({}) from stdin with (format csv, header true, encoding 'UTF8')"
                ).format(incoming_id, column_list)
                with cur.copy(copy_sql) as copy:
                    while chunk := stream.read(CHUNK_SIZE):
                        copy.write(chunk)

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

                load_log.succeed(cur, load_id, rows_loaded=rows, rows_replaced=replaced)
    except Exception as error:
        load_log.fail(hook, load_id, error)
        raise

    log.info(
        "Load %s: loaded %s rows from %s into %s, replacing %s",
        load_id, rows, source_uri, table, replaced,
    )
    return rows
