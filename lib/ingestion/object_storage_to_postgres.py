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

log = logging.getLogger(__name__)

CHUNK_SIZE = 1024 * 1024

# Lineage columns added to every loaded table, after the source columns.
METADATA_COLUMNS = {
    "_source_file": "text not null",
    "_source_row": "bigint not null",
    "_loaded_at": "timestamptz not null",
}


def load_csv(
    *,
    source_uri: str,
    source_conn_id: str,
    postgres_conn_id: str,
    table: str,
    columns: list[str],
) -> int:
    """Appends the rows of a CSV file to a table, creating the table if it does not exist.

    Every source column is created as text, so malformed values cannot break the load;
    typing happens downstream. Each row also records its source file, its position in
    the file and the load time. The file is streamed into COPY in chunks, so memory use
    does not grow with file size. Returns the number of rows loaded.
    """
    schema, name = table.split(".")
    table_id = sql.Identifier(schema, name)
    incoming_id = sql.Identifier("_incoming")
    column_list = sql.SQL(", ").join(map(sql.Identifier, columns))
    text_columns = [sql.SQL("{} text").format(sql.Identifier(c)) for c in columns]
    metadata_columns = [
        sql.SQL("{} {}").format(sql.Identifier(c), sql.SQL(definition))
        for c, definition in METADATA_COLUMNS.items()
    ]
    source = ObjectStoragePath(source_uri, conn_id=source_conn_id)

    # The connection block commits on success, rolls back on error, then closes the
    # connection: everything below runs in one transaction.
    with PostgresHook(postgres_conn_id=postgres_conn_id).get_conn() as conn:
        with conn.cursor() as cur, source.open("rb", block_size=CHUNK_SIZE) as stream:
            cur.execute(
                sql.SQL("create table if not exists {} ({})").format(
                    table_id, sql.SQL(", ").join(text_columns + metadata_columns)
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

            # now() is the transaction's start time, so all rows of a load share it.
            cur.execute(
                sql.SQL(
                    "insert into {} ({}, _source_file, _source_row, _loaded_at)"
                    " select {}, %s::text, _source_row, now() from {} order by _source_row"
                ).format(table_id, column_list, column_list, incoming_id),
                (source_uri,),
            )
            rows = cur.rowcount

    log.info("Loaded %s rows from %s into %s", rows, source_uri, table)
    return rows
