"""Shared building blocks for code that works on warehouse tables."""

from __future__ import annotations

from dataclasses import dataclass

from psycopg import Cursor, sql


@dataclass(frozen=True)
class TableName:
    """A schema-qualified table name, e.g. raw.customer_transactions."""

    schema: str
    name: str

    @classmethod
    def parse(cls, table: str) -> TableName:
        schema, name = table.split(".")
        return cls(schema, name)

    @property
    def identifier(self) -> sql.Identifier:
        """The quoted name, for composing SQL."""
        return sql.Identifier(self.schema, self.name)

    @property
    def slug(self) -> str:
        """The name without a dot, e.g. for task IDs: raw_customer_transactions."""
        return f"{self.schema}_{self.name}"

    def in_schema(self, name: str) -> sql.Identifier:
        """Another table in the same schema, e.g. one of its partitions."""
        return sql.Identifier(self.schema, name)

    def __str__(self) -> str:
        return f"{self.schema}.{self.name}"


def advisory_xact_lock(cur: Cursor, key: str) -> None:
    """Takes the Postgres advisory lock named `key`, waiting while another transaction
    holds it, until this transaction ends. Keys share one space with the dbt build locks
    (dbt/macros/model_lock.sql), which are named after a model's relation, e.g.
    core.fct_transactions: keys for anything else must not look like a table name."""
    cur.execute("select pg_advisory_xact_lock(hashtextextended(%s, 0))", (key,))
