"""The ingestion section of a pipeline config: which file is loaded into which table."""

from __future__ import annotations

import re

from pydantic import Field, field_validator

from lib.validation import IDENTIFIER, Identifier, StrictModel


class Source(StrictModel):
    conn_id: str
    uri: str


class Target(StrictModel):
    conn_id: str
    table: str

    @field_validator("table")
    @classmethod
    def _schema_and_table(cls, table: str) -> str:
        if not re.fullmatch(rf"{IDENTIFIER}\.{IDENTIFIER}", table):
            raise ValueError("must be <schema>.<table>, in lowercase, e.g. raw.customer_transactions")
        return table


class IngestionConfig(StrictModel):
    source: Source
    target: Target
    # The columns the file must have; see schema_drift.py.
    columns: list[Identifier] = Field(min_length=1)
    # Days the raw table keeps a file's rows after its latest load. Unset, as long as
    # the core models built on it need (see lib/pipelines/builder.py); it cannot be
    # shorter, or a full refresh could not rebuild their history.
    retention_days: int | None = Field(default=None, gt=0)

    @field_validator("columns")
    @classmethod
    def _no_duplicates(cls, columns: list[str]) -> list[str]:
        duplicates = sorted({c for c in columns if columns.count(c) > 1})
        if duplicates:
            raise ValueError(f"duplicate columns: {', '.join(duplicates)}")
        return columns
