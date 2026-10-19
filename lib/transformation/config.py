"""The transformation section of a pipeline config: which dbt models run after the load."""

from __future__ import annotations

from pydantic import Field

from lib.validation import StrictModel


class TransformationConfig(StrictModel):
    # dbt selection syntax, e.g. ["source:raw.customer_transactions+"]. Unset, the
    # pipeline runs the models built from its target table (see lib/pipelines/builder.py).
    select: list[str] | None = Field(default=None, min_length=1)
