"""Shared building blocks for validating configs with pydantic."""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, ConfigDict, StringConstraints

# A lowercase SQL identifier, e.g. a schema, table or column name.
IDENTIFIER = r"[a-z_][a-z0-9_]*"
Identifier = Annotated[str, StringConstraints(pattern=rf"^{IDENTIFIER}$")]


class StrictModel(BaseModel):
    # Unknown keys are errors, so a misspelt key is not silently ignored.
    model_config = ConfigDict(extra="forbid", frozen=True)
