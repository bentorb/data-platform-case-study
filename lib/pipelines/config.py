"""Pipeline configs: one YAML file per pipeline in pipelines/, declaring where its data
comes from and where it is loaded. Validated when the DAG is parsed, so a broken config
fails its own DAG with a readable import error.
"""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path
from typing import Annotated

import yaml
from pydantic import BaseModel, ConfigDict, Field, StringConstraints, ValidationError, field_validator

# The pipelines/ folder at the repo root, next to lib/.
PIPELINES_PATH = Path(__file__).resolve().parents[2] / "pipelines"

IDENTIFIER = r"[a-z_][a-z0-9_]*"
Identifier = Annotated[str, StringConstraints(pattern=rf"^{IDENTIFIER}$")]


class _Config(BaseModel):
    # Unknown keys are errors, so a misspelt key is not silently ignored.
    model_config = ConfigDict(extra="forbid", frozen=True)


class Source(_Config):
    conn_id: str
    uri: str


class Target(_Config):
    conn_id: str
    table: str

    @field_validator("table")
    @classmethod
    def _schema_and_table(cls, table: str) -> str:
        if not re.fullmatch(rf"{IDENTIFIER}\.{IDENTIFIER}", table):
            raise ValueError("must be <schema>.<table>, in lowercase, e.g. raw.customer_transactions")
        return table


class PipelineConfig(_Config):
    description: str
    owner: str
    schedule: str
    start_date: date
    source: Source
    target: Target
    columns: list[Identifier] = Field(min_length=1)

    @field_validator("columns")
    @classmethod
    def _no_duplicates(cls, columns: list[str]) -> list[str]:
        duplicates = sorted({c for c in columns if columns.count(c) > 1})
        if duplicates:
            raise ValueError(f"duplicate columns: {', '.join(duplicates)}")
        return columns


class InvalidConfigError(ValueError):
    """A pipeline config is missing, is not valid YAML, or does not match PipelineConfig."""


def config_path(name: str) -> Path:
    return PIPELINES_PATH / f"{name}.yml"


def load_config(name: str) -> PipelineConfig:
    """Reads and validates the config of the pipeline `name` (pipelines/<name>.yml)."""
    path = config_path(name)
    try:
        return PipelineConfig.model_validate(yaml.safe_load(path.read_text()))
    except (OSError, yaml.YAMLError, ValidationError) as error:
        raise InvalidConfigError(f"Invalid pipeline config {path}:\n{error}") from error
