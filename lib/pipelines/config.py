"""Pipeline configs: one YAML file per pipeline in pipelines/, declaring when it runs,
what it loads and which dbt models it then runs. Validated when the DAG is parsed, so
a broken config fails its own DAG with a readable import error.

Each section is defined next to the code that uses it: ingestion in
lib/ingestion/config.py, transformation in lib/transformation/config.py.
"""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path

import yaml
from pydantic import ValidationError, field_validator

from lib.ingestion.config import IngestionConfig
from lib.transformation.config import TransformationConfig
from lib.validation import StrictModel

# The pipelines/ folder at the repo root, next to lib/.
PIPELINES_PATH = Path(__file__).resolve().parents[2] / "pipelines"


class PipelineConfig(StrictModel):
    description: str
    # The team that owns the pipeline: its tasks' owner, and whose alert webhook
    # (connection alerts_<owner>) failures go to; see lib/orchestration/callbacks.py.
    owner: str
    schedule: str
    start_date: date
    ingestion: IngestionConfig
    transformation: TransformationConfig = TransformationConfig()

    @field_validator("owner")
    @classmethod
    def _owner_name(cls, owner: str) -> str:
        if not re.fullmatch(r"[a-z][a-z0-9-]*", owner):
            raise ValueError("must be lowercase letters, digits and hyphens, e.g. sales-data")
        return owner


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
