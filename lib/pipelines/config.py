"""Pipeline configs: one YAML file per pipeline in pipelines/, declaring when it runs,
what it loads and which dbt models it then runs. Validated when the DAG is parsed, so
a broken config fails its own DAG with a readable import error.

Each section is defined next to the code that uses it: ingestion in
lib/ingestion/config.py, transformation in lib/transformation/config.py.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import yaml
from pydantic import ValidationError

from lib.ingestion.config import IngestionConfig
from lib.transformation.config import TransformationConfig
from lib.validation import StrictModel

# The pipelines/ folder at the repo root, next to lib/.
PIPELINES_PATH = Path(__file__).resolve().parents[2] / "pipelines"


class PipelineConfig(StrictModel):
    description: str
    owner: str
    schedule: str
    start_date: date
    ingestion: IngestionConfig
    transformation: TransformationConfig = TransformationConfig()


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
