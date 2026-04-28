"""pipekit — A lightweight Python library for defining and running local ETL pipelines."""

from pipekit.pipeline import Pipeline, Step, PipelineError
from pipekit.runners import SequentialRunner, RunResult
from pipekit.context import PipelineContext
from pipekit.transforms import (
    map_field,
    filter_records,
    rename_field,
)
from pipekit.validators import (
    require_fields,
    validate_field,
    coerce_field,
    ValidationError,
)
from pipekit.io import (
    read_json,
    write_json,
    read_csv,
    write_csv,
    read_jsonl,
)

__all__ = [
    # Pipeline core
    "Pipeline",
    "Step",
    "PipelineError",
    # Runners
    "SequentialRunner",
    "RunResult",
    # Context
    "PipelineContext",
    # Transforms
    "map_field",
    "filter_records",
    "rename_field",
    # Validators
    "require_fields",
    "validate_field",
    "coerce_field",
    "ValidationError",
    # IO
    "read_json",
    "write_json",
    "read_csv",
    "write_csv",
    "read_jsonl",
]
