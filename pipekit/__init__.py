"""pipekit — A lightweight Python library for defining and running local ETL pipelines."""

from pipekit.pipeline import Pipeline, Step, PipelineError
from pipekit.runners import SequentialRunner, RunResult
from pipekit.context import PipelineContext
from pipekit.logging import PipelineLogger
from pipekit.validators import ValidationError, require_fields, validate_field
from pipekit.transforms import map_field, filter_records, rename_field
from pipekit.io import read_json, write_json, read_csv, write_csv, read_jsonl, write_jsonl
from pipekit.retry import with_retry, RetryError

__all__ = [
    # Core
    "Pipeline",
    "Step",
    "PipelineError",
    # Runners
    "SequentialRunner",
    "RunResult",
    # Context
    "PipelineContext",
    # Logging
    "PipelineLogger",
    # Validators
    "ValidationError",
    "require_fields",
    "validate_field",
    # Transforms
    "map_field",
    "filter_records",
    "rename_field",
    # IO
    "read_json",
    "write_json",
    "read_csv",
    "write_csv",
    "read_jsonl",
    "write_jsonl",
    # Retry
    "with_retry",
    "RetryError",
]

__version__ = "0.1.0"
