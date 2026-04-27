"""pipekit — A lightweight Python library for defining and running local ETL pipelines."""

from pipekit.pipeline import Pipeline, Step, PipelineError
from pipekit.transforms import (
    map_field,
    filter_records,
    rename_field,
    drop_fields,
    add_field,
    flatten,
    deduplicate,
)
from pipekit.io import (
    read_json,
    write_json,
    read_csv,
    write_csv,
    read_jsonl,
    write_jsonl,
)

__version__ = "0.1.0"
__all__ = [
    "Pipeline",
    "Step",
    "PipelineError",
    # transforms
    "map_field",
    "filter_records",
    "rename_field",
    "drop_fields",
    "add_field",
    "flatten",
    "deduplicate",
    # io
    "read_json",
    "write_json",
    "read_csv",
    "write_csv",
    "read_jsonl",
    "write_jsonl",
]
