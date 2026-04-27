"""Simple I/O helpers for reading and writing common data formats."""

import csv
import json
from pathlib import Path
from typing import Any, Dict, List, Union


def read_json(path: Union[str, Path]) -> List[Dict[str, Any]]:
    """Read a JSON file and return a list of records."""
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        return data
    return [data]


def write_json(data: List[Dict[str, Any]], path: Union[str, Path], indent: int = 2) -> None:
    """Write a list of records to a JSON file."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=indent, default=str)


def read_csv(path: Union[str, Path], delimiter: str = ",") -> List[Dict[str, Any]]:
    """Read a CSV file and return a list of records as dicts."""
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        return [dict(row) for row in reader]


def write_csv(
    data: List[Dict[str, Any]],
    path: Union[str, Path],
    fieldnames: List[str] = None,
    delimiter: str = ",",
) -> None:
    """Write a list of records to a CSV file."""
    if not data:
        Path(path).write_text("", encoding="utf-8")
        return
    fields = fieldnames or list(data[0].keys())
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, delimiter=delimiter, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(data)


def read_jsonl(path: Union[str, Path]) -> List[Dict[str, Any]]:
    """Read a newline-delimited JSON file."""
    records = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def write_jsonl(data: List[Dict[str, Any]], path: Union[str, Path]) -> None:
    """Write records to a newline-delimited JSON file."""
    with open(path, "w", encoding="utf-8") as f:
        for record in data:
            f.write(json.dumps(record, default=str) + "\n")
