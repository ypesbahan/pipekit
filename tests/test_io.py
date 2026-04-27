"""Tests for pipekit I/O helpers."""

import json
import pytest
from pathlib import Path
from pipekit.io import read_json, write_json, read_csv, write_csv, read_jsonl, write_jsonl

SAMPLE = [
    {"id": 1, "name": "alice", "score": 95},
    {"id": 2, "name": "bob", "score": 80},
]


@pytest.fixture
def tmp_path_dir(tmp_path):
    return tmp_path


def test_write_and_read_json(tmp_path):
    path = tmp_path / "data.json"
    write_json(SAMPLE, path)
    result = read_json(path)
    assert result == SAMPLE


def test_read_json_single_object(tmp_path):
    path = tmp_path / "single.json"
    path.write_text(json.dumps({"key": "value"}), encoding="utf-8")
    result = read_json(path)
    assert result == [{"key": "value"}]


def test_write_and_read_csv(tmp_path):
    path = tmp_path / "data.csv"
    write_csv(SAMPLE, path)
    result = read_csv(path)
    assert len(result) == 2
    assert result[0]["name"] == "alice"
    assert result[1]["name"] == "bob"


def test_write_csv_empty_data(tmp_path):
    path = tmp_path / "empty.csv"
    write_csv([], path)
    assert path.read_text(encoding="utf-8") == ""


def test_write_csv_custom_fieldnames(tmp_path):
    path = tmp_path / "partial.csv"
    write_csv(SAMPLE, path, fieldnames=["id", "name"])
    result = read_csv(path)
    assert "score" not in result[0]
    assert "id" in result[0]


def test_write_and_read_jsonl(tmp_path):
    path = tmp_path / "data.jsonl"
    write_jsonl(SAMPLE, path)
    result = read_jsonl(path)
    assert result == SAMPLE


def test_read_jsonl_skips_blank_lines(tmp_path):
    path = tmp_path / "with_blanks.jsonl"
    path.write_text(
        '{"a": 1}\n\n{"a": 2}\n',
        encoding="utf-8",
    )
    result = read_jsonl(path)
    assert len(result) == 2


def test_json_roundtrip_preserves_types(tmp_path):
    data = [{"x": 1, "y": 3.14, "z": True, "w": None}]
    path = tmp_path / "types.json"
    write_json(data, path)
    result = read_json(path)
    assert result[0]["x"] == 1
    assert result[0]["z"] is True
    assert result[0]["w"] is None
