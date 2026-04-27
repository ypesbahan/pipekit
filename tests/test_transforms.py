"""Tests for built-in transform functions."""

import pytest
from pipekit.transforms import (
    map_field,
    filter_records,
    rename_field,
    drop_fields,
    add_field,
    flatten,
    deduplicate,
)

SAMPLE_DATA = [
    {"name": "alice", "age": 30, "score": 85},
    {"name": "bob", "age": 25, "score": 90},
    {"name": "charlie", "age": 35, "score": 70},
]


def test_map_field_transforms_values():
    result = map_field("name", str.upper)(SAMPLE_DATA)
    assert result[0]["name"] == "ALICE"
    assert result[1]["name"] == "BOB"


def test_map_field_preserves_other_fields():
    result = map_field("age", lambda x: x + 1)(SAMPLE_DATA)
    assert result[0]["age"] == 31
    assert result[0]["name"] == "alice"


def test_filter_records_removes_non_matching():
    result = filter_records(lambda r: r["age"] > 28)(SAMPLE_DATA)
    assert len(result) == 2
    assert all(r["age"] > 28 for r in result)


def test_filter_records_empty_result():
    result = filter_records(lambda r: r["age"] > 100)(SAMPLE_DATA)
    assert result == []


def test_rename_field_changes_key():
    result = rename_field("name", "full_name")(SAMPLE_DATA)
    assert "full_name" in result[0]
    assert "name" not in result[0]
    assert result[0]["full_name"] == "alice"


def test_rename_field_ignores_missing_key():
    result = rename_field("nonexistent", "new_field")(SAMPLE_DATA)
    assert "new_field" not in result[0]


def test_drop_fields_removes_keys():
    result = drop_fields("age", "score")(SAMPLE_DATA)
    assert "age" not in result[0]
    assert "score" not in result[0]
    assert "name" in result[0]


def test_add_field_computes_new_value():
    result = add_field("senior", lambda r: r["age"] >= 30)(SAMPLE_DATA)
    assert result[0]["senior"] is True
    assert result[1]["senior"] is False


def test_flatten_reduces_nesting():
    nested = [[1, 2], [3, 4], [5]]
    assert flatten(nested) == [1, 2, 3, 4, 5]


def test_deduplicate_removes_exact_duplicates():
    data = [*SAMPLE_DATA, SAMPLE_DATA[0]]
    result = deduplicate()(data)
    assert len(result) == 3


def test_deduplicate_by_key():
    data = [
        {"id": 1, "val": "a"},
        {"id": 1, "val": "b"},
        {"id": 2, "val": "c"},
    ]
    result = deduplicate(key="id")(data)
    assert len(result) == 2
    assert result[0]["val"] == "a"
