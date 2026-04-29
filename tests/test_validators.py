"""Tests for pipekit.validators module."""

import pytest
from pipekit.validators import (
    require_fields,
    validate_field,
    coerce_field,
    ValidationError,
)


# --- require_fields ---

def test_require_fields_keeps_complete_records():
    records = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
    result = require_fields("name", "age")(records)
    assert result == records


def test_require_fields_drops_missing_field():
    records = [{"name": "Alice"}, {"name": "Bob", "age": 25}]
    result = require_fields("name", "age")(records)
    assert result == [{"name": "Bob", "age": 25}]


def test_require_fields_drops_none_value():
    records = [{"name": "Alice", "age": None}]
    result = require_fields("age")(records)
    assert result == []


def test_require_fields_strict_raises():
    records = [{"name": "Alice"}]
    with pytest.raises(ValidationError, match="missing required fields"):
        require_fields("age", strict=True)(records)


def test_require_fields_empty_input():
    assert require_fields("x")([])==[]


def test_require_fields_multiple_fields_all_missing():
    """All required fields missing should drop the record (or raise in strict mode)."""
    records = [{"unrelated": "value"}]
    result = require_fields("name", "age")(records)
    assert result == []


def test_require_fields_strict_raises_with_multiple_missing():
    """Strict mode should raise even when multiple fields are missing."""
    records = [{"unrelated": "value"}]
    with pytest.raises(ValidationError, match="missing required fields"):
        require_fields("name", "age", strict=True)(records)


# --- validate_field ---

def test_validate_field_keeps_valid_records():
    records = [{"score": 90}, {"score": 70}]
    result = validate_field("score", lambda v: v >= 60)(records)
    assert result == records


def test_validate_field_drops_invalid_records():
    records = [{"score": 90}, {"score": 40}]
    result = validate_field("score", lambda v: v >= 60)(records)
    assert result == [{"score": 90}]


def test_validate_field_strict_raises_default_message():
    records = [{"score": 10}]
    with pytest.raises(ValidationError, match="failed validation"):
        validate_field("score", lambda v: v >= 60, strict=True)(records)


def test_validate_field_strict_raises_custom_message():
    records = [{"score": 10}]
    with pytest.raises(ValidationError, match="too low"):
        validate_field("score", lambda v: v >= 60, strict=True, error_message="too low")(records)


def test_validate_field_missing_key_treated_as_invalid():
    records = [{"other": 1}]
    result = validate_field("score", lambda v: v is not None)(records)
    assert result == []


# --- coerce_field ---

def test_coerce_field_converts_values():
    records = [{"age": "30"}, {"age": "25"}]
    result = coerce_field("age", int)(records)
    assert result == [{"age": 30}, {"age": 25}]


def test_coerce_field_drops_on_error_by_default():
    records = [{"age": "thirty"}, {"age": "25"}]
    result = coerce_field("age", int)(records)
    assert result == [{"age": 25}]


def test_coerce_field_reraises_when_skip_errors_false():
    records = [{"age": "thirty"}]
    with pytest.raises((ValueError, TypeError)):
        coerce_field("age", int, skip_errors=False)(records)


def test_coerce_field_preserves_other_fields():
    records = [{"age": "30", "name": "Alice"}]
    result = coerce_field("age", int)(records)
    assert result == [{"age": 30, "name": "Alice"}]


def test_coerce_field_missing_key_drops_record():
    """Records missing the target field should be dropped when coercion is attempted."""
    records = [{"name": "Alice"}, {"age": "25", "name": "Bob"}]
    result = coerce_field("age", int)(records)
    assert result == [{"age": 25, "name": "Bob"}]
