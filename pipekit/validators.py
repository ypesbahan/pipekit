"""Field and record validation transforms for pipekit pipelines."""

from typing import Any, Callable, Dict, List, Optional


class ValidationError(Exception):
    """Raised when a record fails validation in strict mode."""
    pass


def require_fields(*fields: str, strict: bool = False):
    """Return a step function that drops (or raises on) records missing required fields.

    Args:
        *fields: Field names that must be present and non-None.
        strict: If True, raise ValidationError instead of dropping the record.

    Returns:
        A transform function suitable for use as a pipeline step.
    """
    def _transform(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        result = []
        for record in records:
            missing = [f for f in fields if f not in record or record[f] is None]
            if missing:
                if strict:
                    raise ValidationError(
                        f"Record missing required fields: {missing}. Record: {record}"
                    )
                continue
            result.append(record)
        return result
    return _transform


def validate_field(
    field: str,
    validator: Callable[[Any], bool],
    strict: bool = False,
    error_message: Optional[str] = None,
):
    """Return a step function that validates a field value using a callable.

    Args:
        field: The field name to validate.
        validator: A callable that returns True if the value is valid.
        strict: If True, raise ValidationError on failure instead of dropping.
        error_message: Optional custom error message for strict mode.

    Returns:
        A transform function suitable for use as a pipeline step.
    """
    def _transform(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        result = []
        for record in records:
            value = record.get(field)
            if not validator(value):
                if strict:
                    msg = error_message or (
                        f"Field '{field}' failed validation with value: {value!r}"
                    )
                    raise ValidationError(msg)
                continue
            result.append(record)
        return result
    return _transform


def coerce_field(field: str, coerce_fn: Callable[[Any], Any], skip_errors: bool = True):
    """Return a step function that coerces a field value using a callable.

    Args:
        field: The field name to coerce.
        coerce_fn: A callable to apply to the field value (e.g. int, str, float).
        skip_errors: If True, drop records where coercion fails; otherwise re-raise.

    Returns:
        A transform function suitable for use as a pipeline step.
    """
    def _transform(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        result = []
        for record in records:
            try:
                coerced = dict(record)
                coerced[field] = coerce_fn(record[field])
                result.append(coerced)
            except (ValueError, TypeError, KeyError):
                if not skip_errors:
                    raise
        return result
    return _transform
