"""Built-in reusable transform functions for common ETL operations."""

from typing import Any, Callable, Iterable, List


def map_field(field: str, func: Callable) -> Callable:
    """Apply a function to a specific field in each record."""
    def _transform(data: List[dict]) -> List[dict]:
        return [{**record, field: func(record[field])} for record in data]
    _transform.__name__ = f"map_field({field})"
    return _transform


def filter_records(predicate: Callable[[dict], bool]) -> Callable:
    """Filter records based on a predicate function."""
    def _transform(data: List[dict]) -> List[dict]:
        return [record for record in data if predicate(record)]
    _transform.__name__ = "filter_records"
    return _transform


def rename_field(old_name: str, new_name: str) -> Callable:
    """Rename a field in each record."""
    def _transform(data: List[dict]) -> List[dict]:
        result = []
        for record in data:
            new_record = {**record}
            if old_name in new_record:
                new_record[new_name] = new_record.pop(old_name)
            result.append(new_record)
        return result
    _transform.__name__ = f"rename_field({old_name} -> {new_name})"
    return _transform


def drop_fields(*fields: str) -> Callable:
    """Remove specified fields from each record."""
    def _transform(data: List[dict]) -> List[dict]:
        return [{k: v for k, v in record.items() if k not in fields} for record in data]
    _transform.__name__ = f"drop_fields({', '.join(fields)})"
    return _transform


def add_field(field: str, value_func: Callable[[dict], Any]) -> Callable:
    """Add a new computed field to each record."""
    def _transform(data: List[dict]) -> List[dict]:
        return [{**record, field: value_func(record)} for record in data]
    _transform.__name__ = f"add_field({field})"
    return _transform


def flatten(data: Iterable[Iterable]) -> List:
    """Flatten one level of nesting from a list of lists."""
    return [item for sublist in data for item in sublist]


def deduplicate(key: str = None) -> Callable:
    """Remove duplicate records, optionally by a specific key field."""
    def _transform(data: List[dict]) -> List[dict]:
        if key is None:
            seen = set()
            result = []
            for record in data:
                frozen = tuple(sorted(record.items()))
                if frozen not in seen:
                    seen.add(frozen)
                    result.append(record)
            return result
        seen_keys = set()
        result = []
        for record in data:
            k = record.get(key)
            if k not in seen_keys:
                seen_keys.add(k)
                result.append(record)
        return result
    _transform.__name__ = f"deduplicate(key={key})"
    return _transform
