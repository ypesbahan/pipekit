"""Simple step-level caching for pipeline runs."""

import hashlib
import json
import os
import pickle
from typing import Any, Optional


class CacheError(Exception):
    pass


class StepCache:
    """File-backed cache for step outputs, keyed by step name and input hash."""

    def __init__(self, cache_dir: str = ".pipekit_cache"):
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)

    def _hash_data(self, data: Any) -> str:
        try:
            serialized = json.dumps(data, sort_keys=True, default=str).encode()
        except (TypeError, ValueError):
            serialized = pickle.dumps(data)
        return hashlib.md5(serialized).hexdigest()

    def _cache_path(self, step_name: str, data_hash: str) -> str:
        filename = f"{step_name}_{data_hash}.pkl"
        return os.path.join(self.cache_dir, filename)

    def get(self, step_name: str, input_data: Any) -> Optional[Any]:
        """Return cached output for the given step and input, or None if not cached."""
        data_hash = self._hash_data(input_data)
        path = self._cache_path(step_name, data_hash)
        if os.path.exists(path):
            try:
                with open(path, "rb") as f:
                    return pickle.load(f)
            except (pickle.UnpicklingError, EOFError) as e:
                raise CacheError(f"Failed to load cache for step '{step_name}': {e}") from e
        return None

    def set(self, step_name: str, input_data: Any, output_data: Any) -> None:
        """Store the output for the given step and input."""
        data_hash = self._hash_data(input_data)
        path = self._cache_path(step_name, data_hash)
        try:
            with open(path, "wb") as f:
                pickle.dump(output_data, f)
        except (pickle.PicklingError, OSError) as e:
            raise CacheError(f"Failed to write cache for step '{step_name}': {e}") from e

    def invalidate(self, step_name: Optional[str] = None) -> int:
        """Remove cached entries. If step_name given, only remove that step's entries."""
        removed = 0
        for filename in os.listdir(self.cache_dir):
            if not filename.endswith(".pkl"):
                continue
            if step_name is None or filename.startswith(f"{step_name}_"):
                os.remove(os.path.join(self.cache_dir, filename))
                removed += 1
        return removed

    def has(self, step_name: str, input_data: Any) -> bool:
        """Return True if a cached result exists for the given step and input."""
        data_hash = self._hash_data(input_data)
        return os.path.exists(self._cache_path(step_name, data_hash))
