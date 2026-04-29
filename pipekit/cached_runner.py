"""A pipeline runner that caches step outputs to avoid redundant recomputation."""

from typing import Any, List, Optional

from pipekit.cache import StepCache
from pipekit.pipeline import Step
from pipekit.runners import RunResult, SequentialRunner


class CachedRunner:
    """Wraps SequentialRunner and transparently caches each step's output.

    On subsequent runs with identical input data for a step, the cached
    result is returned instead of re-executing the step function.
    """

    def __init__(self, steps: List[Step], cache_dir: str = ".pipekit_cache"):
        self.steps = steps
        self.cache = StepCache(cache_dir=cache_dir)
        self._cache_hits: List[str] = []
        self._cache_misses: List[str] = []

    def run(self, data: Any) -> RunResult:
        """Run all steps sequentially, using cached outputs where available."""
        self._cache_hits = []
        self._cache_misses = []
        current = data

        for step in self.steps:
            if self.cache.has(step.name, current):
                current = self.cache.get(step.name, current)
                self._cache_hits.append(step.name)
            else:
                current = step.run(current)
                self.cache.set(step.name, data, current)
                self._cache_misses.append(step.name)

        return RunResult(
            output=current,
            steps_run=len(self.steps),
            cache_hits=list(self._cache_hits),
            cache_misses=list(self._cache_misses),
        )

    def invalidate(self, step_name: Optional[str] = None) -> int:
        """Invalidate cache entries for a specific step or all steps."""
        return self.cache.invalidate(step_name)

    @property
    def cache_hits(self) -> List[str]:
        return list(self._cache_hits)

    @property
    def cache_misses(self) -> List[str]:
        return list(self._cache_misses)
