"""Core Pipeline class for defining and running local ETL pipelines."""

from typing import Any, Callable, List, Optional
import logging
import time

logger = logging.getLogger(__name__)


class Step:
    """Represents a single transformation step in a pipeline."""

    def __init__(self, name: str, func: Callable[[Any], Any]):
        self.name = name
        self.func = func

    def run(self, data: Any) -> Any:
        logger.debug("Running step: %s", self.name)
        return self.func(data)


class PipelineError(Exception):
    """Raised when a pipeline step fails."""

    def __init__(self, step_name: str, original: Exception):
        self.step_name = step_name
        self.original = original
        super().__init__(f"Pipeline failed at step '{step_name}': {original}")


class Pipeline:
    """A lightweight ETL pipeline that chains steps sequentially."""

    def __init__(self, name: str):
        self.name = name
        self._steps: List[Step] = []

    def step(self, name: Optional[str] = None) -> Callable:
        """Decorator to register a function as a pipeline step."""

        def decorator(func: Callable[[Any], Any]) -> Callable:
            step_name = name or func.__name__
            self._steps.append(Step(step_name, func))
            return func

        return decorator

    def add_step(self, func: Callable[[Any], Any], name: Optional[str] = None) -> "Pipeline":
        """Imperatively add a step to the pipeline."""
        step_name = name or func.__name__
        self._steps.append(Step(step_name, func))
        return self

    def run(self, initial_data: Any = None) -> Any:
        """Execute all steps in order, passing output of each as input to the next."""
        logger.info("Starting pipeline: %s (%d steps)", self.name, len(self._steps))
        start = time.monotonic()
        data = initial_data

        for step in self._steps:
            try:
                data = step.run(data)
            except Exception as exc:
                raise PipelineError(step.name, exc) from exc

        elapsed = time.monotonic() - start
        logger.info("Pipeline '%s' completed in %.3fs", self.name, elapsed)
        return data

    def __repr__(self) -> str:
        step_names = [s.name for s in self._steps]
        return f"Pipeline(name={self.name!r}, steps={step_names})"
