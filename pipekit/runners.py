"""Pipeline runners for different execution strategies.

This module provides runner classes that control how pipelines are executed,
including sequential, dry-run, and logged execution modes.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Callable, List, Optional

from pipekit.pipeline import Pipeline, PipelineError

logger = logging.getLogger(__name__)


class RunResult:
    """Holds the result of a pipeline run, including timing and step metadata."""

    def __init__(self) -> None:
        self.steps_executed: List[str] = []
        self.step_timings: dict[str, float] = {}
        self.output: Any = None
        self.success: bool = False
        self.error: Optional[Exception] = None
        self.total_time: float = 0.0

    def __repr__(self) -> str:
        status = "OK" if self.success else "FAILED"
        return (
            f"<RunResult status={status} steps={len(self.steps_executed)} "
            f"total_time={self.total_time:.3f}s>"
        )


class SequentialRunner:
    """Runs a pipeline sequentially, step by step.

    This is the default runner. Each step receives the output of the previous
    step as its input, starting with the provided initial data.

    Example::

        runner = SequentialRunner(verbose=True)
        result = runner.run(pipeline, initial_data=[{"id": 1}])
        print(result.output)
    """

    def __init__(self, verbose: bool = False) -> None:
        """
        Args:
            verbose: If True, log step names and timing information during the run.
        """
        self.verbose = verbose

    def run(self, pipeline: Pipeline, initial_data: Any = None) -> RunResult:
        """Execute all steps in the pipeline sequentially.

        Args:
            pipeline: The pipeline instance to run.
            initial_data: The data passed into the first step.

        Returns:
            A RunResult containing output data, step names, and timing info.
        """
        result = RunResult()
        data = initial_data
        overall_start = time.perf_counter()

        try:
            for step in pipeline.steps:
                step_start = time.perf_counter()

                if self.verbose:
                    logger.info("Running step: %s", step.name)

                data = step.run(data)

                elapsed = time.perf_counter() - step_start
                result.steps_executed.append(step.name)
                result.step_timings[step.name] = elapsed

                if self.verbose:
                    logger.info("Step '%s' completed in %.3fs", step.name, elapsed)

        except Exception as exc:  # noqa: BLE001
            result.error = exc
            result.success = False
            result.total_time = time.perf_counter() - overall_start
            logger.error("Pipeline failed at step '%s': %s", step.name, exc)
            raise PipelineError(f"Step '{step.name}' raised an error: {exc}") from exc

        result.output = data
        result.success = True
        result.total_time = time.perf_counter() - overall_start

        if self.verbose:
            logger.info("Pipeline completed in %.3fs", result.total_time)

        return result


class DryRunRunner:
    """Simulates a pipeline run without executing step functions.

    Useful for validating pipeline structure and step registration before
    committing to a full run.

    Example::

        runner = DryRunRunner()
        result = runner.run(pipeline, initial_data=[{"id": 1}])
        print(result.steps_executed)  # lists steps that would have run
    """

    def run(self, pipeline: Pipeline, initial_data: Any = None) -> RunResult:
        """Simulate the pipeline run without invoking step functions.

        Args:
            pipeline: The pipeline instance to inspect.
            initial_data: Passed through unchanged as the output.

        Returns:
            A RunResult with step names populated but no actual processing done.
        """
        result = RunResult()

        for step in pipeline.steps:
            logger.info("[DRY RUN] Would execute step: %s", step.name)
            result.steps_executed.append(step.name)
            result.step_timings[step.name] = 0.0

        result.output = initial_data
        result.success = True
        result.total_time = 0.0
        return result
