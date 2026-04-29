"""DAG-aware pipeline runner that respects step dependencies."""

from __future__ import annotations
from typing import Any, Dict, List, Optional

from pipekit.pipeline import Step
from pipekit.scheduler import StepScheduler
from pipekit.runners import RunResult


class DAGRunner:
    """Runs pipeline steps in dependency-resolved order.

    Unlike SequentialRunner, DAGRunner accepts an explicit dependency
    map and uses StepScheduler to determine execution order before
    running each step in sequence.

    Example::

        runner = DAGRunner()
        runner.add_step(load_step)
        runner.add_step(transform_step, depends_on=["load"])
        runner.add_step(export_step, depends_on=["transform"])
        result = runner.run(initial_data=[])
    """

    def __init__(self) -> None:
        self._steps: Dict[str, Step] = {}
        self._scheduler: StepScheduler = StepScheduler()

    def add_step(
        self,
        step: Step,
        depends_on: Optional[List[str]] = None,
    ) -> None:
        """Register a step with optional dependencies by step name."""
        self._steps[step.name] = step
        self._scheduler.add_step(step.name, depends_on=depends_on)

    def run(self, initial_data: Any = None) -> RunResult:
        """Execute all steps in resolved dependency order.

        Args:
            initial_data: Data passed into the first step(s).

        Returns:
            RunResult with final data and per-step metadata.
        """
        order = self._scheduler.resolve()
        data = initial_data
        step_results: List[Dict[str, Any]] = []
        errors: List[str] = []

        for name in order:
            if name not in self._steps:
                continue
            step = self._steps[name]
            try:
                data = step.run(data)
                step_results.append({"step": name, "status": "ok"})
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{name}: {exc}")
                step_results.append({"step": name, "status": "error", "error": str(exc)})
                break

        return RunResult(
            data=data,
            steps_run=[r["step"] for r in step_results],
            errors=errors,
        )

    @property
    def step_names(self) -> List[str]:
        """Names of all registered steps."""
        return list(self._steps.keys())

    @property
    def resolved_order(self) -> List[str]:
        """Preview the resolved execution order without running."""
        order = self._scheduler.resolve()
        return [name for name in order if name in self._steps]
