"""Step scheduler for dependency-aware pipeline execution ordering."""

from __future__ import annotations
from typing import Dict, List, Optional, Set
from collections import defaultdict, deque


class SchedulerError(Exception):
    """Raised when the scheduler encounters an invalid dependency graph."""
    pass


class StepScheduler:
    """Resolves step execution order based on declared dependencies.

    Uses Kahn's algorithm (topological sort) to produce a valid
    execution order, and raises SchedulerError on cycles.
    """

    def __init__(self) -> None:
        self._dependencies: Dict[str, Set[str]] = defaultdict(set)
        self._steps: Set[str] = set()

    def add_step(self, name: str, depends_on: Optional[List[str]] = None) -> None:
        """Register a step and its optional dependencies."""
        self._steps.add(name)
        if depends_on:
            for dep in depends_on:
                self._steps.add(dep)
                self._dependencies[name].add(dep)

    def resolve(self) -> List[str]:
        """Return steps in a valid topological execution order.

        Raises:
            SchedulerError: If a cycle is detected in the dependency graph.
        """
        in_degree: Dict[str, int] = {step: 0 for step in self._steps}
        dependents: Dict[str, List[str]] = defaultdict(list)

        for step, deps in self._dependencies.items():
            for dep in deps:
                dependents[dep].append(step)
                in_degree[step] += 1

        queue: deque[str] = deque(
            sorted(s for s, deg in in_degree.items() if deg == 0)
        )
        order: List[str] = []

        while queue:
            current = queue.popleft()
            order.append(current)
            for dependent in sorted(dependents[current]):
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        if len(order) != len(self._steps):
            remaining = self._steps - set(order)
            raise SchedulerError(
                f"Cycle detected in step dependencies involving: {sorted(remaining)}"
            )

        return order

    def dependencies_of(self, name: str) -> Set[str]:
        """Return the direct dependencies of a given step."""
        return set(self._dependencies.get(name, set()))

    @property
    def steps(self) -> Set[str]:
        """All registered step names."""
        return set(self._steps)
