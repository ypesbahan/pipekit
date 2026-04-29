"""Step profiling utilities for pipekit pipelines."""

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


@dataclass
class StepProfile:
    """Holds profiling data for a single step execution."""

    step_name: str
    duration_seconds: float
    input_count: int
    output_count: int
    dropped_count: int = field(init=False)

    def __post_init__(self) -> None:
        self.dropped_count = self.input_count - self.output_count

    def __repr__(self) -> str:
        return (
            f"StepProfile(step={self.step_name!r}, "
            f"duration={self.duration_seconds:.4f}s, "
            f"in={self.input_count}, out={self.output_count}, "
            f"dropped={self.dropped_count})"
        )


class PipelineProfiler:
    """Collects and reports per-step performance profiles."""

    def __init__(self) -> None:
        self._profiles: List[StepProfile] = []

    def profile_step(
        self,
        step_name: str,
        fn: Callable[[List[Any]], List[Any]],
        data: List[Any],
    ) -> List[Any]:
        """Run *fn* on *data*, record timing and record counts, return result."""
        input_count = len(data)
        start = time.perf_counter()
        result = fn(data)
        duration = time.perf_counter() - start
        output_count = len(result) if result is not None else 0
        profile = StepProfile(
            step_name=step_name,
            duration_seconds=duration,
            input_count=input_count,
            output_count=output_count,
        )
        self._profiles.append(profile)
        return result

    def profiles(self) -> List[StepProfile]:
        """Return a copy of all collected profiles."""
        return list(self._profiles)

    def summary(self) -> Dict[str, Any]:
        """Return a summary dict with totals and per-step breakdown."""
        total_duration = sum(p.duration_seconds for p in self._profiles)
        return {
            "total_duration_seconds": round(total_duration, 6),
            "step_count": len(self._profiles),
            "steps": [
                {
                    "step": p.step_name,
                    "duration_seconds": round(p.duration_seconds, 6),
                    "input_count": p.input_count,
                    "output_count": p.output_count,
                    "dropped_count": p.dropped_count,
                }
                for p in self._profiles
            ],
        }

    def reset(self) -> None:
        """Clear all collected profiles."""
        self._profiles.clear()
