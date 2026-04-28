"""Pipeline execution context for passing metadata and shared state between steps."""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional
import time


@dataclass
class PipelineContext:
    """Holds shared metadata and state accessible to all steps in a pipeline run."""

    run_id: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)
    _step_timings: Dict[str, float] = field(default_factory=dict, repr=False)
    _start_time: Optional[float] = field(default=None, repr=False)

    def start(self) -> None:
        """Mark the pipeline run as started."""
        self._start_time = time.monotonic()

    def record_step_timing(self, step_name: str, elapsed: float) -> None:
        """Record how long a step took to execute."""
        self._step_timings[step_name] = elapsed

    def get_step_timing(self, step_name: str) -> Optional[float]:
        """Return the recorded timing for a step, or None if not recorded."""
        return self._step_timings.get(step_name)

    @property
    def elapsed(self) -> Optional[float]:
        """Return total elapsed seconds since start(), or None if not started."""
        if self._start_time is None:
            return None
        return time.monotonic() - self._start_time

    def set(self, key: str, value: Any) -> None:
        """Store a value in the metadata dict."""
        self.metadata[key] = value

    def get(self, key: str, default: Any = None) -> Any:
        """Retrieve a value from the metadata dict."""
        return self.metadata.get(key, default)

    def summary(self) -> Dict[str, Any]:
        """Return a summary dict of the run context."""
        return {
            "run_id": self.run_id,
            "elapsed": self.elapsed,
            "step_timings": dict(self._step_timings),
            "metadata": dict(self.metadata),
        }
