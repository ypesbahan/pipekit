"""Structured step-level logging for pipekit pipelines."""

import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional


LEVELS = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40}


class PipelineLogger:
    """Collects and emits structured log entries during pipeline execution."""

    def __init__(self, name: str = "pipekit", level: str = "INFO", stream=None):
        self.name = name
        self.level = LEVELS.get(level.upper(), 20)
        self._stream = stream or sys.stdout
        self._entries: List[Dict[str, Any]] = []

    def _log(self, level: str, message: str, step: Optional[str] = None, **kwargs):
        if LEVELS.get(level, 0) < self.level:
            return
        entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": level,
            "logger": self.name,
            "message": message,
        }
        if step is not None:
            entry["step"] = step
        entry.update(kwargs)
        self._entries.append(entry)
        self._emit(entry)

    def _emit(self, entry: Dict[str, Any]):
        step_part = f" [{entry['step']}]" if "step" in entry else ""
        line = f"{entry['timestamp']} {entry['level']}{step_part}: {entry['message']}"
        extra = {k: v for k, v in entry.items() if k not in ("timestamp", "level", "step", "message", "logger")}
        if extra:
            line += f" | {extra}"
        print(line, file=self._stream)

    def debug(self, message: str, step: Optional[str] = None, **kwargs):
        self._log("DEBUG", message, step=step, **kwargs)

    def info(self, message: str, step: Optional[str] = None, **kwargs):
        self._log("INFO", message, step=step, **kwargs)

    def warning(self, message: str, step: Optional[str] = None, **kwargs):
        self._log("WARNING", message, step=step, **kwargs)

    def error(self, message: str, step: Optional[str] = None, **kwargs):
        self._log("ERROR", message, step=step, **kwargs)

    def log_step_start(self, step: str, record_count: int):
        self.info("Step starting", step=step, input_records=record_count)

    def log_step_end(self, step: str, record_count: int, elapsed_ms: float):
        self.info("Step complete", step=step, output_records=record_count, elapsed_ms=round(elapsed_ms, 2))

    def get_entries(self) -> List[Dict[str, Any]]:
        return list(self._entries)

    def clear(self):
        self._entries.clear()
