"""Retry utilities for pipekit pipeline steps."""

import time
import functools
from typing import Callable, Optional, Tuple, Type


class RetryError(Exception):
    """Raised when all retry attempts are exhausted."""

    def __init__(self, step_name: str, attempts: int, last_exception: Exception):
        self.step_name = step_name
        self.attempts = attempts
        self.last_exception = last_exception
        super().__init__(
            f"Step '{step_name}' failed after {attempts} attempt(s): {last_exception}"
        )


def with_retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    step_name: Optional[str] = None,
) -> Callable:
    """Decorator that retries a step function on failure.

    Args:
        max_attempts: Maximum number of attempts before raising RetryError.
        delay: Initial delay in seconds between attempts.
        backoff: Multiplier applied to delay after each failure.
        exceptions: Tuple of exception types that trigger a retry.
        step_name: Optional name used in error messages (defaults to function name).

    Returns:
        Decorated function with retry logic applied.
    """
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")
    if delay < 0:
        raise ValueError("delay must be non-negative")
    if backoff < 1.0:
        raise ValueError("backoff must be >= 1.0")

    def decorator(fn: Callable) -> Callable:
        name = step_name or fn.__name__

        @functools.wraps(fn)
        def wrapper(data, *args, **kwargs):
            current_delay = delay
            last_exc: Optional[Exception] = None

            for attempt in range(1, max_attempts + 1):
                try:
                    return fn(data, *args, **kwargs)
                except exceptions as exc:
                    last_exc = exc
                    if attempt < max_attempts:
                        time.sleep(current_delay)
                        current_delay *= backoff

            raise RetryError(name, max_attempts, last_exc)

        wrapper._retry_config = {
            "max_attempts": max_attempts,
            "delay": delay,
            "backoff": backoff,
        }
        return wrapper

    return decorator
