"""Tests for pipekit.retry module."""

import pytest
from unittest.mock import patch
from pipekit.retry import with_retry, RetryError


def test_retry_succeeds_on_first_attempt():
    calls = []

    @with_retry(max_attempts=3)
    def step(data):
        calls.append(1)
        return data

    result = step([{"x": 1}])
    assert result == [{"x": 1}]
    assert len(calls) == 1


def test_retry_succeeds_after_failures():
    attempts = []

    @with_retry(max_attempts=3, delay=0)
    def step(data):
        attempts.append(1)
        if len(attempts) < 3:
            raise ValueError("not yet")
        return data

    result = step([{"a": 1}])
    assert result == [{"a": 1}]
    assert len(attempts) == 3


def test_retry_raises_retry_error_after_exhaustion():
    @with_retry(max_attempts=2, delay=0)
    def step(data):
        raise RuntimeError("always fails")

    with pytest.raises(RetryError) as exc_info:
        step([])

    err = exc_info.value
    assert err.attempts == 2
    assert "step" in str(err).lower() or "always fails" in str(err)
    assert isinstance(err.last_exception, RuntimeError)


def test_retry_only_catches_specified_exceptions():
    @with_retry(max_attempts=3, delay=0, exceptions=(ValueError,))
    def step(data):
        raise TypeError("wrong type")

    with pytest.raises(TypeError):
        step([])


def test_retry_respects_delay_and_backoff():
    sleep_calls = []

    @with_retry(max_attempts=3, delay=1.0, backoff=2.0)
    def step(data):
        raise ValueError("fail")

    with patch("pipekit.retry.time.sleep", side_effect=lambda s: sleep_calls.append(s)):
        with pytest.raises(RetryError):
            step([])

    assert sleep_calls == [1.0, 2.0]


def test_retry_preserves_function_name():
    @with_retry(max_attempts=2)
    def my_transform(data):
        return data

    assert my_transform.__name__ == "my_transform"


def test_retry_stores_config_on_wrapper():
    @with_retry(max_attempts=4, delay=0.5, backoff=1.5)
    def step(data):
        return data

    assert step._retry_config["max_attempts"] == 4
    assert step._retry_config["delay"] == 0.5
    assert step._retry_config["backoff"] == 1.5


def test_retry_invalid_max_attempts_raises():
    with pytest.raises(ValueError, match="max_attempts"):
        with_retry(max_attempts=0)


def test_retry_invalid_backoff_raises():
    with pytest.raises(ValueError, match="backoff"):
        with_retry(backoff=0.5)


def test_retry_step_name_override_in_error():
    @with_retry(max_attempts=1, step_name="custom_step")
    def step(data):
        raise ValueError("oops")

    with pytest.raises(RetryError) as exc_info:
        step([])

    assert "custom_step" in str(exc_info.value)
