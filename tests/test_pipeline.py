"""Tests for pipekit.pipeline module."""

import pytest
from pipekit.pipeline import Pipeline, PipelineError, Step


def test_step_runs_function():
    step = Step("double", lambda x: x * 2)
    assert step.run(5) == 10


def test_pipeline_runs_empty_with_initial_data():
    pipeline = Pipeline("empty")
    result = pipeline.run(42)
    assert result == 42


def test_pipeline_chains_steps():
    pipeline = Pipeline("math")
    pipeline.add_step(lambda x: x + 1, name="increment")
    pipeline.add_step(lambda x: x * 3, name="triple")
    result = pipeline.run(4)
    assert result == 15  # (4 + 1) * 3


def test_pipeline_decorator_registers_step():
    pipeline = Pipeline("decorated")

    @pipeline.step(name="stringify")
    def to_str(data):
        return str(data)

    result = pipeline.run(99)
    assert result == "99"


def test_pipeline_decorator_uses_function_name_as_default():
    pipeline = Pipeline("named")

    @pipeline.step()
    def my_transform(data):
        return data.upper()

    assert pipeline._steps[0].name == "my_transform"


def test_pipeline_add_step_returns_self_for_chaining():
    pipeline = Pipeline("chain")
    result = pipeline.add_step(lambda x: x, name="noop")
    assert result is pipeline


def test_pipeline_raises_pipeline_error_on_step_failure():
    pipeline = Pipeline("failing")
    pipeline.add_step(lambda x: x / 0, name="divide_by_zero")

    with pytest.raises(PipelineError) as exc_info:
        pipeline.run(10)

    assert exc_info.value.step_name == "divide_by_zero"
    assert isinstance(exc_info.value.original, ZeroDivisionError)


def test_pipeline_error_message_includes_step_name():
    pipeline = Pipeline("err")
    pipeline.add_step(lambda x: (_ for _ in ()).throw(ValueError("bad value")), name="bad_step")

    with pytest.raises(PipelineError, match="bad_step"):
        pipeline.run(None)


def test_pipeline_repr():
    pipeline = Pipeline("demo")
    pipeline.add_step(lambda x: x, name="noop")
    assert "demo" in repr(pipeline)
    assert "noop" in repr(pipeline)
