"""Tests for the DAGRunner, which executes pipeline steps respecting dependency order."""

import pytest
from pipekit.dag_runner import DAGRunner
from pipekit.pipeline import Step


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_step(name, fn):
    """Convenience wrapper to create a named Step."""
    return Step(name=name, fn=fn)


# ---------------------------------------------------------------------------
# Basic execution
# ---------------------------------------------------------------------------

def test_dag_runner_single_step_no_deps():
    """A single step with no dependencies should run and return transformed data."""
    step = make_step("double", lambda data: [{"v": r["v"] * 2} for r in data])
    runner = DAGRunner()
    runner.add_step(step)

    result = runner.run([{"v": 3}])
    assert result == [{"v": 6}]


def test_dag_runner_linear_chain():
    """Steps chained A -> B -> C should execute in order, each receiving the previous output."""
    add_one = make_step("add_one", lambda data: [{"v": r["v"] + 1} for r in data])
    double  = make_step("double",  lambda data: [{"v": r["v"] * 2} for r in data])
    to_str  = make_step("to_str",  lambda data: [{"v": str(r["v"])} for r in data])

    runner = DAGRunner()
    runner.add_step(add_one)
    runner.add_step(double,  depends_on=["add_one"])
    runner.add_step(to_str,  depends_on=["double"])

    result = runner.run([{"v": 4}])
    # (4 + 1) * 2 = 10  -> "10"
    assert result == [{"v": "10"}]


def test_dag_runner_step_names_reflects_added_steps():
    """step_names should list every registered step."""
    s1 = make_step("s1", lambda d: d)
    s2 = make_step("s2", lambda d: d)

    runner = DAGRunner()
    runner.add_step(s1)
    runner.add_step(s2, depends_on=["s1"])

    assert set(runner.step_names) == {"s1", "s2"}


# ---------------------------------------------------------------------------
# Diamond / fan-out patterns
# ---------------------------------------------------------------------------

def test_dag_runner_diamond_dependency():
    """
    Diamond pattern:  root -> left, root -> right -> merge
    The final merge step receives the output of whichever branch ran last
    according to the resolved topological order.
    """
    calls = []

    def root(data):
        calls.append("root")
        return [{"v": r["v"] + 10} for r in data]

    def left(data):
        calls.append("left")
        return data  # pass-through

    def right(data):
        calls.append("right")
        return data  # pass-through

    def merge(data):
        calls.append("merge")
        return data

    runner = DAGRunner()
    runner.add_step(make_step("root",  root))
    runner.add_step(make_step("left",  left),  depends_on=["root"])
    runner.add_step(make_step("right", right), depends_on=["root"])
    runner.add_step(make_step("merge", merge), depends_on=["left", "right"])

    runner.run([{"v": 1}])

    # root must be first, merge must be last
    assert calls[0] == "root"
    assert calls[-1] == "merge"
    assert set(calls) == {"root", "left", "right", "merge"}


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

def test_dag_runner_unknown_dependency_raises():
    """Adding a step that depends on a non-existent step name should raise."""
    runner = DAGRunner()
    step = make_step("orphan", lambda d: d)

    with pytest.raises(Exception):
        runner.add_step(step, depends_on=["nonexistent"])
        runner.run([])


def test_dag_runner_step_exception_propagates():
    """If a step raises during execution the exception should bubble up."""
    def boom(data):
        raise ValueError("step exploded")

    runner = DAGRunner()
    runner.add_step(make_step("boom", boom))

    with pytest.raises(ValueError, match="step exploded"):
        runner.run([{"x": 1}])


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

def test_dag_runner_empty_input_data():
    """Running with an empty list should not raise and should return an empty list."""
    runner = DAGRunner()
    runner.add_step(make_step("noop", lambda d: d))

    assert runner.run([]) == []


def test_dag_runner_no_steps_returns_initial_data():
    """A runner with no steps should return the initial data unchanged."""
    runner = DAGRunner()
    data = [{"a": 1}]
    assert runner.run(data) == data
