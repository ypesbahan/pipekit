"""Tests for pipekit.scheduler.StepScheduler."""

import pytest
from pipekit.scheduler import StepScheduler, SchedulerError


def test_single_step_no_deps():
    scheduler = StepScheduler()
    scheduler.add_step("load")
    assert scheduler.resolve() == ["load"]


def test_linear_chain_resolves_in_order():
    scheduler = StepScheduler()
    scheduler.add_step("load")
    scheduler.add_step("transform", depends_on=["load"])
    scheduler.add_step("export", depends_on=["transform"])
    assert scheduler.resolve() == ["load", "transform", "export"]


def test_independent_steps_are_included():
    scheduler = StepScheduler()
    scheduler.add_step("load_a")
    scheduler.add_step("load_b")
    order = scheduler.resolve()
    assert set(order) == {"load_a", "load_b"}
    assert len(order) == 2


def test_diamond_dependency_resolves():
    scheduler = StepScheduler()
    scheduler.add_step("source")
    scheduler.add_step("branch_a", depends_on=["source"])
    scheduler.add_step("branch_b", depends_on=["source"])
    scheduler.add_step("merge", depends_on=["branch_a", "branch_b"])
    order = scheduler.resolve()
    assert order.index("source") < order.index("branch_a")
    assert order.index("source") < order.index("branch_b")
    assert order.index("branch_a") < order.index("merge")
    assert order.index("branch_b") < order.index("merge")


def test_cycle_raises_scheduler_error():
    scheduler = StepScheduler()
    scheduler.add_step("a", depends_on=["b"])
    scheduler.add_step("b", depends_on=["a"])
    with pytest.raises(SchedulerError, match="Cycle detected"):
        scheduler.resolve()


def test_self_cycle_raises_scheduler_error():
    scheduler = StepScheduler()
    scheduler.add_step("a", depends_on=["a"])
    with pytest.raises(SchedulerError, match="Cycle detected"):
        scheduler.resolve()


def test_dependencies_of_returns_direct_deps():
    scheduler = StepScheduler()
    scheduler.add_step("c", depends_on=["a", "b"])
    assert scheduler.dependencies_of("c") == {"a", "b"}


def test_dependencies_of_unknown_step_returns_empty():
    scheduler = StepScheduler()
    assert scheduler.dependencies_of("missing") == set()


def test_steps_property_includes_all_registered():
    scheduler = StepScheduler()
    scheduler.add_step("load")
    scheduler.add_step("transform", depends_on=["load"])
    assert scheduler.steps == {"load", "transform"}


def test_dependency_declared_before_step_is_registered():
    scheduler = StepScheduler()
    # 'load' is implicitly registered via depends_on
    scheduler.add_step("transform", depends_on=["load"])
    scheduler.add_step("load")
    order = scheduler.resolve()
    assert order.index("load") < order.index("transform")
