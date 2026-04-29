"""Tests for pipekit.profiler."""

import pytest
from pipekit.profiler import PipelineProfiler, StepProfile


# ---------------------------------------------------------------------------
# StepProfile
# ---------------------------------------------------------------------------

def test_step_profile_computes_dropped_count():
    profile = StepProfile(step_name="filter", duration_seconds=0.01, input_count=10, output_count=7)
    assert profile.dropped_count == 3


def test_step_profile_zero_dropped_when_counts_equal():
    profile = StepProfile(step_name="map", duration_seconds=0.001, input_count=5, output_count=5)
    assert profile.dropped_count == 0


def test_step_profile_repr_contains_step_name():
    profile = StepProfile(step_name="enrich", duration_seconds=0.5, input_count=4, output_count=4)
    assert "enrich" in repr(profile)


# ---------------------------------------------------------------------------
# PipelineProfiler
# ---------------------------------------------------------------------------

@pytest.fixture()
def profiler():
    return PipelineProfiler()


def test_profiler_starts_empty(profiler):
    assert profiler.profiles() == []


def test_profiler_records_single_step(profiler):
    data = [{"x": i} for i in range(5)]
    result = profiler.profile_step("identity", lambda d: d, data)
    assert result == data
    profiles = profiler.profiles()
    assert len(profiles) == 1
    assert profiles[0].step_name == "identity"
    assert profiles[0].input_count == 5
    assert profiles[0].output_count == 5


def test_profiler_records_filtered_step(profiler):
    data = list(range(10))
    result = profiler.profile_step("drop_half", lambda d: d[:5], data)
    assert result == list(range(5))
    p = profiler.profiles()[0]
    assert p.input_count == 10
    assert p.output_count == 5
    assert p.dropped_count == 5


def test_profiler_accumulates_multiple_steps(profiler):
    profiler.profile_step("step_a", lambda d: d, [1, 2, 3])
    profiler.profile_step("step_b", lambda d: d[:1], [1, 2, 3])
    assert len(profiler.profiles()) == 2


def test_profiler_summary_structure(profiler):
    profiler.profile_step("s1", lambda d: d, [{"a": 1}, {"a": 2}])
    summary = profiler.summary()
    assert summary["step_count"] == 1
    assert "total_duration_seconds" in summary
    assert len(summary["steps"]) == 1
    step_info = summary["steps"][0]
    assert step_info["step"] == "s1"
    assert step_info["input_count"] == 2
    assert step_info["output_count"] == 2
    assert step_info["dropped_count"] == 0


def test_profiler_summary_total_duration_is_sum(profiler):
    profiler.profile_step("a", lambda d: d, [1])
    profiler.profile_step("b", lambda d: d, [1])
    summary = profiler.summary()
    expected = sum(p.duration_seconds for p in profiler.profiles())
    assert abs(summary["total_duration_seconds"] - expected) < 1e-9


def test_profiler_reset_clears_profiles(profiler):
    profiler.profile_step("x", lambda d: d, [1, 2])
    profiler.reset()
    assert profiler.profiles() == []
    assert profiler.summary()["step_count"] == 0


def test_profiles_returns_copy(profiler):
    profiler.profile_step("y", lambda d: d, [1])
    copy = profiler.profiles()
    copy.clear()
    assert len(profiler.profiles()) == 1
