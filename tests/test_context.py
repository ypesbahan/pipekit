"""Tests for pipekit.context.PipelineContext."""

import time
import pytest
from pipekit.context import PipelineContext


def test_context_default_values():
    ctx = PipelineContext(run_id="run-1")
    assert ctx.run_id == "run-1"
    assert ctx.metadata == {}
    assert ctx.elapsed is None


def test_context_set_and_get():
    ctx = PipelineContext()
    ctx.set("source", "s3://bucket/data.csv")
    assert ctx.get("source") == "s3://bucket/data.csv"


def test_context_get_missing_key_returns_default():
    ctx = PipelineContext()
    assert ctx.get("missing") is None
    assert ctx.get("missing", 42) == 42


def test_context_start_sets_elapsed():
    ctx = PipelineContext()
    assert ctx.elapsed is None
    ctx.start()
    time.sleep(0.01)
    assert ctx.elapsed is not None
    assert ctx.elapsed >= 0.01


def test_context_record_and_get_step_timing():
    ctx = PipelineContext()
    ctx.record_step_timing("load", 1.23)
    ctx.record_step_timing("transform", 0.45)
    assert ctx.get_step_timing("load") == pytest.approx(1.23)
    assert ctx.get_step_timing("transform") == pytest.approx(0.45)
    assert ctx.get_step_timing("nonexistent") is None


def test_context_summary_structure():
    ctx = PipelineContext(run_id="abc-123")
    ctx.start()
    ctx.set("env", "test")
    ctx.record_step_timing("step_a", 0.5)
    summary = ctx.summary()
    assert summary["run_id"] == "abc-123"
    assert "elapsed" in summary
    assert summary["metadata"] == {"env": "test"}
    assert summary["step_timings"] == {"step_a": 0.5}


def test_context_summary_without_start():
    ctx = PipelineContext(run_id="no-start")
    summary = ctx.summary()
    assert summary["elapsed"] is None


def test_context_metadata_isolation():
    ctx1 = PipelineContext()
    ctx2 = PipelineContext()
    ctx1.set("key", "value1")
    ctx2.set("key", "value2")
    assert ctx1.get("key") == "value1"
    assert ctx2.get("key") == "value2"
