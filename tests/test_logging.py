"""Tests for pipekit.logging module."""

import io
import pytest
from pipekit.logging import PipelineLogger


@pytest.fixture
def stream():
    return io.StringIO()


@pytest.fixture
def logger(stream):
    return PipelineLogger(name="test", level="DEBUG", stream=stream)


def test_logger_records_info_entry(logger):
    logger.info("hello world")
    entries = logger.get_entries()
    assert len(entries) == 1
    assert entries[0]["message"] == "hello world"
    assert entries[0]["level"] == "INFO"


def test_logger_records_step_name(logger):
    logger.info("processing", step="my_step")
    entry = logger.get_entries()[0]
    assert entry["step"] == "my_step"


def test_logger_respects_level_filter(stream):
    logger = PipelineLogger(name="test", level="WARNING", stream=stream)
    logger.debug("ignored")
    logger.info("also ignored")
    logger.warning("kept")
    entries = logger.get_entries()
    assert len(entries) == 1
    assert entries[0]["level"] == "WARNING"


def test_logger_includes_extra_kwargs(logger):
    logger.info("count", step="s1", input_records=42)
    entry = logger.get_entries()[0]
    assert entry["input_records"] == 42


def test_logger_emits_to_stream(logger, stream):
    logger.info("emitted message", step="emit_step")
    output = stream.getvalue()
    assert "emitted message" in output
    assert "[emit_step]" in output
    assert "INFO" in output


def test_log_step_start(logger):
    logger.log_step_start("load", record_count=10)
    entry = logger.get_entries()[0]
    assert entry["step"] == "load"
    assert entry["input_records"] == 10
    assert "starting" in entry["message"].lower()


def test_log_step_end(logger):
    logger.log_step_end("transform", record_count=8, elapsed_ms=12.345)
    entry = logger.get_entries()[0]
    assert entry["step"] == "transform"
    assert entry["output_records"] == 8
    assert entry["elapsed_ms"] == 12.35


def test_clear_removes_entries(logger):
    logger.info("one")
    logger.info("two")
    assert len(logger.get_entries()) == 2
    logger.clear()
    assert logger.get_entries() == []


def test_get_entries_returns_copy(logger):
    logger.info("entry")
    entries = logger.get_entries()
    entries.clear()
    assert len(logger.get_entries()) == 1
