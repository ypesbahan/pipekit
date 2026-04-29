"""Tests for pipekit.cache.StepCache."""

import os
import pytest
from pipekit.cache import StepCache, CacheError


@pytest.fixture
def cache(tmp_path):
    return StepCache(cache_dir=str(tmp_path / "cache"))


def test_cache_miss_returns_none(cache):
    result = cache.get("my_step", [{"a": 1}])
    assert result is None


def test_cache_hit_returns_stored_value(cache):
    data_in = [{"x": 1}, {"x": 2}]
    data_out = [{"x": 2}, {"x": 4}]
    cache.set("double", data_in, data_out)
    result = cache.get("double", data_in)
    assert result == data_out


def test_has_returns_false_when_not_cached(cache):
    assert cache.has("step", [{"a": 1}]) is False


def test_has_returns_true_after_set(cache):
    cache.set("step", [{"a": 1}], [{"a": 2}])
    assert cache.has("step", [{"a": 1}]) is True


def test_different_inputs_produce_different_cache_entries(cache):
    cache.set("step", [{"a": 1}], "result_a")
    cache.set("step", [{"a": 2}], "result_b")
    assert cache.get("step", [{"a": 1}]) == "result_a"
    assert cache.get("step", [{"a": 2}]) == "result_b"


def test_different_step_names_do_not_collide(cache):
    data = [{"v": 99}]
    cache.set("step_a", data, "output_a")
    cache.set("step_b", data, "output_b")
    assert cache.get("step_a", data) == "output_a"
    assert cache.get("step_b", data) == "output_b"


def test_invalidate_specific_step(cache):
    cache.set("step_a", [1], "a")
    cache.set("step_b", [1], "b")
    removed = cache.invalidate("step_a")
    assert removed == 1
    assert cache.get("step_a", [1]) is None
    assert cache.get("step_b", [1]) == "b"


def test_invalidate_all(cache):
    cache.set("step_a", [1], "a")
    cache.set("step_b", [2], "b")
    removed = cache.invalidate()
    assert removed == 2
    assert cache.get("step_a", [1]) is None
    assert cache.get("step_b", [2]) is None


def test_cache_handles_non_json_serializable_input(cache):
    class CustomObj:
        pass
    obj = CustomObj()
    cache.set("step", obj, ["result"])
    # Cannot retrieve by equality for non-serializable; just ensure no crash on set
    assert True


def test_cache_dir_is_created(tmp_path):
    cache_dir = str(tmp_path / "new_cache_dir")
    assert not os.path.exists(cache_dir)
    StepCache(cache_dir=cache_dir)
    assert os.path.exists(cache_dir)
