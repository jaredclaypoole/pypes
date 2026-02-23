import tempfile
from pathlib import Path

from pydantic import BaseModel

from pypes.caching.base import CacheKeyBase
from pypes.caching.null import NullCache
from pypes.caching.dir import DirCachedStringDict

import pytest


class SimpleCacheKey(CacheKeyBase, BaseModel, frozen=True):
    key: str


def test_null_cache():
    null_cache = NullCache()

    key = SimpleCacheKey(key="first_key")
    assert key not in null_cache

    with pytest.raises(ValueError):
        null_cache[key] = "dummy"
    assert key not in null_cache

    with pytest.raises(ValueError):
        dummy = null_cache[key]
    assert key not in null_cache


def test_dir_cached_string_dict():
    with tempfile.TemporaryDirectory() as tmpdirname:
        cache_dir = Path(tmpdirname) / "the_cache"
        with pytest.raises(AssertionError):
            DirCachedStringDict(cache_dir=cache_dir, assert_exists=True)

        key1 = "dummy1"
        key2 = "dummy2"
        val1 = "dummy1_value"
        val2 = "dummy2_value"

        cache1 = DirCachedStringDict(cache_dir=cache_dir, assert_exists=False)
        assert not (cache_dir / f"{key1}.txt").exists()
        assert key1 not in cache1
        assert key2 not in cache1

        cache1[key1] = val1
        assert (cache_dir / f"{key1}.txt").exists()
        assert key1 in cache1
        assert key2 not in cache1
        assert cache1[key1] == val1

        cache1[key2] = val2
        assert (cache_dir / f"{key1}.txt").exists()
        assert (cache_dir / f"{key2}.txt").exists()
        assert key1 in cache1
        assert key2 in cache1
        assert cache1[key1] == val1
        assert cache1[key2] == val2

        cache2 = DirCachedStringDict(cache_dir=cache_dir, assert_exists=True)
        assert key1 in cache2
        assert key2 in cache2
        assert cache2[key1] == val1
        assert cache2[key2] == val2

        dict_expected = {
            key1: val1,
            key2: val2,
        }
        assert list(cache2.items()) == list(dict_expected.items())
        assert list(cache2.keys()) == list(dict_expected.keys())
        assert list(cache2.values()) == list(dict_expected.values())
        assert list(cache2) == list(dict_expected)
