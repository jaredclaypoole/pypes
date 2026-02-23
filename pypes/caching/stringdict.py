from typing import Iterable

from .base import CacheBase, HashType


class CachedStringDictBase(CacheBase):
    def __init__(
        self,
        assert_exists: bool = False,
    ):
        self._data: dict[HashType, str] = {}
        self._init_cache(assert_exists=assert_exists)

    def _init_cache(self, assert_exists: bool) -> None:
        raise NotImplementedError()  # pragma: no cover

    def _update_cache(self, key: HashType, value: str) -> None:
        raise NotImplementedError()  # pragma: no cover

    def __setitem__(self, key: HashType, value: str) -> None:
        self._update_cache(key, value)
        self._data[key] = value

    def __getitem__(self, key: HashType) -> str:
        return self._data[key]

    def __contains__(self, key: HashType) -> bool:
        return key in self._data

    def items(self) -> Iterable[tuple[HashType, str]]:
        yield from self._data.items()

    def keys(self) -> Iterable[HashType]:
        yield from self._data.keys()

    def values(self) -> Iterable[str]:
        yield from self._data.values()

    def __iter__(self) -> Iterable[HashType]:
        yield from self.keys()
