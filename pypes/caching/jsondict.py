from typing import Iterable, Any

from .base import CacheBase, HashType


ValueType = dict[str, Any]


class CachedJsonDictBase(CacheBase):
    def __init__(
        self,
        assert_exists: bool = False,
    ):
        self._data: dict[HashType, ValueType] = {}
        self._init_cache(assert_exists=assert_exists)

    def _init_cache(self, assert_exists: bool) -> None:
        raise NotImplementedError()  # pragma: no cover

    def _update_cache(self, key: HashType, value: ValueType) -> None:
        raise NotImplementedError()  # pragma: no cover

    def __setitem__(self, key: HashType, value: ValueType) -> None:
        self._update_cache(key, value)
        self._data[key] = value

    def __getitem__(self, key: HashType) -> ValueType:
        return self._data[key]

    def __contains__(self, key: HashType) -> bool:
        return key in self._data

    def items(self) -> Iterable[tuple[HashType, ValueType]]:
        yield from self._data.items()

    def keys(self) -> Iterable[HashType]:
        yield from self._data.keys()

    def values(self) -> Iterable[ValueType]:
        yield from self._data.values()

    def __iter__(self) -> Iterable[HashType]:
        yield from self.keys()
