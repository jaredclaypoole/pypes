from typing import Any


HashType = str
ValueType = Any


class CacheKeyBase:
    pass


class CacheBase:
    def __contains__(self, key: CacheKeyBase) -> bool:
        raise NotImplementedError()  # pragma: no cover

    def __getitem__(self, key: CacheKeyBase) -> ValueType:
        raise NotImplementedError()  # pragma: no cover

    def __setitem__(self, key: CacheKeyBase, value: ValueType) -> None:
        raise NotImplementedError()  # pragma: no cover
