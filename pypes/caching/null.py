from .base import CacheBase, CacheKeyBase, ValueType


class NullCache(CacheBase):
    def __contains__(self, key: CacheKeyBase) -> bool:
        return False

    def __getitem__(self, key: CacheKeyBase) -> ValueType:
        raise ValueError(f"This cache can never contain any items")

    def __setitem__(self, key: CacheKeyBase, value: ValueType) -> None:
        raise ValueError(f"This cache can never contain any items")
