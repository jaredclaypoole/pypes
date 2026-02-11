from pydantic import BaseModel

from ..caching.base import HashType, CacheKeyBase, CacheBase


HeadingType = str


class ArtifactCacheKey(CacheKeyBase, BaseModel, frozen=True):
    heading: HeadingType
    hash: HashType


class ArtifactCache(CacheBase):
    def __init__(self):
        self.cache_by_heading: dict[HeadingType, CacheBase] = {}
