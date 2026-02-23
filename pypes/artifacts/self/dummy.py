from functools import cached_property

from pydantic import BaseModel, ConfigDict

from ...core.interface import StepInputBase
from .base import (
    ArtifactResponseBase,
    ArtifactResolverBase,
    ArtifactSelfRequestBase,
)
from ..caching import ArtifactCacheKey
from ...caching.dir import DirCachedStringDict
from ...utils.hashing import myhash


class DummyStrDictArtifactResponse(ArtifactResponseBase, BaseModel, frozen=True):
    request: "DummyStrDictArtifactSelfRequest"
    content: str
    cache_hit: bool


class DummyStrDictArtifactSelfRequest(ArtifactSelfRequestBase, BaseModel, frozen=True):
    content: str
    cache_heading: str

    @cached_property
    def cache_key(self) -> ArtifactCacheKey:
        return ArtifactCacheKey(heading=self.cache_heading, hash=myhash(self))

    def init_cache(self, resolver: ArtifactResolverBase) -> None:
        heading = self.cache_key.heading
        if heading not in resolver.step_cache.cache_by_heading:
            cache_base_dir = resolver.pipeline.cache_base_dir
            assert cache_base_dir is not None
            step_cache_dir = cache_base_dir / resolver.step.cache_subdir
            cache_dir = step_cache_dir / heading

            sub_cache = DirCachedStringDict(cache_dir=cache_dir)
            resolver.step_cache.cache_by_heading[heading] = sub_cache
            print(f"{cache_dir=}")

    def resolve(self, resolver: ArtifactResolverBase) -> ArtifactResponseBase:
        heading = self.cache_key.heading
        cache_dict = resolver.step_cache.cache_by_heading[heading]
        request_key = self.cache_key.hash

        response_text: str|None = None
        cache_hit = False

        if request_key in cache_dict:
            response_text = cache_dict[request_key]
            cache_hit = True

        if response_text is None:
            response_text = self.content

            cache_dict[request_key] = response_text

        return DummyStrDictArtifactResponse(
            request=self,
            content=response_text,
            cache_hit=cache_hit,
        )
