from functools import cached_property
import random
from string import Template

from pydantic import BaseModel

from ...core.interface import StepInputBase
from .base import (
    ArtifactResponseBase,
    ArtifactResolverBase,
    ArtifactSelfRequestBase,
)
from ..caching import ArtifactCacheKey
from ...caching.dir import DirCachedStringDict
from ...utils.hashing import myhash


class FakeLLMArtifactResponse(ArtifactResponseBase, BaseModel, frozen=True):
    request: "FakeLLMArtifactSelfRequest"
    text: str


class FakeLLMArtifactSelfRequest(ArtifactSelfRequestBase, BaseModel, frozen=True):
    input: StepInputBase
    model: str
    prompt_template_str: str
    prompt_kwargs: dict[str, str]
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

    def resolve(self, resolver: ArtifactResolverBase) -> ArtifactResponseBase:
        heading = self.cache_key.heading
        cache_dict = resolver.step_cache.cache_by_heading[heading]
        request_key = self.cache_key.hash

        response_text: str|None = None

        if request_key in cache_dict:
            response_text = cache_dict[request_key]

        if response_text is None:
            random_value = random.randint(10_000, 99_999)
            prompt_template = Template(
                self.prompt_template_str,
            )
            prompt_text = prompt_template.substitute(**self.prompt_kwargs)
            response_text = f"""
[randomness={random_value}]
[model={self.model}]
This is a fake LLM response to the following prompt:
{prompt_text}
""".strip()

            cache_dict[request_key] = response_text

        return FakeLLMArtifactResponse(
            request=self,
            text=response_text,
        )
