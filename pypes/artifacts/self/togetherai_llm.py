from functools import cached_property
import os

from pydantic import BaseModel, ConfigDict

from ...core.interface import StepInputBase
from .base import (
    ArtifactResponseBase,
    ArtifactResolverBase,
    ArtifactSelfRequestBase,
)
from ..caching import ArtifactCacheKey
from ...caching.dir import DirCachedJsonDict
from ...utils.hashing import myhash


class TogetherLLMArtifactResponse(ArtifactResponseBase, BaseModel, frozen=True):
    request: "TogetherLLMArtifactSelfRequest"
    response_dict: dict[str, str]


class TogetherLLMArtifactSelfRequest(ArtifactSelfRequestBase, BaseModel, frozen=True):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    model: str
    system_prompt: str|None = None
    prompt: str
    temperature: float|None = None
    max_tokens: int|None = None
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

            sub_cache = DirCachedJsonDict(cache_dir=cache_dir)
            resolver.step_cache.cache_by_heading[heading] = sub_cache

    def resolve(self, resolver: ArtifactResolverBase) -> ArtifactResponseBase:
        heading = self.cache_key.heading
        cache_dict = resolver.step_cache.cache_by_heading[heading]
        request_key = self.cache_key.hash

        response_dict: dict[str, str]|None = None

        if request_key in cache_dict:
            response_dict = cache_dict[request_key]

        if response_dict is None:
            try:
                import together
            except OSError:
                raise ValueError(f"Could not import together.  Perhaps run `pip install together` in your venv?")

            api_key = os.environ.get("TOGETHER_API_KEY", None)
            if not api_key:
                raise ValueError(f"Could not read env var TOGETHER_API_KEY")

            client = together.Together(api_key=api_key)
            messages = []
            if self.system_prompt:
                messages += [
                    {"role": "system", "content": self.system_prompt},
                ]
            messages += [
                    {"role": "user", "content": self.prompt},
            ]

            together_kwargs = {}
            if self.max_tokens is not None and self.max_tokens > 0:
                together_kwargs["max_tokens"] = self.max_tokens
            if self.temperature is not None:
                together_kwargs["temperature"] = self.temperature

            together_resp = client.chat.completions.create(
                model=self.model,
                messages=messages,
                max_tokens=self.max_tokens,
                temperature=self.temperature,
            )

            message = together_resp.choices[0].message
            response_dict = dict(
                content=message.content,
                reasoning=message.reasoning
            )

            cache_dict[request_key] = response_dict

        return TogetherLLMArtifactResponse(
            request=self,
            response_dict=response_dict,
        )
