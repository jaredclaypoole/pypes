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


class InstructorLLMArtifactResponse(ArtifactResponseBase, BaseModel, frozen=True):
    request: "InstructorLLMArtifactSelfRequest"
    response_obj: BaseModel


class InstructorLLMArtifactSelfRequest(ArtifactSelfRequestBase, BaseModel, frozen=True):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    trial: int
    model: str
    system_prompt: str|None = None
    prompt: str
    response_model: type[BaseModel]
    temperature: float|None = None
    max_tokens: int|None = None
    max_retries: int = 3
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

        response_obj: BaseModel|None = None

        if request_key in cache_dict:
            json_obj = cache_dict[request_key]
            response_obj = self.response_model.model_validate(json_obj)

        if response_obj is None:
            try:
                import instructor
            except OSError:
                raise ValueError(f"Could not import instructor.  Perhaps run `pip install instructor` in your venv?")

            client = instructor.from_provider(
                model=self.model,
            )
            messages = []
            if self.system_prompt:
                messages += [
                    {"role": "system", "content": self.system_prompt},
                ]
            messages += [
                    {"role": "user", "content": self.prompt},
            ]

            client_kwargs = dict(max_retries=self.max_retries)
            if self.max_tokens is not None and self.max_tokens > 0:
                client_kwargs["max_tokens"] = self.max_tokens
            if self.temperature is not None:
                client_kwargs["temperature"] = self.temperature

            response_obj = client.create(
                messages=messages,
                response_model=self.response_model,
                **client_kwargs,
            )

            assert response_obj is not None

            response_dict = response_obj.model_dump()
            cache_dict[request_key] = response_dict

        return InstructorLLMArtifactResponse(
            request=self,
            response_obj=response_obj,
        )
