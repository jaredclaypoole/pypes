from ..core.interface import PipelineStepInterface, PipelineInterface
from .caching import ArtifactCacheKey, ArtifactCache


class ArtifactRequestBase:
    @property
    def cache_key(self) -> ArtifactCacheKey:
        raise NotImplementedError()


class ArtifactResponseBase:
    pass


class ArtifactResolverBase:
    def __init__(self):
        super().__init__()
        self.pipeline: PipelineInterface|None = None
        self.step: PipelineStepInterface|None = None

        self.step_cache = ArtifactCache()

    def register_pipeline(self, pipeline: PipelineInterface) -> None:
        self.pipeline = pipeline

    def register_step(self, step: PipelineStepInterface) -> None:
        self.step = step

    def resolve_request(self, request: ArtifactRequestBase) -> ArtifactResponseBase:
        raise NotImplementedError()
