from ..core.interface import PipelineStepInterface, PipelineInterface


class ArtifactRequestBase:
    pass


class ArtifactResponseBase:
    pass


class ArtifactResolverBase:
    def register_pipeline(self, pipeline: PipelineInterface) -> None:
        pass

    def register_step(self, step: PipelineStepInterface) -> None:
        pass

    def resolve_request(self, request: ArtifactRequestBase) -> ArtifactResponseBase:
        raise NotImplementedError()
