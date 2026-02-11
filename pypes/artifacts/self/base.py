from ..base import ArtifactRequestBase, ArtifactResponseBase, ArtifactResolverBase


class ArtifactSelfRequestBase(ArtifactRequestBase):
    def init_cache(self, resolver: ArtifactResolverBase) -> None:
        raise NotImplementedError()

    def resolve(self, resolver: ArtifactResolverBase) -> ArtifactResponseBase:
        raise NotImplementedError()
