from ..base import ArtifactRequestBase, ArtifactResponseBase, ArtifactResolverBase


class ArtifactSelfRequestBase(ArtifactRequestBase):
    def init_cache(self, resolver: ArtifactResolverBase) -> None:
        raise NotImplementedError()  # pragma: no cover

    def resolve(self, resolver: ArtifactResolverBase) -> ArtifactResponseBase:
        raise NotImplementedError()  # pragma: no cover
