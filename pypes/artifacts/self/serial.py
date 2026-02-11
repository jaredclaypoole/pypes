from ..base import ArtifactRequestBase, ArtifactResponseBase, ArtifactResolverBase
from .base import ArtifactSelfRequestBase


class ArtifactSerialSelfResolver(ArtifactResolverBase):
    def resolve_request(self, request: ArtifactRequestBase) -> ArtifactResponseBase:
        assert isinstance(request, ArtifactSelfRequestBase)

        request.init_cache(self)
        return request.resolve(self)
