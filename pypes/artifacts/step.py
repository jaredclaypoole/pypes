from typing import Generator

from ..core.mytyping import (
    StepInputBase,
    StepOutputBase,
    DepsType,
)
from ..core.interface import PipelineInterface
from ..base.step import PipelineStepBase
from .base import ArtifactRequestBase, ArtifactResponseBase, ArtifactResolverBase


class PipelineStepWithArtifacts(PipelineStepBase):
    def __init__(
        self,
        step_name: str,
        deps_spec: list[str]|str|None,
        artifact_resolver: ArtifactResolverBase,
        proto_input_type: type[StepInputBase]|None = None,
        input_type: type[StepInputBase] = StepInputBase,
        output_type: type[StepOutputBase] = StepOutputBase,
    ):
        super().__init__(
            step_name=step_name,
            deps_spec=deps_spec,
            proto_input_type=proto_input_type,
            input_type=input_type,
            output_type=output_type,
        )
        self._artifact_resolver = artifact_resolver
        self._artifact_resolver.register_step(self)

    def gen_input_to_output(self, input: StepInputBase, **deps: DepsType) \
            -> Generator[ArtifactRequestBase, ArtifactResponseBase, StepOutputBase]:
        raise NotImplementedError()  # pragma: no cover

    def input_to_output(self, input: StepInputBase, **deps: DepsType) -> StepOutputBase:
        gen = self.gen_input_to_output(input=input, **deps)
        response: ArtifactResponseBase|None = None
        while True:
            try:
                request = gen.send(response)
            except StopIteration as error:
                ret = error.value
                return ret

            response = self._artifact_resolver.resolve_request(request)

    def set_pipeline(self, pipeline: PipelineInterface) -> None:
        super().set_pipeline(pipeline)
        self._artifact_resolver.register_pipeline(pipeline)
