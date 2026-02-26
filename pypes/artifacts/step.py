from typing import Generator, Callable, Any, TypeVar

from ..core.mytyping import (
    StepInputBase,
    StepOutputBase,
    DepsType,
)
from ..core.interface import PipelineInterface
from ..base.step import PipelineStepBase
from .base import ArtifactRequestBase, ArtifactResponseBase, ArtifactResolverBase
from ..utils.autosubclass import auto_subclass
from ..utils.read_type_hints import get_first_param_and_return_type, unpack_generator_type_hint


C = TypeVar("C", bound=type[Any])


class PipelineStepWithArtifacts(PipelineStepBase):
    def __init__(
        self,
        step_name: str,
        substep_name: str = "base",
        deps_spec: list[str]|str|None = None,
        artifact_resolver: ArtifactResolverBase | None = None,
        proto_input_type: type[StepInputBase]|None = None,
        input_type: type[StepInputBase] = StepInputBase,
        output_type: type[StepOutputBase] = StepOutputBase,
    ):
        if artifact_resolver is None:
            raise ValueError("A non-None artifact_resolver must be passed explicitly")

        super().__init__(
            step_name=step_name,
            substep_name=substep_name,
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

    @classmethod
    def auto_step(
        cls,
        step_name: str,
        substep_name: str = "base",
        *,
        deps_spec: list[str]|str|None = None,
        artifact_resolver: ArtifactResolverBase|None = None,
        proto_input_type: type[StepInputBase]|None = None,
        **kwargs_for_init,
    ) -> Callable[[C], C]:
        if artifact_resolver is None:
            raise ValueError("A non-None artifact_resolver must be passed explicitly to auto_step")

        def fkwargs(other_class: C) -> dict[str, Any]:
            gen_input_to_output_method = getattr(other_class, "gen_input_to_output", None)
            if gen_input_to_output_method is None:
                raise ValueError(f"Expected decorated class to have a `gen_input_to_output` method")
            input_type, gen_type = get_first_param_and_return_type(gen_input_to_output_method)
            _, _, output_type = unpack_generator_type_hint(gen_type)
            return dict(input_type=input_type, output_type=output_type)

        auto_suclass_deco = auto_subclass(
            cls,
            fkwargs=fkwargs,
            step_name=step_name,
            substep_name=substep_name,
            deps_spec=deps_spec,
            proto_input_type=proto_input_type,
            artifact_resolver=artifact_resolver,
            **kwargs_for_init,
        )
        return auto_suclass_deco
