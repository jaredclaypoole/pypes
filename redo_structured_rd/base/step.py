from pathlib import Path
from typing import Iterable

from ..core.mytyping import (
    DepsType,
    FullDepsDict,
    ConfigType,
    StepInputBase,
    StepOutputBase,
)
from ..core.interface import PipelineStepInterface
from ..resolvers.deps import DepsResolver
from ..resolvers.config import ConfigResolver


class PipelineStepBase(PipelineStepInterface):
    def __init__(
        self,
        step_name: str,
        substep_name: str = "base",
        deps_spec: list[str]|str|None = None,
        proto_input_type: type[StepInputBase]|None = None,
        input_type: type[StepInputBase] = StepInputBase,
        output_type: type[StepOutputBase] = StepOutputBase,
    ):
        super().__init__()
        self._step_name = step_name
        self._substep_name = substep_name
        self.deps_spec = deps_spec
        self._proto_input_type = proto_input_type or input_type
        self.input_type = input_type
        self.output_type = output_type

        self._deps_resolver = DepsResolver()
        self._config_resolver = ConfigResolver(self)

    @property
    def step_name(self) -> str:
        return self._step_name

    @property
    def proto_input_type(self) -> StepInputBase:
        return self._proto_input_type

    @property
    def substep_name(self) -> str:
        return self._substep_name

    @property
    def cache_subdir(self) -> Path:
        return Path(self.step_name) / self.substep_name

    def resolve_deps(self) -> Iterable[FullDepsDict]:
        yield from self._deps_resolver.resolve_deps(self.deps_spec, self.pipeline.results)

    def unpack_deps(self, full_deps_dict: FullDepsDict) -> dict[str, DepsType]:
        deps_dict = {
            name: full_step_output.output
            for name, full_step_output in full_deps_dict.items()
        }
        return deps_dict

    def config_to_inputs(self, config: ConfigType) -> Iterable[StepInputBase]:
        yield from self._config_resolver.resolve_sub_config(config)

    def input_to_output(self, input: StepInputBase, **deps: DepsType) -> StepOutputBase:
        raise NotImplementedError()
