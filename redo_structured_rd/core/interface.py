from pathlib import Path
from typing import Iterable

from .mytyping import (
    ConfigType,
    DepsType,
    FullDepsDict,
    StepInputBase,
    StepOutputBase,
    ResultsSpec,
)


class PipelineInterface:
    @property
    def results(self) -> ResultsSpec:
        raise NotImplementedError()

    @property
    def cache_base_dir(self) -> Path|None:
        raise NotImplementedError()


class PipelineStepInterface:
    @property
    def step_name(self) -> str:
        raise NotImplementedError()

    @property
    def proto_input_type(self) -> type[StepInputBase]:
        raise NotImplementedError()

    @property
    def cache_subdir(self) -> Path:
        raise NotImplementedError()

    def set_pipeline(self, pipeline: "PipelineInterface") -> None:
        self.pipeline = pipeline

    def resolve_deps(self) -> Iterable[FullDepsDict]:
        raise NotImplementedError()

    def unpack_deps(self, full_deps_dict: FullDepsDict) -> dict[str, DepsType]:
        raise NotImplementedError()

    def config_to_inputs(self, config: ConfigType) -> Iterable[StepInputBase]:
        raise NotImplementedError()

    def input_to_output(self, input: StepInputBase, **deps: DepsType) -> StepOutputBase:
        raise NotImplementedError()
