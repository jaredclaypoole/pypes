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
        raise NotImplementedError()  # pragma: no cover

    @property
    def cache_base_dir(self) -> Path|None:
        raise NotImplementedError()  # pragma: no cover


class PipelineStepInterface:
    @property
    def step_name(self) -> str:
        raise NotImplementedError()  # pragma: no cover

    @property
    def proto_input_type(self) -> type[StepInputBase]:
        raise NotImplementedError()  # pragma: no cover

    @property
    def cache_subdir(self) -> Path:
        raise NotImplementedError()  # pragma: no cover

    def set_pipeline(self, pipeline: "PipelineInterface") -> None:
        self.pipeline = pipeline

    def resolve_deps(self) -> Iterable[FullDepsDict]:
        raise NotImplementedError()  # pragma: no cover

    def unpack_deps(self, full_deps_dict: FullDepsDict) -> dict[str, DepsType]:
        raise NotImplementedError()  # pragma: no cover

    def full_config_to_inputs(self, full_config: ConfigType) -> Iterable[StepInputBase]:
        raise NotImplementedError()  # pragma: no cover

    def input_to_output(self, input: StepInputBase, **deps: DepsType) -> StepOutputBase:
        raise NotImplementedError()  # pragma: no cover
