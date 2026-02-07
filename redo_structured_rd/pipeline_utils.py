from pathlib import Path
from dataclasses import dataclass, field
import itertools
import collections.abc
from typing import Iterable, Generator, Callable, Any

from omegaconf import DictConfig, OmegaConf
import pandas as pd
import dill


def make_tuples(label: str, the_list: list[Any]) -> Iterable[tuple[str, Any]]:
    for val in the_list:
        yield label, val


def replace(obj: DictConfig, **kwargs) -> DictConfig:
    obj = obj.copy()
    for key, value in kwargs.items():
        obj[key] = value
    return obj


ConfigType = DictConfig
SubConfigType = DictConfig


def sub_config_to_dict(sub_config: SubConfigType) -> dict:
    if isinstance(sub_config, DictConfig):
        return OmegaConf.to_container(sub_config)
    else:
        raise NotImplementedError(type(sub_config))


class StepInputBase:
    pass


class StepOutputBase:
    pass

DepsType = StepOutputBase
DepsSpecType = list[str]|str|None


class FullDepsDict(dict):
    def __init__(self, upstream_by_label: dict[str, "FullStepOutput"]):
        super().__init__(upstream_by_label)

    def as_row(self) -> pd.Series:
        return pd.Series(list(self.values()), index=list(self.keys()))

    @classmethod
    def from_row(cls, row: pd.Series) -> "FullDepsDict":
        return cls(row.to_dict())

    @classmethod
    def list_from_df(cls, df: pd.DataFrame) -> list["FullDepsDict"]:
        return [
            cls.from_row(row)
            for _idx, row in df.iterrows()
        ]

    def to_simple_dict(self) -> dict[str, StepOutputBase]:
        return {
            name: full_step_output.output
            for name, full_step_output in self.items()
        }

    def __hash__(self):
        return object.__hash__(self)


@dataclass(frozen=True, eq=False)
class FullStepOutput:
    deps: FullDepsDict
    output: StepOutputBase
    step_name: str

    def as_row(self, full_output: bool = True) -> pd.Series:
        output = self if full_output else self.output
        row0 = self.deps.as_row()
        row1 = pd.Series([output], index=[self.step_name])
        return pd.concat([row0, row1])

    @classmethod
    def list_to_df(cls, the_list: list["FullStepOutput"]) -> pd.DataFrame:
        rows = [elem.as_row() for elem in the_list]
        return pd.DataFrame(rows)


ResultsSpec = dict[str, list[FullStepOutput]]


class DepsResolver:
    def resolve_deps(self, deps_spec: DepsSpecType, prev_results: ResultsSpec) -> Iterable[FullDepsDict]:
        if not deps_spec:
            return [FullDepsDict({})]
        if isinstance(deps_spec, str):
            deps_spec = [deps_spec]

        unprocessed_deps: list[str] = list(reversed(deps_spec))

        df0 = None
        while unprocessed_deps:
            dep = unprocessed_deps.pop()
            df1 = FullStepOutput.list_to_df(prev_results[dep])
            if df0 is None:
                df0 = df1
            else:
                df0 = df0.join(df1, how="outer")

        return FullDepsDict.list_from_df(df0)


class ConfigResolver:
    def __init__(self, step: "PipelineStepBase"):
        super().__init__()
        self.step = step

    def get_sub_config(self, full_config: ConfigType) -> SubConfigType:
        step_name = self.step.step_name
        sub_config = full_config[step_name]
        return sub_config

    def resolve_sub_config(self, sub_config: SubConfigType) -> Iterable[StepInputBase]:
        proto_input_type = self.step.proto_input_type
        sub_config = sub_config_to_dict(sub_config).copy()
        ntrials = sub_config.pop("ntrials", 1)
        config_dict0 = {}
        scan_vals = {}
        for key, value in sub_config.items():
            if isinstance(value, list):
                scan_vals[key] = value
                config_dict0[key] = None
            else:
                config_dict0[key] = value

        keys_tup = tuple(scan_vals.keys())
        for vals_tup in itertools.product(*scan_vals.values()):
            for trial in range(ntrials):
                config_dict = {
                    **dict(trial=trial),
                    **config_dict0,
                }
                for key, val in zip(keys_tup, vals_tup, strict=True):
                    config_dict[key] = val
                if issubclass(proto_input_type, DictConfig):
                    yield proto_input_type(config_dict)
                else:
                    yield proto_input_type(**config_dict)


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
        self.proto_input_type = proto_input_type or input_type
        self.input_type = input_type
        self.output_type = output_type

        self._deps_resolver = DepsResolver()
        self._config_resolver = ConfigResolver(self)

    @property
    def step_name(self) -> str:
        return self._step_name

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
        raise NotImplementedError()

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


class PipelineBase(PipelineInterface):
    def __init__(self):
        self._steps: dict[str, PipelineStepInterface] = {}
        self._results: ResultsSpec = {}
        self._cache_base_dir: Path|None = None

    @property
    def results(self) -> ResultsSpec:
        return self._results

    @property
    def cache_base_dir(self) -> Path|None:
        return self._cache_base_dir

    def process_config(self, config_full: DictConfig) -> None:
        sub_config: DictConfig = config_full.get("pipeline", DictConfig({}))
        if cache_base_dir := sub_config.get("cache_base_dir"):
            self._cache_base_dir = Path(cache_base_dir)

    def run(self, config: DictConfig) -> None:
        self.process_config(config)
        for step_name in self._steps.keys():
            self._execute_step(step_name, config_full=config)

    def save_results(self, dill_path: Path|None = None) -> None:
        if dill_path is None:
            dill_path = Path("./data/dill/all_results.dill")
        with open(dill_path, 'wb') as fdill:
            dill.dump(self._results, fdill)

    def add_steps(self, steps: Iterable[PipelineStepInterface]) -> None:
        for step in steps:
            self.add_step(step)

    def add_step(self, step: PipelineStepInterface) -> None:
        step_name = step.step_name
        if step_name in self._steps:
            raise ValueError(f"Already registered step named {step_name}")
        step.set_pipeline(self)
        self._steps[step_name] = step

    def get_instances(self, step_name: str) -> Iterable[FullStepOutput]:
        return self._results[step_name]

    def _execute_step(self, step_name: str, config_full: DictConfig) -> None:
        step = self._steps[step_name]
        assert step_name not in self._results
        self._results[step_name] = []
        config: DictConfig = config_full[step_name]
        for input in step.config_to_inputs(config):
            for full_deps_dict in step.resolve_deps():
                deps_dict = full_deps_dict.to_simple_dict()
                assert not "input" in deps_dict
                step_output = step.input_to_output(input=input, **deps_dict)
                full_step_output = FullStepOutput(
                    deps=full_deps_dict,
                    output=step_output,
                    step_name=step_name,
                )
                self._results[step_name].append(full_step_output)
