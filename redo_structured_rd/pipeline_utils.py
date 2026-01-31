from pathlib import Path
from dataclasses import dataclass, field
import itertools
import collections.abc
from typing import Iterable, Callable, Any, get_type_hints, get_origin, get_args

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
    def post_init(self, step_spec: "StepSpec") -> None:
        self.step_spec = step_spec

    def resolve_deps(self, deps_spec: list[str]|str|None, prev_results: ResultsSpec) -> Iterable[FullDepsDict]:
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

        # the_prod = itertools.product(
        #     *[make_tuples(dep, self.get_instances(dep)) for dep in deps_spec]
        # )
        # for prod_tup in the_prod:
        #     yield FullDepsDict(dict(prod_tup))


class ConfigResolver:
    def post_init(self, step_spec: "StepSpec") -> None:
        self.step_spec = step_spec

    def get_sub_config(self, full_config: ConfigType) -> SubConfigType:
        step_name = self.step_spec.name
        sub_config = full_config[step_name]
        return sub_config

    def resolve_sub_config(self, sub_config: SubConfigType) -> Iterable[StepInputBase]:
        input_type = self.step_spec.input_type
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
                if issubclass(input_type, DictConfig):
                    yield input_type(config_dict)
                else:
                    yield input_type(**config_dict)


@dataclass
class StepSpec:
    name: str
    fcn: Callable[..., Iterable[StepOutputBase]]
    deps_resolver: DepsResolver = field(default_factory=DepsResolver)
    config_resolver: ConfigResolver = field(default_factory=ConfigResolver)
    input_type: type[StepInputBase] = StepInputBase
    output_type: type[StepOutputBase] = StepOutputBase

    def __post_init__(self) -> None:
        self.deps_resolver.post_init(self)
        self.config_resolver.post_init(self)


class PipelineBase:
    def __init__(self):
        self._steps: dict[str, StepSpec] = {}
        self._results: ResultsSpec = {}

    def run(self, config: DictConfig) -> None:
        for step_name in self._steps.keys():
            self._execute_step(step_name, config_full=config)

    def step(self, step_name: str|None = None, **kwargs):
        """
        Decorator to add a step to the pipeline
        """
        def deco(fcn: Callable[..., Iterable[StepOutputBase]]):
            nonlocal step_name
            if step_name is None:
                step_name = fcn.__name__

            hints = get_type_hints(fcn)
            if next(iter(hints)) != "input":
                raise TypeError(f"Expected the first argument of the decorated function to be named `input` and type annotated")
            input_t = hints["input"]
            for hint_name in hints:
                pass
            if hint_name != "return":
                raise TypeError(f"Expected the return of the decorated function to be type annotated")

            ret_t = hints["return"]
            origin = get_origin(ret_t)
            if origin is not collections.abc.Iterable:
                raise TypeError("Return type of the decorated function must be Iterable[...].")
            args = get_args(ret_t)
            if len(args) != 1:
                raise TypeError("Return type of the decorated function must be Iterable[T] with one type argument.")
            output_t = args[0]

            step_spec = StepSpec(
                name=step_name,
                fcn=fcn,
                input_type=input_t,
                output_type=output_t,
                **kwargs
            )
            self._register_step(step_name, step_spec)
        return deco

    def save_results(self, dill_path: Path|None = None) -> None:
        if dill_path is None:
            dill_path = Path("./data/dill/all_results.dill")
        with open(dill_path, 'wb') as fdill:
            dill.dump(self._results, fdill)

    def _register_step(self, step_name: str, step_spec: StepSpec) -> None:
        if step_name in self._steps:
            raise ValueError(f"Already registered step named {step_name}")
        self._steps[step_name] = step_spec

    def get_instances(self, step_name: str) -> Iterable[FullStepOutput]:
        return self._results[step_name]

    def _execute_step(self, step_name: str, config_full: DictConfig) -> None:
        step_spec = self._steps[step_name]
        assert step_name not in self._results
        self._results[step_name] = []
        config: DictConfig = config_full[step_name]
        deps_spec = config.get("deps")
        for input in step_spec.config_resolver.resolve_sub_config(config):
            for full_deps_dict in step_spec.deps_resolver.resolve_deps(deps_spec, prev_results=self._results):
                deps_dict = full_deps_dict.to_simple_dict()
                assert not "input" in deps_dict
                step_outputs = list(step_spec.fcn(input=input, **deps_dict))
                full_step_outputs = [
                    FullStepOutput(
                        deps=full_deps_dict,
                        output=output,
                        step_name=step_name,
                    )
                    for output in step_outputs
                ]
                self._results[step_name].extend(full_step_outputs)
