from pathlib import Path
from dataclasses import dataclass, field
import itertools
from typing import Iterable, Callable, Any

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


class StepInputBase:
    pass


class StepOutputBase:
    pass




class FullDepsDict(DictConfig):
    def __init__(self, upstream_by_label: dict[str, StepOutputBase]):
        super().__init__(upstream_by_label)

    def as_row(self) -> pd.Series:
        return pd.Series(list(self.values()), index=list(self.keys()))

    @classmethod
    def from_row(cls, row: pd.Series) -> "FullDepsDict":
        return FullDepsDict(row.to_dict())

    @classmethod
    def list_from_df(cls, df: pd.DataFrame) -> list["FullDepsDict"]:
        return [
            cls.from_row(row)
            for _idx, row in df.iterrows()
        ]


@dataclass
class FullStepOutput:
    deps: FullDepsDict
    output: StepOutputBase
    step_name: str

    def as_row(self) -> pd.Series:
        row0 = self.deps.as_row()
        row1 = pd.Series([self.output], index=[self.step_name])
        return pd.concat([row0, row1])

    @classmethod
    def list_to_df(cls, the_list: list["FullStepOutput"]) -> pd.DataFrame:
        rows = [elem.as_row() for elem in the_list]
        return pd.DataFrame(rows)


class DepsResolver:
    def resolve_deps(self, deps_spec: list[str]|str|None) -> Iterable[FullDepsDict]:
        if not deps_spec:
            return [FullDepsDict({})]
        if isinstance(deps_spec, str):
            deps_spec = [deps_spec]

        unprocessed_deps: list[str] = list(reversed(deps_spec))

        df0 = None
        while unprocessed_deps:
            dep = unprocessed_deps.pop()
            df1 = FullStepOutput.list_to_df(self._results[dep])
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
    def resolve_full_config(self, step_name: str, full_config: DictConfig) -> Iterable[StepInputBase]:
        sub_config = full_config[step_name]
        yield from self.resolve_sub_config(sub_config)

    def resolve_sub_config(self, sub_config: DictConfig) -> Iterable[StepInputBase]:
        sub_config = OmegaConf.to_container(sub_config).copy()
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
                yield DictConfig(config_dict)


@dataclass
class StepSpec:
    name: str
    fcn: Callable[..., Iterable[StepOutputBase]]
    deps_resolver: DepsResolver = field(default_factory=DepsResolver)
    config_resolver: ConfigResolver = field(default_factory=ConfigResolver)
    input_type: type[StepInputBase] = StepInputBase
    output_type: type[StepOutputBase] = StepOutputBase




class PipelineBase:
    def __init__(self):
        self._steps: dict[str, StepSpec] = {}
        self._results: dict[str, list[FullStepOutput]] = {}

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
            step_spec = StepSpec(
                name=step_name,
                fcn=fcn,
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

    # def _sub_config_to_specs(self, sub_config: DictConfig) -> Iterable[DictConfig]:
        # myTODO

    # def _get_deps_dicts(self, deps_spec: list[str]|str|None) -> Iterable[FullDepsDict]:
    #     myTODO

    def _execute_step(self, step_name: str, config_full: DictConfig) -> None:
        step_spec = self._steps[step_name]
        assert step_name not in self._results
        self._results[step_name] = []
        config: DictConfig = config_full[step_name]
        deps_spec = config.get("deps")
        for input in step_spec.config_resolver.resolve_sub_config(config):
            for deps_dict in step_spec.deps_resolver.resolve_deps(deps_spec):
                assert not "input" in deps_dict
                step_outputs = list(step_spec.fcn(input=input, **deps_dict))
                full_step_outputs = [
                    FullStepOutput(
                        deps=deps_dict,
                        output=output,
                        step_name=step_name,
                    )
                    for output in step_outputs
                ]
                self._results[step_name].extend(full_step_outputs)


class StepInput(DictConfig, StepInputBase):
    pass

class StepData(DictConfig):
    pass


@dataclass
class CompoundStepOutput(StepOutputBase):
    input: StepInputBase
    data: StepData


class Pipeline(PipelineBase):
    pass


pipeline = Pipeline()


@pipeline.step("doc")
def load_document(input: StepInput, **kwargs) -> Iterable[CompoundStepOutput]:
    dir_path = Path(input.dir_path)
    glob: str = input.glob
    for fpath in sorted(dir_path.glob(glob)):
        text = fpath.read_text().strip()
        data = StepData(dict(text=text))
        input_mod = replace(input, path=fpath, name=fpath.stem)
        yield CompoundStepOutput(input=input_mod, data=data)


@pipeline.step()
def truncated_doc(input: StepInput, doc: CompoundStepOutput, **kwargs) -> Iterable[CompoundStepOutput]:
    nsentences: int = input.nsentences
    text: str = doc.data.text
    sentences = text.split(".")[:nsentences]
    sentences = [s.strip() for s in sentences] + [""]
    new_text = ". ".join(sentences)[:-1]
    data = StepData(dict(text=new_text))
    yield CompoundStepOutput(input=input, data=data)


@pipeline.step()
def translated_doc(input: StepInput, truncated_doc: CompoundStepOutput, **kwargs) -> Iterable[CompoundStepOutput]:
    language: str = input.language
    text: str = truncated_doc.data.text
    new_text = f"[language={language}] {text}"
    data = StepData(dict(text=new_text))
    yield CompoundStepOutput(input=input, data=data)
