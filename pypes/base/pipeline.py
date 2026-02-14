from pathlib import Path
from typing import Iterable

import dill
from tqdm import tqdm

from ..core.interface import PipelineStepInterface, PipelineInterface
from ..core.mytyping import (
    ResultsSpec,
    ConfigType,
    SubConfigType,
    FullStepOutput,
)


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

    def process_config(self, full_config: ConfigType) -> None:
        sub_config: SubConfigType = full_config.get("pipeline", SubConfigType({}))
        if cache_base_dir := sub_config.get("cache_base_dir"):
            self._cache_base_dir = Path(cache_base_dir)

    def run(self, config: ConfigType) -> None:
        self.process_config(config)
        for step_name in self._steps.keys():
            self._execute_step(step_name, full_config=config)

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

    def _execute_step(self, step_name: str, full_config: ConfigType) -> None:
        step = self._steps[step_name]
        assert step_name not in self._results
        self._results[step_name] = []
        with tqdm(desc=f"{step_name} ") as pbar:
            for input in step.full_config_to_inputs(full_config):
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
                    pbar.update()
