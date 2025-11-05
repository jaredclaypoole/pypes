from pathlib import Path
from dataclasses import dataclass
from typing import Generator

from omegaconf import DictConfig


@dataclass
class StepOutput:
    spec: DictConfig
    data: DictConfig


class Pipeline:
    def auto_step(self, step_name: str):
        def deco(fcn):
            def inner(*args, **kwargs):
                return fcn(*args, **kwargs)
            return inner
        return deco

pipeline = Pipeline()


@pipeline.auto_step("doc")
def load_document(spec: DictConfig) -> Generator[StepOutput]:
    dir_path = Path(spec.dir_path)
    glob: str = spec.glob
    for fpath in sorted(dir_path.glob(glob)):
        text = fpath.read_text().strip()
        data = DictConfig({"text": text})
        yield StepOutput(spec=spec, data=data)
