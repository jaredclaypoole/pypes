from pathlib import Path
from dataclasses import dataclass
from typing import Iterable

from omegaconf import DictConfig

from redo_structured_rd.pipeline_utils import (
    replace,
    PipelineBase,
    StepInputBase,
    StepOutputBase,
)


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