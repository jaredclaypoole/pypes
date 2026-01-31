from pathlib import Path
from typing import Iterable, Any

from pydantic import BaseModel

from redo_structured_rd.pipeline_utils import (
    PipelineBase,
    StepInputBase,
    StepOutputBase,
)

def get_fields_dict(model: BaseModel) -> dict[str, Any]:
    return {name: getattr(model, name) for name in model.model_fields}


class Pipeline(PipelineBase):
    pass


pipeline = Pipeline()


class StepInput(BaseModel, frozen=True):
    trial: int


class DocInput(StepInput):
    dir_path: str
    glob: str

class DocOutput(DocInput):
    path: str
    name: str
    text: str

@pipeline.step("doc")
def load_document(input: DocInput, **kwargs) -> Iterable[DocOutput]:
    dir_path = Path(input.dir_path)
    glob: str = input.glob
    for fpath in sorted(dir_path.glob(glob)):
        text = fpath.read_text().strip()
        output = DocOutput(
            **get_fields_dict(input),
            path=str(fpath),
            name=fpath.stem,
            text=text,
        )
        yield output


class TruncatedDocInput(StepInput):
    nsentences: int

class TruncatedDocOutput(TruncatedDocInput):
    text: str

@pipeline.step()
def truncated_doc(input: TruncatedDocInput, doc: DocOutput, **kwargs) -> Iterable[TruncatedDocOutput]:
    nsentences = input.nsentences
    text = doc.text
    sentences = text.split(".")[:nsentences]
    sentences = [s.strip() for s in sentences] + [""]
    new_text = ". ".join(sentences)[:-1]
    output = TruncatedDocOutput(
        **get_fields_dict(input),
        text=new_text,
    )
    yield output


class TranslatedDocInput(StepInput):
    language: str

class TranslatedDocOutput(TranslatedDocInput):
    text: str

@pipeline.step()
def translated_doc(input: TranslatedDocInput, truncated_doc: TruncatedDocOutput, **kwargs) -> Iterable[TranslatedDocOutput]:
    language = input.language
    text = truncated_doc.text
    new_text = f"[language={language}] {text}"
    output = TranslatedDocOutput(
        **get_fields_dict(input),
        text=new_text,
    )
    yield output
