from pathlib import Path
from typing import Iterable, Any

from pydantic import BaseModel

from pypes.core.mytyping import (
    StepInputBase,
    StepOutputBase,
    ConfigType,
)
from pypes.base.step import PipelineStepBase
from pypes.base.pipeline import PipelineBase


def get_fields_dict(model: BaseModel) -> dict[str, Any]:
    return {name: getattr(model, name) for name in type(model).model_fields}


class Pipeline(PipelineBase):
    pass


pipeline = Pipeline(name=Path(__file__).stem)


class StepInput(BaseModel, frozen=True):
    trial: int


class DocProtoInput(StepInput):
    dir_path: str
    glob: str

class DocInput(DocProtoInput):
    path: str
    name: str

class DocOutput(DocInput):
    text: str

@PipelineStepBase.auto_step("doc", proto_input_type=DocProtoInput)
class DocStep:
    def full_config_to_inputs(self, full_config: ConfigType, **kwargs) -> Iterable[DocInput]:
        for proto_input in super().full_config_to_inputs(full_config, **kwargs):
            assert isinstance(proto_input, DocProtoInput)
            dir_path = Path(proto_input.dir_path)
            for fpath in sorted(dir_path.glob(proto_input.glob)):
                yield DocInput(
                    **get_fields_dict(proto_input),
                    path=str(fpath),
                    name=fpath.stem,
                )

    def input_to_output(self, input: DocInput, **kwargs) -> DocOutput:
        fpath = Path(input.path)
        text = fpath.read_text().strip()
        output = DocOutput(
            **get_fields_dict(input),
            text=text,
        )
        return output


class TruncatedDocInput(StepInput):
    nsentences: int

class TruncatedDocOutput(TruncatedDocInput):
    text: str

@PipelineStepBase.auto_step("truncated_doc", deps_spec="doc")
class TruncatedDocStep:
    def input_to_output(self, input: TruncatedDocInput, doc: DocOutput, **kwargs) -> TruncatedDocOutput:
        sentences = doc.text.split(".")[:input.nsentences]
        sentences = [s.strip() for s in sentences] + [""]
        new_text = ". ".join(sentences)[:-1]
        output = TruncatedDocOutput(
            **get_fields_dict(input),
            text=new_text,
        )
        return output


class TranslatedDocInput(StepInput):
    language: str

class TranslatedDocOutput(TranslatedDocInput):
    text: str

@PipelineStepBase.auto_step("translated_doc", deps_spec="truncated_doc")
class TranslatedDocStep:
    def input_to_output(self, input: TranslatedDocInput, truncated_doc: TruncatedDocOutput, **kwargs) -> TranslatedDocOutput:
        new_text = f"[language={input.language}] {truncated_doc.text}"
        output = TranslatedDocOutput(
            **get_fields_dict(input),
            text=new_text,
        )
        return output


pipeline.add_steps(
    [
        DocStep(),
        TruncatedDocStep(),
        TranslatedDocStep(),
    ],
)
