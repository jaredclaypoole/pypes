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


pipeline = Pipeline()


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

class DocStep(PipelineStepBase):
    def __init__(self):
        super().__init__(
            step_name="doc",
            deps_spec=None,
            proto_input_type=DocProtoInput,
            input_type=DocInput,
            output_type=DocOutput,
        )

    def config_to_inputs(self, config: ConfigType) -> Iterable[DocInput]:
        for proto_input in super().config_to_inputs(config):
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

class TruncatedDocStep(PipelineStepBase):
    def __init__(self):
        super().__init__(
            step_name="truncated_doc",
            deps_spec="doc",
            input_type=TruncatedDocInput,
            output_type=TruncatedDocOutput,
        )

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

class TranslatedDocStep(PipelineStepBase):
    def __init__(self):
        super().__init__(
            step_name="translated_doc",
            deps_spec="truncated_doc",
            input_type=TranslatedDocInput,
            output_type=TranslatedDocOutput,
        )

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
