from pathlib import Path
from typing import Iterable, Generator

from pydantic import BaseModel

from pypes.core.mytyping import (
    StepInputBase,
    StepOutputBase,
    ConfigType,
)
from pypes.base.step import PipelineStepBase
from pypes.base.pipeline import PipelineBase
from pypes.artifacts.base import (
    ArtifactRequestBase,
    ArtifactResponseBase,
)
from pypes.artifacts.step import PipelineStepWithArtifacts
from pypes.utils.pydantic_utils import get_fields_dict

from pypes.artifacts.self.serial import ArtifactSerialSelfResolver
from pypes.artifacts.self.fakellm import FakeLLMArtifactSelfRequest, FakeLLMArtifactResponse


class Pipeline(PipelineBase):
    pass


pipeline = Pipeline()


class StepInput(StepInputBase, BaseModel, frozen=True):
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

    def full_config_to_inputs(self, full_config: ConfigType) -> Iterable[DocInput]:
        for proto_input in super().full_config_to_inputs(full_config):
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


class SummInput(StepInput):
    nwords: int
    model: str
    prompt_version: str


class SummOutput(SummInput):
    summ: str


class SummStep(PipelineStepWithArtifacts):
    def __init__(self):
        super().__init__(
            step_name="summ",
            deps_spec="doc",
            artifact_resolver=ArtifactSerialSelfResolver(),
            input_type=SummInput,
            output_type=SummOutput,
        )

    def gen_input_to_output(self, input: SummInput, doc: DocOutput, **kwargs) \
            -> Generator[ArtifactRequestBase, ArtifactResponseBase, SummOutput]:

        match input.prompt_version:
            case "v001-basic":
                prompt_template_str = f"""
Summarize the following document in at most $nwords words.
Your response should consist only of the summary, not any commentary.
Do not exceed $nwords words.

Document:
$doc

Summary:
""".strip()
            case _:
                raise NotImplementedError()

        prompt_kwargs = dict(
            doc=doc.text,
            nwords=f"{input.nwords}",
        )
        request = FakeLLMArtifactSelfRequest(
            input=input,
            model=input.model,
            prompt_template_str=prompt_template_str,
            prompt_kwargs=prompt_kwargs,
            cache_heading="default",
        )

        response = yield request
        assert isinstance(response, FakeLLMArtifactResponse)
        summ_text = response.text
        output = SummOutput(
            **get_fields_dict(input),
            summ=summ_text,
        )
        return output


pipeline.add_steps(
    [
        DocStep(),
        SummStep(),
    ],
)
