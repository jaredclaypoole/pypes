from pathlib import Path
import random
from string import Template
from typing import Iterable, Any, Generator

from pydantic import BaseModel

from redo_structured_rd.pipeline_utils import (
    PipelineStepBase,
    PipelineStepWithArtifacts,
    ArtifactRequestBase,
    ArtifactResponseBase,
    ArtifactResolverBase,
    PipelineBase,
    StepInputBase,
    StepOutputBase,
    ConfigType,
)

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


class FakeLLMArtifactRequest(ArtifactRequestBase, BaseModel, frozen=True):
    model: str
    prompt_template_str: str
    prompt_kwargs: dict[str, str]
    cache: bool = True
    cache_heading: str = "default"


class FakeLLMArtifactResponse(ArtifactResponseBase, BaseModel, frozen=True):
    request: FakeLLMArtifactRequest
    text: str


class ArtifactResolver(ArtifactResolverBase):
    def resolve_request(self, request: ArtifactRequestBase) -> ArtifactResponseBase:
        assert isinstance(request, FakeLLMArtifactRequest)
        if request.cache:
            raise NotImplementedError("We still need to implement caching; for now we just have artifact control flow without caching")

        random_value = random.randint(10_000, 99_999)
        prompt_template = Template(
            request.prompt_template_str,
        )
        prompt_text = prompt_template.substitute(**request.prompt_kwargs)
        response_text = f"""
[randomness={random_value}]
This is a fake LLM response to the following prompt:
{prompt_text}
""".strip()
        return FakeLLMArtifactResponse(
            request=request,
            text=response_text,
        )


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
            artifact_resolver=ArtifactResolver(),
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
        do_cache = False
        if not do_cache:
            print(f"Warning: Not caching")
        request = FakeLLMArtifactRequest(
            model=input.model,
            prompt_template_str=prompt_template_str,
            prompt_kwargs=prompt_kwargs,
            cache=do_cache,
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