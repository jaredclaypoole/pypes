from pathlib import Path
import random
from string import Template
import hashlib
from typing import Iterable, Any, Generator

from pydantic import BaseModel

from redo_structured_rd.core.mytyping import (
    StepInputBase,
    StepOutputBase,
    ConfigType,
)
from redo_structured_rd.core.interface import (
    PipelineStepInterface,
    PipelineInterface,
)
from redo_structured_rd.base.step import PipelineStepBase
from redo_structured_rd.base.pipeline import PipelineBase
from redo_structured_rd.artifacts.base import (
    ArtifactRequestBase,
    ArtifactResponseBase,
    ArtifactResolverBase,
)
from redo_structured_rd.artifacts.step import PipelineStepWithArtifacts


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
    input: StepInput
    model: str
    prompt_template_str: str
    prompt_kwargs: dict[str, str]
    cache: bool = True
    cache_heading: str = "default"


class FakeLLMArtifactResponse(ArtifactResponseBase, BaseModel, frozen=True):
    request: FakeLLMArtifactRequest
    text: str


def hash(obj: Any) -> str:
    if isinstance(obj, BaseModel):
        return hash(tuple(get_fields_dict(obj).items()))
    elif isinstance(obj, tuple):
        return hash(str(obj))
    elif isinstance(obj, list):
        return hash(tuple(obj))
    elif isinstance(obj, dict):
        return hash(tuple(obj.items()))
    elif isinstance(obj, (int, float)):
        return hash(str(obj))
    elif isinstance(obj, str):
        return hashlib.sha256(obj.encode("utf-8")).hexdigest()
    else:
        raise NotImplementedError(type(obj))


class CachedStringDictBase:
    def __init__(
        self,
        assert_exists: bool = False,
    ):
        self._data: dict[str, str] = {}
        self._init_cache(assert_exists=assert_exists)

    def _init_cache(self, assert_exists: bool) -> None:
        raise NotImplementedError()

    def _update_cache(self, key: str, value: str) -> None:
        raise NotImplementedError()

    def __setitem__(self, key: str, value: str) -> None:
        self._update_cache(key, value)
        self._data[key] = value

    def __getitem__(self, key: str) -> str:
        return self._data[key]

    def items(self) -> Iterable[tuple[str, str]]:
        yield from self._data.items()

    def keys(self) -> Iterable[str]:
        yield from self._data.keys()

    def values(self) -> Iterable[str]:
        yield from self._data.values()

    def __iter__(self) -> Iterable[str]:
        yield from self.keys()


class DirCachedStringDict(CachedStringDictBase):
    def __init__(
        self,
        cache_dir: Path,
        assert_exists: bool = False,
    ):
        self.cache_dir = cache_dir
        super().__init__(assert_exists=assert_exists)

    def _init_cache(self, assert_exists: bool) -> None:
        if assert_exists:
            assert self.cache_dir.exists()
        else:
            self.cache_dir.mkdir(exist_ok=True, parents=True)

        fpaths = sorted(self.cache_dir.glob("*.txt"))
        for fpath in fpaths:
            self._data[fpath.stem] = fpath.read_text().strip()

    def _update_cache(self, key: str, value: str) -> None:
        with open(self.cache_dir / f"{key}.txt", 'w') as ftxt:
            print(value, file=ftxt)


class ArtifactResolver(ArtifactResolverBase):
    def __init__(self):
        super().__init__()
        self.pipeline: PipelineInterface|None = None
        self.step: PipelineStepInterface|None = None

        self._cache_dict_by_dir: dict[Path, DirCachedStringDict] = {}

    def register_pipeline(self, pipeline: PipelineInterface) -> None:
        self.pipeline = pipeline

    def register_step(self, step: PipelineStepInterface) -> None:
        self.step = step

    def resolve_request(self, request: ArtifactRequestBase) -> ArtifactResponseBase:
        assert isinstance(request, FakeLLMArtifactRequest)
        response_text: str|None = None

        if request.cache:
            cache_base_dir = self.pipeline.cache_base_dir
            assert cache_base_dir is not None
            step_cache_dir = cache_base_dir / self.step.cache_subdir
            cache_dir = step_cache_dir / request.cache_heading
            cache_dict = self._cache_dict_by_dir.get(cache_dir)
            if cache_dict is None:
                cache_dict = DirCachedStringDict(cache_dir=cache_dir)
                self._cache_dict_by_dir[cache_dir] = cache_dict

            request_key = hash(request)
            if request_key in cache_dict:
                response_text = cache_dict[request_key]

        if response_text is None:
            random_value = random.randint(10_000, 99_999)
            prompt_template = Template(
                request.prompt_template_str,
            )
            prompt_text = prompt_template.substitute(**request.prompt_kwargs)
            response_text = f"""
[randomness={random_value}]
[model={request.model}]
This is a fake LLM response to the following prompt:
{prompt_text}
""".strip()

            if request.cache:
                cache_dict[request_key] = response_text

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
        request = FakeLLMArtifactRequest(
            input=input,
            model=input.model,
            prompt_template_str=prompt_template_str,
            prompt_kwargs=prompt_kwargs,
            cache=True,
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
