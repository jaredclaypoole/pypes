from pathlib import Path
import tempfile
from typing import Generator

from omegaconf import OmegaConf
from pydantic import BaseModel

from pypes.core.mytyping import (
    StepInputBase,
    StepOutputBase,
    ConfigType,
    FullStepOutput,
    ResultsSpec,
)
from pypes.base.step import PipelineStepBase
from pypes.base.pipeline import PipelineBase
from pypes.artifacts.step import PipelineStepWithArtifacts
from pypes.artifacts.self.serial import (
    ArtifactSerialSelfResolver,
)
from pypes.artifacts.self.dummy import (
    DummyStrDictArtifactSelfRequest,
    DummyStrDictArtifactResponse,
)
from pypes.utils.pydantic_utils import get_fields_dict

import pytest


config_str = """
doc:
  - name: first-doc
    text: "This is my first document. It is short."
  - name: second-doc
    text: "This is another document. It is slightly longer."

translated_doc:
  language: fr

"""


class Pipeline(PipelineBase):
    pass


class StepInput(BaseModel, frozen=True):
    trial: int


class DocInput(StepInput):
    name: str
    text: str

class DocOutput(DocInput):
    pass

@PipelineStepBase.auto_step("doc")
class DocStep:
    def input_to_output(self, input: DocInput, **kwargs) -> DocOutput:
        output = DocOutput(
            **get_fields_dict(input),
        )
        return output


class TranslatedDocInput(StepInput):
    language: str

class TranslatedDocOutput(TranslatedDocInput):
    text: str
    cache_hit: bool

@PipelineStepWithArtifacts.auto_step(
    "translated_doc",
    deps_spec="doc",
    artifact_resolver=ArtifactSerialSelfResolver(),
)
class TranslatedDocStep:
    def gen_input_to_output(self, input: TranslatedDocInput, doc: DocOutput, **kwargs) \
            -> Generator[DummyStrDictArtifactSelfRequest, DummyStrDictArtifactResponse, TranslatedDocOutput]:
        new_text = f"[language={input.language}] {doc.text}"

        request = DummyStrDictArtifactSelfRequest(
            content=new_text,
            cache_heading="dummy",
        )

        response = yield request

        new_text_from_artifact = response.content
        output = TranslatedDocOutput(
            **get_fields_dict(input),
            text=new_text_from_artifact,
            cache_hit=response.cache_hit,
        )
        return output


def create_pipeline() -> Pipeline:
    the_pipeline = Pipeline()
    the_pipeline.add_steps(
        [
            DocStep(),
            TranslatedDocStep(),
        ],
    )
    return the_pipeline


def get_outputs(step_results: list[FullStepOutput]) -> list[StepOutputBase]:
    return [fso.output for fso in step_results]


def test_artifact_pipeline():
    with tempfile.TemporaryDirectory() as tmpdirname:
        tmp_dir = Path(tmpdirname)

        lines = [
            "pipeline:",
            f"  cache_base_dir: {str(tmp_dir)}",
        ]
        meta_config_str = "\n".join(lines)
        full_config_str = "\n".join([meta_config_str, config_str])
        full_config = OmegaConf.create(full_config_str)

        first_pipeline = create_pipeline()

        first_pipeline.run(full_config)
        first_results = first_pipeline.results

        doc_config = full_config["doc"]
        assert get_outputs(first_results["doc"]) == [
            DocOutput(
                trial=0,
                name=doc_config[0].name,
                text=doc_config[0].text,
            ),
            DocOutput(
                trial=0,
                name=doc_config[1].name,
                text=doc_config[1].text,
            ),
        ]

        assert get_outputs(first_results["translated_doc"]) == [
            TranslatedDocOutput(
                trial=0,
                language="fr",
                text="[language=fr] This is my first document. It is short.",
                cache_hit=False,
            ),
            TranslatedDocOutput(
                trial=0,
                language="fr",
                text="[language=fr] This is another document. It is slightly longer.",
                cache_hit=False,
            ),
        ]

        cache_dir_expected = tmp_dir / "translated_doc/base/dummy"
        assert cache_dir_expected.exists()
        assert len(list(cache_dir_expected.glob("*.txt"))) == 2

        second_pipeline = create_pipeline()
        second_pipeline.run(full_config)
        second_results = second_pipeline.results

        assert get_outputs(second_results["translated_doc"]) == [
            TranslatedDocOutput(
                trial=0,
                language="fr",
                text="[language=fr] This is my first document. It is short.",
                cache_hit=True,
            ),
            TranslatedDocOutput(
                trial=0,
                language="fr",
                text="[language=fr] This is another document. It is slightly longer.",
                cache_hit=True,
            ),
        ]


def test_explicit_deco():
    class FirstStep:
        def gen_input_to_output(self, input: DocInput, **kwargs) -> Generator[str, str, DocOutput]:
            name = yield "name"
            text = yield "text"
            output = DocOutput(
                **get_fields_dict(input),
                name=name,
                text=text,
            )
            return output

    with pytest.raises(ValueError):
        PipelineStepWithArtifacts.auto_step("first_step")(FirstStep)

    DecoratedFirstStep = PipelineStepWithArtifacts.auto_step("first_step", artifact_resolver=ArtifactSerialSelfResolver())(FirstStep)
    step = DecoratedFirstStep()
    assert step.input_type == DocInput
    assert step.output_type == DocOutput

    class DummyFirstStep:
        pass

    with pytest.raises(ValueError):
        PipelineStepWithArtifacts.auto_step("dummy_first_step", artifact_resolver=ArtifactSerialSelfResolver())(DummyFirstStep)
