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
from pypes.artifacts.self.fakellm import (
    FakeLLMArtifactSelfRequest,
    FakeLLMArtifactResponse,
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


class StepInput(StepInputBase, BaseModel, frozen=True):
    trial: int


class DocInput(StepInput):
    name: str
    text: str

class DocOutput(DocInput):
    pass

class DocStep(PipelineStepBase):
    def __init__(self):
        super().__init__(
            step_name="doc",
            deps_spec=None,
            input_type=DocInput,
            output_type=DocOutput,
        )

    def input_to_output(self, input: DocInput, **kwargs) -> DocOutput:
        output = DocOutput(
            **get_fields_dict(input),
        )
        return output



class TranslatedDocInput(StepInput):
    language: str

class TranslatedDocOutput(TranslatedDocInput):
    text: str

class TranslatedDocStep(PipelineStepWithArtifacts):
    def __init__(self):
        super().__init__(
            step_name="translated_doc",
            deps_spec="doc",
            artifact_resolver=ArtifactSerialSelfResolver(),
            input_type=TranslatedDocInput,
            output_type=TranslatedDocOutput,
        )

    def gen_input_to_output(self, input: TranslatedDocInput, doc: DocOutput, **kwargs) \
            -> Generator[FakeLLMArtifactSelfRequest, FakeLLMArtifactResponse, TranslatedDocOutput]:
        new_text = f"[language={input.language}] {doc.text}"
        prompt_template_str = """
Translate the following document into $language:

{$document}
""".strip()

        request = FakeLLMArtifactSelfRequest(
            input=input,
            model="fake-llm",
            prompt_template_str=prompt_template_str,
            prompt_kwargs=dict(language=input.language, document=doc.text),
            cache_heading="fake_llm",
        )

        response = yield request

        new_text_from_artifact = response.text
        output = TranslatedDocOutput(
            **get_fields_dict(input),
            text=new_text_from_artifact,
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

        translated_doc_results1 = first_results["translated_doc"]
        assert len(translated_doc_results1) == 2

        sample_output1: TranslatedDocOutput = translated_doc_results1[0].output
        text = sample_output1.text
        lines = [line.strip() for line in text.splitlines()]
        first, second, third, *rest = lines
        assert third == "This is a fake LLM response to the following prompt:"

        cache_dir_expected = tmp_dir / "translated_doc/base/fake_llm"
        assert cache_dir_expected.exists()
        assert len(list(cache_dir_expected.glob("*.txt"))) == 2

        second_pipeline = create_pipeline()
        second_pipeline.run(full_config)
        second_results = second_pipeline.results
        translated_doc_results2 = second_results["translated_doc"]
        sample_output2: TranslatedDocOutput = translated_doc_results2[0].output

        assert sample_output2.text == sample_output1.text
