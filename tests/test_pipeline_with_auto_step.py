from pathlib import Path
import tempfile

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
from pypes.utils.pydantic_utils import get_fields_dict

import pytest


config_str = """
doc:
  - name: first-doc
    text: "This is my first document. It is short."
  - name: second-doc
    text: "This is another document. It is slightly longer."

truncated_doc:
  nsentences: [1, 2]

translated_doc:
  ntrials: 2
  language: [fr, de]

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


@pytest.fixture
def pipeline() -> Pipeline:
    the_pipeline = Pipeline()
    the_pipeline.add_steps(
        [
            DocStep(),
            TruncatedDocStep(),
            TranslatedDocStep(),
        ],
    )
    return the_pipeline


def get_outputs(step_results: list[FullStepOutput]) -> list[StepOutputBase]:
    return [fso.output for fso in step_results]


def test_pipeline(pipeline: Pipeline):
    full_config = OmegaConf.create(config_str)

    pipeline.run(full_config)
    results = pipeline.results

    doc_config = full_config["doc"]
    assert get_outputs(results["doc"]) == [
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

    assert get_outputs(results["truncated_doc"]) == [
        TruncatedDocOutput(
            trial=0,
            nsentences=1,
            text="This is my first document.",
        ),
        TruncatedDocOutput(
            trial=0,
            nsentences=1,
            text="This is another document.",
        ),
        TruncatedDocOutput(
            trial=0,
            nsentences=2,
            text="This is my first document. It is short.",
        ),
        TruncatedDocOutput(
            trial=0,
            nsentences=2,
            text="This is another document. It is slightly longer.",
        ),
    ]

    assert get_outputs(results["translated_doc"]) == [
        TranslatedDocOutput(
            trial=0,
            language="fr",
            text="[language=fr] This is my first document.",
        ),
        TranslatedDocOutput(
            trial=0,
            language="fr",
            text="[language=fr] This is another document.",
        ),
        TranslatedDocOutput(
            trial=0,
            language="fr",
            text="[language=fr] This is my first document. It is short.",
        ),
        TranslatedDocOutput(
            trial=0,
            language="fr",
            text="[language=fr] This is another document. It is slightly longer.",
        ),
        TranslatedDocOutput(
            trial=1,
            language="fr",
            text="[language=fr] This is my first document.",
        ),
        TranslatedDocOutput(
            trial=1,
            language="fr",
            text="[language=fr] This is another document.",
        ),
        TranslatedDocOutput(
            trial=1,
            language="fr",
            text="[language=fr] This is my first document. It is short.",
        ),
        TranslatedDocOutput(
            trial=1,
            language="fr",
            text="[language=fr] This is another document. It is slightly longer.",
        ),
        TranslatedDocOutput(
            trial=0,
            language="de",
            text="[language=de] This is my first document.",
        ),
        TranslatedDocOutput(
            trial=0,
            language="de",
            text="[language=de] This is another document.",
        ),
        TranslatedDocOutput(
            trial=0,
            language="de",
            text="[language=de] This is my first document. It is short.",
        ),
        TranslatedDocOutput(
            trial=0,
            language="de",
            text="[language=de] This is another document. It is slightly longer.",
        ),
        TranslatedDocOutput(
            trial=1,
            language="de",
            text="[language=de] This is my first document.",
        ),
        TranslatedDocOutput(
            trial=1,
            language="de",
            text="[language=de] This is another document.",
        ),
        TranslatedDocOutput(
            trial=1,
            language="de",
            text="[language=de] This is my first document. It is short.",
        ),
        TranslatedDocOutput(
            trial=1,
            language="de",
            text="[language=de] This is another document. It is slightly longer.",
        ),
    ]


def test_save_results(pipeline: Pipeline):
    full_config = OmegaConf.create(config_str)
    pipeline.run(full_config)
    with tempfile.TemporaryDirectory() as tmpdirname:
        fpath = Path(tmpdirname) / "results.dill"
        assert not fpath.exists()
        pipeline.save_results(dill_path=fpath)
        assert fpath.exists()


def test_duplicate_step(pipeline: Pipeline):
    with pytest.raises(ValueError):
        pipeline.add_step(DocStep())


def test_explicit_deco():
    class FirstStep:
        def input_to_output(self, input: DocInput, **kwargs) -> DocOutput:
            output = DocOutput(
                **get_fields_dict(input),
            )
            return output

    DecoratedFirstStep = PipelineStepBase.auto_step("first_step")(FirstStep)
    step = DecoratedFirstStep()
    assert step.input_type == DocInput
    assert step.output_type == DocOutput

    class DummyFirstStep:
        pass

    with pytest.raises(ValueError):
        PipelineStepBase.auto_step("dummy_first_step")(DummyFirstStep)
