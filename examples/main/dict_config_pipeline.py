from pathlib import Path
from typing import Iterable, Any

from omegaconf import OmegaConf, DictConfig

from pypes.base.step import PipelineStepBase
from pypes.base.pipeline import PipelineBase


def get_fields_dict(config: DictConfig) -> dict[str, Any]:
    return OmegaConf.to_container(config)


def replace(config: DictConfig, **kwargs: Any) -> DictConfig:
    the_dict = {
        **get_fields_dict(config),
        **kwargs,
    }
    return DictConfig(the_dict)


class Pipeline(PipelineBase):
    pass


pipeline = Pipeline()


@PipelineStepBase.auto_step("doc")
class DocStep:
    def full_config_to_inputs(self, full_config: DictConfig) -> Iterable[DictConfig]:
        for proto_input in super().full_config_to_inputs(full_config):
            assert isinstance(proto_input, DictConfig)
            dir_path = Path(proto_input.dir_path)
            for fpath in sorted(dir_path.glob(proto_input.glob)):
                yield replace(
                    proto_input,
                    path=str(fpath),
                    name=fpath.stem,
                )

    def input_to_output(self, input: DictConfig, **kwargs) -> DictConfig:
        fpath = Path(input.path)
        text = fpath.read_text().strip()
        return replace(
            input,
            text=text,
        )


@PipelineStepBase.auto_step("truncated_doc", deps_spec="doc")
class TruncatedDocStep:
    def input_to_output(self, input: DictConfig, doc: DictConfig, **kwargs) -> DictConfig:
        sentences = doc.text.split(".")[:input.nsentences]
        sentences = [s.strip() for s in sentences] + [""]
        new_text = ". ".join(sentences)[:-1]
        return replace(
            input,
            text=new_text,
        )


@PipelineStepBase.auto_step("translated_doc", deps_spec="truncated_doc")
class TranslatedDocStep:
    def input_to_output(self, input: DictConfig, truncated_doc: DictConfig, **kwargs) -> DictConfig:
        new_text = f"[language={input.language}] {truncated_doc.text}"
        return replace(
            input,
            text=new_text,
        )


pipeline.add_steps(
    [
        DocStep(),
        TruncatedDocStep(),
        TranslatedDocStep(),
    ],
)
