from pathlib import Path

import hydra
from omegaconf import DictConfig, OmegaConf
from tqdm import tqdm

from pypes.base.pipeline import PipelineBase
from .example_registry import all_example_modules


def main():
    for example_module in tqdm(all_example_modules, desc="Running example modules: "):
        run(example_module)


def run(example_module):
    pipeline_module = example_module
    pipeline: PipelineBase = pipeline_module.pipeline
    config_path = Path(pipeline_module.__file__).parent / "config"

    @hydra.main(version_base=None, config_path=str(config_path), config_name="default")
    def hydra_run(config: DictConfig) -> None:
        pipeline.run(config)
        pipeline.save_results()

    hydra_run()
